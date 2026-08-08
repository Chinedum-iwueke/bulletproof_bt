from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bt.research_data.fetching.chunking import iter_chunks, overlap_for_dataset
from bt.research_data.fetching.fetch_jobs import FetchJob, execute_fetch_job, validate_before_commit
from bt.research_data.fetching.retry import ExchangeRateLimiter, RetryPolicy
from bt.research_data.fetching.scheduler import ChunkScheduler
from bt.research_data.fetching.state import CoverageStore, FetchKey, FetchStateStore, compute_coverage_row
from bt.research_data.storage import ResearchDataStore


def _ohlcv(symbol: str, start: str = "2021-01-01 00:00", rows: int = 2) -> pd.DataFrame:
    ts = pd.date_range(pd.Timestamp(start, tz="UTC"), periods=rows, freq="1min")
    return pd.DataFrame(
        {
            "ts": ts,
            "exchange": "binance",
            "symbol": symbol,
            "open": [1.0] * rows,
            "high": [2.0] * rows,
            "low": [0.5] * rows,
            "close": [1.5] * rows,
            "volume": [10.0] * rows,
            "quote_volume": [15.0] * rows,
            "trade_count": [1] * rows,
        }
    )


class FakeAdapter:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def fetch_ohlcv(self, symbol, start, end, timeframe):
        return self.frame[(self.frame["ts"] >= start) & (self.frame["ts"] < end)].reset_index(drop=True)

    def fetch_spot_ohlcv(self, symbol, start, end, timeframe):
        return self.fetch_ohlcv(symbol, start, end, timeframe)


def test_kline_chunking_uses_1500_one_minute_candles() -> None:
    chunks = list(iter_chunks("2021-01-01", "2021-01-03 02:00", "ohlcv", "1m"))

    assert chunks[0].end - chunks[0].start == pd.Timedelta(minutes=1500)
    assert chunks[-1].end == pd.Timestamp("2021-01-03 02:00", tz="UTC")


def test_scheduler_resumes_from_last_successful_timestamp(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    state = FetchStateStore(store)
    key = FetchKey("perp", "binance", "BTCUSDT", "ohlcv", "1m")
    state.update(
        key,
        status="success",
        last_attempt_ts=pd.Timestamp("2021-01-02", tz="UTC"),
        last_successful_ts=pd.Timestamp("2021-01-02", tz="UTC"),
        last_row_count=1500,
    )

    plan = ChunkScheduler(store, state).plan_backfill(
        "binance", "BTCUSDT", "ohlcv", "1m", "2021-01-01", "2021-01-03"
    )

    assert plan.jobs[0].chunk.start == pd.Timestamp("2021-01-02", tz="UTC")


def test_stale_running_fetch_state_can_be_reset_without_losing_checkpoint(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    state = FetchStateStore(store)
    key = FetchKey("perp", "bybit", "BTCUSDT", "ohlcv", "1m")
    state.update(
        key,
        status="running",
        last_attempt_ts=pd.Timestamp("2021-01-03", tz="UTC"),
        last_successful_ts=pd.Timestamp("2021-01-02", tz="UTC"),
        last_row_count=1500,
    )
    df = state.read()
    df.loc[df["exchange"].eq("bybit"), "updated_at"] = pd.Timestamp("2021-01-03", tz="UTC")
    store.write_atomic(df, state.path)

    reset = state.mark_stale_running_failed(
        exchange="bybit",
        older_than=pd.Timestamp("2021-01-04", tz="UTC"),
    )

    row = state.get(key)
    assert reset == 1
    assert row is not None
    assert row["status"] == "failed"
    assert row["last_successful_ts"] == pd.Timestamp("2021-01-02", tz="UTC")


def test_update_overlap_safety_constants() -> None:
    assert overlap_for_dataset("ohlcv") == pd.Timedelta(days=3)
    assert overlap_for_dataset("mark") == pd.Timedelta(days=3)
    assert overlap_for_dataset("index") == pd.Timedelta(days=3)
    assert overlap_for_dataset("oi") == pd.Timedelta(days=3)
    assert overlap_for_dataset("funding") == pd.Timedelta(days=14)


def test_ohlcv_validator_rejects_bad_ohlc_and_duplicate_ts() -> None:
    bad = _ohlcv("BTCUSDT", rows=2)
    bad.loc[0, "high"] = 1.0
    bad.loc[0, "close"] = 2.0

    with pytest.raises(ValueError, match="OHLC"):
        validate_before_commit(bad, "ohlcv", "1m")

    dup = _ohlcv("BTCUSDT", rows=2)
    dup.loc[1, "ts"] = dup.loc[0, "ts"]
    with pytest.raises(ValueError, match="duplicate"):
        validate_before_commit(dup, "ohlcv", "1m")


def test_funding_validator_requires_expected_cadence() -> None:
    funding = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2021-01-01 01:00", tz="UTC")],
            "exchange": ["binance"],
            "symbol": ["BTCUSDT"],
            "funding_rate": [0.01],
            "mark_price_at_funding": [1.0],
        }
    )

    with pytest.raises(ValueError, match="8h cadence"):
        validate_before_commit(funding, "funding", "1m")


def test_bybit_funding_validator_accepts_exchange_published_variable_cadence() -> None:
    funding = pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                pd.Timestamp("2021-01-01 04:00", tz="UTC"),
                pd.Timestamp("2021-01-01 08:00", tz="UTC"),
            ],
            "exchange": ["bybit", "bybit", "bybit"],
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "funding_rate": [0.01, 0.02, 0.03],
            "mark_price_at_funding": [pd.NA, pd.NA, pd.NA],
        }
    )

    validate_before_commit(funding, "funding", "1m")


def test_bybit_funding_validator_rejects_malformed_event_precision() -> None:
    funding = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2021-01-01 00:00:30", tz="UTC")],
            "exchange": ["bybit"],
            "symbol": ["BTCUSDT"],
            "funding_rate": [0.01],
            "mark_price_at_funding": [pd.NA],
        }
    )

    with pytest.raises(ValueError, match="minute event boundaries"):
        validate_before_commit(funding, "funding", "1m")


def test_execute_fetch_job_validates_persists_state_coverage_and_log(tmp_path, monkeypatch) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    state = FetchStateStore(store)
    coverage = CoverageStore(store)
    frame = _ohlcv("BTCUSDT", rows=2)
    job = FetchJob(
        "perp",
        "binance",
        "BTCUSDT",
        "ohlcv",
        "1m",
        next(iter_chunks("2021-01-01", "2021-01-01 00:02", "ohlcv", "1m")),
    )
    log_path = tmp_path / "logs" / "research_data_fetch.log"

    import bt.research_data.fetching.fetch_jobs as fetch_jobs

    logger = fetch_jobs.fetch_logger(log_path)
    monkeypatch.setattr(fetch_jobs, "fetch_logger", lambda: logger)

    execute_fetch_job(
        job,
        FakeAdapter(frame),
        store,
        state,
        coverage,
        RetryPolicy(max_attempts=1),
        ExchangeRateLimiter(0),
    )

    saved = store.read(store.raw_path("binance", "BTCUSDT", "ohlcv", "1m"))
    assert len(saved) == 2
    state_df = state.read()
    assert state_df.loc[0, "status"] == "success"
    coverage_df = store.read(store.manifest_path("coverage"))
    assert coverage_df.loc[0, "missing_rows"] == 0
    payload = json.loads(Path(log_path).read_text().strip())
    assert payload["rows_fetched"] == 2
    assert payload["status"] == "success"


def test_spot_fetch_job_writes_to_spot_namespace(tmp_path, monkeypatch) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    state = FetchStateStore(store)
    coverage = CoverageStore(store)
    frame = _ohlcv("BTCUSDT", rows=2)
    job = FetchJob(
        "spot",
        "binance",
        "BTCUSDT",
        "ohlcv",
        "1m",
        next(iter_chunks("2021-01-01", "2021-01-01 00:02", "ohlcv", "1m")),
    )
    log_path = tmp_path / "logs" / "research_data_fetch.log"

    import bt.research_data.fetching.fetch_jobs as fetch_jobs

    logger = fetch_jobs.fetch_logger(log_path)
    monkeypatch.setattr(fetch_jobs, "fetch_logger", lambda: logger)

    execute_fetch_job(
        job,
        FakeAdapter(frame),
        store,
        state,
        coverage,
        RetryPolicy(max_attempts=1),
        ExchangeRateLimiter(0),
    )

    saved = store.read(store.raw_path("binance", "BTCUSDT", "ohlcv", "1m", market="spot"))
    assert len(saved) == 2
    assert (store.raw_path("binance", "BTCUSDT", "ohlcv", "1m", market="spot").parent / "chunks").exists()
    assert not store.legacy_raw_path("binance", "BTCUSDT", "ohlcv", "1m").exists()
    state_df = state.read()
    assert state_df.loc[0, "market"] == "spot"


def test_coverage_tracks_missing_rows_and_largest_gap() -> None:
    df = _ohlcv("BTCUSDT", rows=3)
    df = df.drop(index=1).reset_index(drop=True)

    row = compute_coverage_row("binance", "BTCUSDT", "ohlcv", "1m", df)

    assert row["expected_rows"] == 3
    assert row["actual_rows"] == 2
    assert row["missing_rows"] == 1
    assert row["largest_gap_minutes"] == 2.0
