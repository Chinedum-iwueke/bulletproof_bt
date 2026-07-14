from __future__ import annotations

import pandas as pd
import pytest

from bt.research_data.alignment import build_research_panel
from bt.research_data.instruments import native_to_canonical_symbol, reconcile_stable_symbols
from bt.research_data.schemas import OHLCV_COLUMNS
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_series, utc_ts
from bt.research_data.universe import build_volatile_universe_from_ohlcv, rank_volatile_scores
from bt.research_data.validation import assert_causal_sources, assert_utc_monotonic_unique


def _ohlcv(symbol: str, rows: list[tuple[object, float, float | None]]) -> pd.DataFrame:
    data = []
    for ts, close, quote_volume in rows:
        timestamp = pd.Timestamp(ts)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        data.append(
            {
                "ts": timestamp,
                "exchange": "binance",
                "symbol": symbol,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1.0,
                "quote_volume": quote_volume if quote_volume is not None else close,
                "trade_count": 1,
            }
        )
    return pd.DataFrame(data, columns=OHLCV_COLUMNS)


def test_parquet_upsert_deduplicates_by_key(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    path = store.raw_path("binance", "BTCUSDT", "ohlcv", "1m")
    first = _ohlcv("BTCUSDT", [("2021-01-01 00:00", 10.0, 10.0), ("2021-01-01 00:01", 11.0, 11.0)])
    second = _ohlcv("BTCUSDT", [("2021-01-01 00:01", 12.0, 12.0), ("2021-01-01 00:02", 13.0, 13.0)])

    store.upsert_dataset("binance", "BTCUSDT", "ohlcv", "1m", first)
    combined = store.upsert_dataset("binance", "BTCUSDT", "ohlcv", "1m", second)

    assert len(combined) == 3
    assert combined.loc[combined["ts"].eq(pd.Timestamp("2021-01-01 00:01", tz="UTC")), "close"].item() == 12.0
    assert path.exists()


def test_spot_and_perp_storage_namespaces_do_not_collide(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    perp = _ohlcv("BTCUSDT", [("2021-01-01 00:00", 10.0, 10.0)])
    spot = _ohlcv("BTCUSDT", [("2021-01-01 00:00", 20.0, 20.0)])
    perp["canonical_symbol"] = "BTC-USDT-PERP"
    spot["canonical_symbol"] = "BTC-USDT-SPOT"

    store.upsert_dataset("binance", "BTCUSDT", "ohlcv", "1m", perp, market="perp")
    store.upsert_dataset("binance", "BTCUSDT", "ohlcv", "1m", spot, market="spot")

    saved_perp = store.read(store.raw_path("binance", "BTCUSDT", "ohlcv", "1m", market="perp"))
    saved_spot = store.read(store.raw_path("binance", "BTCUSDT", "ohlcv", "1m", market="spot"))

    assert saved_perp["close"].item() == 10.0
    assert saved_spot["close"].item() == 20.0
    assert store.raw_path("binance", "BTCUSDT", "ohlcv", "1m", market="perp") != store.raw_path(
        "binance", "BTCUSDT", "ohlcv", "1m", market="spot"
    )


def test_native_symbol_mapping_separates_spot_from_perp() -> None:
    assert native_to_canonical_symbol("BTCUSDT", market="perp") == "BTC-USDT-PERP"
    assert native_to_canonical_symbol("BTCUSDT", market="spot") == "BTC-USDT-SPOT"


def test_timestamp_normalization_to_utc() -> None:
    ts = utc_ts("2021-01-01")
    series = utc_series(["2021-01-01 00:00:00", "2021-01-01 01:00:00+01:00"])

    assert str(ts.tz) == "UTC"
    assert series.iloc[0] == pd.Timestamp("2021-01-01 00:00:00", tz="UTC")
    assert series.iloc[1] == pd.Timestamp("2021-01-01 00:00:00", tz="UTC")


def test_funding_join_is_backward_asof_and_preserves_source_ts() -> None:
    ohlcv = _ohlcv("BTCUSDT", [("2021-01-01 00:00", 100.0, 100.0), ("2021-01-01 00:01", 101.0, 101.0)])
    funding = pd.DataFrame(
        [
            {"ts": pd.Timestamp("2021-01-01 00:00", tz="UTC"), "exchange": "binance", "symbol": "BTCUSDT", "funding_rate": 0.1, "mark_price_at_funding": 100.0},
            {"ts": pd.Timestamp("2021-01-01 00:02", tz="UTC"), "exchange": "binance", "symbol": "BTCUSDT", "funding_rate": 0.2, "mark_price_at_funding": 102.0},
        ]
    )

    panel = build_research_panel(ohlcv, pd.DataFrame(), pd.DataFrame(), funding, pd.DataFrame())

    assert panel["funding_rate"].tolist() == [0.1, 0.1]
    assert panel["funding_source_ts"].tolist() == [pd.Timestamp("2021-01-01 00:00", tz="UTC")] * 2


def test_oi_join_is_backward_asof_and_preserves_source_ts() -> None:
    ohlcv = _ohlcv("BTCUSDT", [("2021-01-01 00:04", 100.0, 100.0), ("2021-01-01 00:05", 101.0, 101.0)])
    oi = pd.DataFrame(
        [
            {"ts": pd.Timestamp("2021-01-01 00:00", tz="UTC"), "exchange": "binance", "symbol": "BTCUSDT", "open_interest": 10.0, "open_interest_value": 1000.0},
            {"ts": pd.Timestamp("2021-01-01 00:06", tz="UTC"), "exchange": "binance", "symbol": "BTCUSDT", "open_interest": 20.0, "open_interest_value": 2000.0},
        ]
    )

    panel = build_research_panel(ohlcv, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), oi)

    assert panel["open_interest"].tolist() == [10.0, 10.0]
    assert panel["oi_source_ts"].tolist() == [pd.Timestamp("2021-01-01 00:00", tz="UTC")] * 2


def test_volatile_universe_ranking_uses_only_prior_data() -> None:
    bars = pd.concat(
        [
            _ohlcv(
                "AAAUSDT",
                [(pd.Timestamp("2020-12-01", tz="UTC") + pd.Timedelta(days=i), 100.0, 10_000_000.0) for i in range(30)]
                + [("2021-01-01 00:00", 100.0, 10_000_000.0), ("2021-01-02 00:00", 200.0, 10_000_000.0), ("2021-01-02 02:00", 600.0, 10_000_000.0)],
            ),
            _ohlcv(
                "BBBUSDT",
                [(pd.Timestamp("2020-12-01", tz="UTC") + pd.Timedelta(days=i), 100.0, 10_000_000.0) for i in range(30)]
                + [("2021-01-01 00:00", 100.0, 10_000_000.0), ("2021-01-02 00:00", 50.0, 10_000_000.0), ("2021-01-02 02:00", 5.0, 10_000_000.0)],
            ),
        ],
        ignore_index=True,
    )

    membership = build_volatile_universe_from_ohlcv(
        bars,
        exchange="binance",
        start=pd.Timestamp("2021-01-02 00:00", tz="UTC"),
        end=pd.Timestamp("2021-01-02 01:00", tz="UTC"),
        rebalance_freq="1h",
        lookback="24h",
        top_gainers=1,
        top_losers=1,
        min_age_days=30,
        min_median_dollar_volume_7d=1,
    )

    assert membership.loc[membership["rank_type"].eq("gainer"), "symbol"].tolist() == ["AAAUSDT"]
    assert membership.loc[membership["rank_type"].eq("gainer"), "score"].item() == pytest.approx(1.0)
    assert membership.loc[membership["rank_type"].eq("loser"), "symbol"].tolist() == ["BBBUSDT"]


def test_volatile_ranker_emits_single_active_row_per_symbol_timestamp() -> None:
    scores = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2021-01-01 00:00", tz="UTC")] * 2,
            "rebalance_id": [0, 0],
            "symbol": ["AAAUSDT", "BBBUSDT"],
            "score": [-0.1, -0.2],
        }
    )

    membership = rank_volatile_scores(scores, exchange="binance", top_gainers=2, top_losers=2)

    assert not membership.duplicated(["ts", "symbol"]).any()
    assert membership.loc[membership["symbol"].eq("AAAUSDT"), "rank_type"].item() == "loser"


def test_stable_universe_symbols_do_not_fail_when_listing_starts_after_2021() -> None:
    instruments = pd.DataFrame(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "onboard_ts": pd.Timestamp("2019-09-01", tz="UTC"),
            },
            {
                "exchange": "binance",
                "symbol": "SUIUSDT",
                "onboard_ts": pd.Timestamp("2023-05-03", tz="UTC"),
            },
        ]
    )

    stable = reconcile_stable_symbols(instruments)

    assert bool(stable.loc[stable["configured_symbol"].eq("BTCUSDT"), "available"].item()) is True
    assert stable.loc[stable["configured_symbol"].eq("SUIUSDT"), "first_seen_ts"].item() == pd.Timestamp("2023-05-03", tz="UTC")
    assert bool(stable.loc[stable["configured_symbol"].eq("TONUSDT"), "available"].item()) is False


def test_validation_catches_duplicate_timestamps() -> None:
    df = _ohlcv("BTCUSDT", [("2021-01-01 00:00", 1.0, 1.0), ("2021-01-01 00:00", 2.0, 2.0)])

    with pytest.raises(ValueError, match="duplicate timestamps"):
        assert_utc_monotonic_unique(df)


def test_validation_catches_source_ts_after_panel_ts() -> None:
    panel = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                "funding_source_ts": pd.Timestamp("2021-01-01 00:01", tz="UTC"),
                "oi_source_ts": pd.NaT,
            }
        ]
    )

    with pytest.raises(ValueError, match="future data"):
        assert_causal_sources(panel)
