"""Fetch job execution and validation-before-persistence."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bt.research_data.fetching.chunking import FetchChunk
from bt.research_data.fetching.retry import ExchangeRateLimiter, RetryPolicy, call_with_retry
from bt.research_data.fetching.state import CoverageStore, FetchKey, FetchStateStore
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import timeframe_delta, utc_ts


@dataclass(frozen=True)
class FetchJob:
    market: str
    exchange: str
    symbol: str
    dataset: str
    timeframe: str
    chunk: FetchChunk


def fetch_logger(log_path: Path = Path("logs/research_data_fetch.log")) -> logging.Logger:
    logger = logging.getLogger("bt.research_data.fetch")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_path)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def log_fetch_event(**payload: object) -> None:
    payload.setdefault("logged_at", utc_ts("now").isoformat())
    fetch_logger().info(json.dumps(payload, default=str, sort_keys=True))


def validate_before_commit(df: pd.DataFrame, dataset: str, timeframe: str) -> None:
    if df.empty:
        return
    if "ts" not in df.columns:
        raise ValueError(f"{dataset} missing ts")
    ts = pd.to_datetime(df["ts"], utc=True)
    if ts.duplicated().any():
        raise ValueError(f"{dataset} contains duplicate timestamps")
    if not ts.is_monotonic_increasing:
        raise ValueError(f"{dataset} timestamps must be monotonic")
    if dataset == "ohlcv":
        for col in ("open", "high", "low", "close", "volume"):
            if col not in df.columns:
                raise ValueError(f"ohlcv missing {col}")
        if (df["volume"] < 0).any():
            raise ValueError("ohlcv volume must be non-negative")
        if (df["high"] < df[["open", "close", "low"]].max(axis=1)).any():
            raise ValueError("ohlcv high violates OHLC relationship")
        if (df["low"] > df[["open", "close", "high"]].min(axis=1)).any():
            raise ValueError("ohlcv low violates OHLC relationship")
    if dataset in {"ohlcv", "mark", "index"}:
        freq = timeframe_delta(timeframe)
        epoch = pd.Timestamp("1970-01-01T00:00:00Z")
        offsets = ((ts - epoch) / freq) % 1
        if (offsets != 0).any():
            raise ValueError(f"{dataset} timestamps must align to {timeframe} candle opens")
    if dataset == "funding":
        exchange = str(df["exchange"].dropna().iloc[0]).lower() if "exchange" in df.columns and df["exchange"].notna().any() else ""
        if exchange == "bybit":
            # Bybit publishes funding as an exchange event stream and the
            # interval can vary by instrument/period. Treat the published event
            # timestamp as the source of truth, while still rejecting malformed
            # sub-minute timestamps after adapter-level jitter normalization.
            epoch = pd.Timestamp("1970-01-01T00:00:00Z")
            minute_offsets = ((ts - epoch) / pd.Timedelta(minutes=1)) % 1
            if (minute_offsets != 0).any():
                raise ValueError("bybit funding timestamps must align to minute event boundaries")
        else:
            cadence = pd.Timedelta(hours=8)
            epoch = pd.Timestamp("1970-01-01T00:00:00Z")
            offsets = ((ts - epoch) / cadence) % 1
            if (offsets != 0).any():
                raise ValueError("funding timestamps must align to 8h cadence")
    if dataset == "oi" and ts.duplicated().any():
        raise ValueError("oi contains duplicate snapshots")


class DatasetFetcher:
    def __init__(self, adapter) -> None:
        self.adapter = adapter

    def fetch(self, job: FetchJob) -> pd.DataFrame:
        if job.dataset == "ohlcv":
            if job.market == "spot" and hasattr(self.adapter, "fetch_spot_ohlcv"):
                return self.adapter.fetch_spot_ohlcv(job.symbol, job.chunk.start, job.chunk.end, job.timeframe)
            return self.adapter.fetch_ohlcv(job.symbol, job.chunk.start, job.chunk.end, job.timeframe)
        if job.market == "spot":
            raise ValueError(f"spot market supports only ohlcv dataset, got {job.dataset}")
        if job.dataset == "mark":
            return self.adapter.fetch_mark(job.symbol, job.chunk.start, job.chunk.end, job.timeframe)
        if job.dataset == "index":
            return self.adapter.fetch_index(job.symbol, job.chunk.start, job.chunk.end, job.timeframe)
        if job.dataset == "funding":
            return self.adapter.fetch_funding(job.symbol, job.chunk.start, job.chunk.end)
        if job.dataset == "oi":
            return self.adapter.fetch_open_interest(job.symbol, job.chunk.start, job.chunk.end)
        raise ValueError(f"unsupported dataset: {job.dataset}")


def execute_fetch_job(
    job: FetchJob,
    adapter,
    store: ResearchDataStore,
    state_store: FetchStateStore,
    coverage_store: CoverageStore,
    retry_policy: RetryPolicy,
    limiter: ExchangeRateLimiter,
) -> pd.DataFrame:
    started = time.monotonic()
    key = FetchKey(job.market, job.exchange, job.symbol, job.dataset, job.timeframe)
    state_store.update(key, status="running", last_attempt_ts=job.chunk.end)
    rows_fetched = 0
    rows_written = 0
    retry_count = 0
    status = "success"
    error = ""
    try:
        fetcher = DatasetFetcher(adapter)
        df, retry_count = call_with_retry(lambda: fetcher.fetch(job), retry_policy, limiter)
        rows_fetched = len(df)
        validate_before_commit(df, job.dataset, job.timeframe)
        raw_timeframe = "5m" if job.dataset == "oi" else job.timeframe
        combined = store.write_dataset_chunk(
            job.exchange,
            job.symbol,
            job.dataset,
            raw_timeframe,
            df,
            job.chunk.start,
            job.chunk.end,
            market=job.market,
        )
        rows_written = len(combined)
        state_store.update(
            key,
            status="success",
            last_attempt_ts=job.chunk.end,
            last_successful_ts=job.chunk.end,
            last_row_count=rows_fetched,
        )
        coverage_store.update_from_dataset(job.exchange, job.symbol, job.dataset, job.timeframe, market=job.market)
        return combined
    except Exception as exc:
        status = "failed"
        error = str(exc)
        state_store.update(
            key,
            status="failed",
            last_attempt_ts=job.chunk.end,
            last_row_count=rows_fetched,
            error_message=error,
        )
        raise
    finally:
        duration = time.monotonic() - started
        log_fetch_event(
            market=job.market,
            exchange=job.exchange,
            symbol=job.symbol,
            dataset=job.dataset,
            timeframe=job.timeframe,
            chunk_start=job.chunk.start.isoformat(),
            chunk_end=job.chunk.end.isoformat(),
            rows_fetched=rows_fetched,
            rows_written=rows_written,
            duration_seconds=round(duration, 6),
            retry_count=retry_count,
            status=status,
            error_message=error,
        )
