"""Top-level fetching orchestration."""
from __future__ import annotations

import pandas as pd

from bt.research_data.exchanges.factory import get_adapter
from bt.research_data.fetching.fetch_jobs import execute_fetch_job
from bt.research_data.fetching.retry import ExchangeRateLimiter, RetryPolicy
from bt.research_data.fetching.scheduler import ChunkScheduler
from bt.research_data.fetching.state import CoverageStore, FetchStateStore
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts


def fetch_backfill(
    exchange: str,
    dataset: str,
    symbol: str,
    timeframe: str,
    start: object,
    end: object,
    *,
    market: str = "perp",
    retry_policy: RetryPolicy | None = None,
    rate_limit_seconds: float = 0.05,
    store: ResearchDataStore | None = None,
) -> None:
    store = store or ResearchDataStore()
    state_store = FetchStateStore(store)
    coverage_store = CoverageStore(store)
    scheduler = ChunkScheduler(store, state_store)
    adapter = get_adapter(exchange, market=market)
    limiter = ExchangeRateLimiter(rate_limit_seconds)
    policy = retry_policy or RetryPolicy()
    plan = scheduler.plan_backfill(exchange, symbol, dataset, timeframe, start, end, resume=True, market=market)
    for job in plan.jobs:
        execute_fetch_job(job, adapter, store, state_store, coverage_store, policy, limiter)


def fetch_update(
    exchange: str,
    *,
    all_symbols: bool = False,
    symbols: list[str] | None = None,
    datasets: list[str] | None = None,
    timeframe: str = "1m",
    end: object = "now",
    market: str = "perp",
    retry_policy: RetryPolicy | None = None,
    rate_limit_seconds: float = 0.05,
    store: ResearchDataStore | None = None,
    continue_on_error: bool = True,
) -> None:
    store = store or ResearchDataStore()
    state_store = FetchStateStore(store)
    coverage_store = CoverageStore(store)
    scheduler = ChunkScheduler(store, state_store)
    adapter = get_adapter(exchange, market=market)
    limiter = ExchangeRateLimiter(rate_limit_seconds)
    policy = retry_policy or RetryPolicy()
    dataset_list = datasets or (["ohlcv"] if market == "spot" else ["ohlcv", "mark", "index", "funding", "oi"])
    if symbols is not None:
        symbol_list = symbols
    elif all_symbols:
        try:
            instruments = adapter.fetch_spot_instruments() if market == "spot" else adapter.fetch_usdt_perp_instruments()
            native_col = "native_symbol" if "native_symbol" in instruments.columns else "symbol"
            symbol_list = instruments[native_col].astype(str).sort_values().tolist()
        except Exception:
            symbol_list = scheduler.symbols_from_local_state(exchange, market=market)
    else:
        symbol_list = scheduler.symbols_from_local_state(exchange, market=market)
    for symbol in symbol_list:
        for dataset in dataset_list:
            for job in scheduler.plan_update(exchange, symbol, dataset, timeframe, utc_ts(end), market=market).jobs:
                try:
                    execute_fetch_job(job, adapter, store, state_store, coverage_store, policy, limiter)
                except Exception:
                    if not continue_on_error:
                        raise
                    break


def fetch_status(store: ResearchDataStore | None = None) -> pd.DataFrame:
    state_store = FetchStateStore(store or ResearchDataStore())
    return state_store.read()
