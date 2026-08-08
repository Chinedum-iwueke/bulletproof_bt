#!/usr/bin/env python3
"""Parallel resumable bootstrap for spot OHLCV research_data."""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.research_data.config import SPOT_RAW_DATASETS
from bt.research_data.exchanges.factory import get_adapter
from bt.research_data.fetching.orchestration import fetch_backfill
from bt.research_data.instruments import write_instrument_manifest, write_stable_universe
from bt.research_data.jobs.build_panel import build_panels
from bt.research_data.jobs.build_universe import build_volatile_universe
from bt.research_data.storage import ResearchDataStore


MARKET = "spot"


def log(message: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    print(f"{now} | {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", required=True, choices=("binance", "bybit"))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--stable-start", default="2021-01-01")
    parser.add_argument("--volatile-start", default="2025-01-01")
    parser.add_argument("--end", default="now")
    parser.add_argument("--skip-panels", action="store_true")
    parser.add_argument("--skip-volatile-universe", action="store_true")
    return parser.parse_args()


def run_fetch_task(task: tuple[str, str, str, str, str, str, float]) -> tuple[str, str, str]:
    exchange, symbol, dataset, timeframe, start, end, rate_limit_seconds = task
    store = ResearchDataStore()
    try:
        fetch_backfill(
            exchange,
            dataset,
            symbol,
            timeframe,
            start,
            end,
            market=MARKET,
            store=store,
            rate_limit_seconds=rate_limit_seconds,
        )
    except Exception as exc:
        raise RuntimeError(f"{type(exc).__name__}: {exc}") from None
    return symbol, dataset, start


def run_tasks(label: str, tasks: list[tuple[str, str, str, str, str, str, float]], workers: int) -> None:
    if not tasks:
        return
    log(f"{label} tasks={len(tasks)} workers={workers}")
    completed = 0
    failed = 0
    with futures.ProcessPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(run_fetch_task, task): task for task in tasks}
        for future in futures.as_completed(future_map):
            exchange, symbol, dataset, _timeframe, start, _end, _rate_limit = future_map[future]
            try:
                future.result()
            except Exception as exc:
                failed += 1
                log(f"{label} failed exchange={exchange} symbol={symbol} dataset={dataset} start={start} error={exc}")
                continue
            completed += 1
            log(f"{label} done {completed}/{len(tasks)} exchange={exchange} symbol={symbol} dataset={dataset}")
    if failed:
        log(f"{label} completed with failures success={completed} failed={failed} total={len(tasks)}")


def stable_start_for_symbol(stable: pd.DataFrame, configured_start: str) -> dict[str, str]:
    configured = pd.Timestamp(configured_start, tz="UTC")
    starts: dict[str, str] = {}
    for row in stable.itertuples(index=False):
        symbol = str(row.native_symbol)
        first_seen = pd.to_datetime(getattr(row, "first_seen_ts", pd.NaT), utc=True, errors="coerce")
        start = max(configured, first_seen) if pd.notna(first_seen) else configured
        starts[symbol] = start.isoformat()
    return starts


def main() -> int:
    args = parse_args()
    exchange = args.exchange.lower()
    store = ResearchDataStore()
    store.ensure_layout()
    adapter = get_adapter(exchange, market=MARKET)

    log(f"refreshing {exchange} spot instruments")
    instruments = adapter.fetch_spot_instruments()
    write_instrument_manifest(store, instruments)
    stable = write_stable_universe(store, instruments, exchange=exchange, market=MARKET)
    stable_symbols = stable.loc[stable["available"], "native_symbol"].astype(str).drop_duplicates().tolist()
    stable_starts = stable_start_for_symbol(stable.loc[stable["available"]].copy(), args.stable_start)
    all_symbols = instruments["native_symbol"].astype(str).drop_duplicates().tolist()
    volatile_seed_symbols = [symbol for symbol in all_symbols if symbol not in set(stable_symbols)]
    log(
        f"exchange={exchange} market=spot stable_symbols={len(stable_symbols)} "
        f"all_symbols={len(all_symbols)} volatile_seed_symbols={len(volatile_seed_symbols)} "
        f"volatile_start={args.volatile_start}"
    )

    rate_limit_seconds = 0.10 if exchange == "binance" else 0.20
    stable_tasks = [
        (exchange, symbol, dataset, args.timeframe, stable_starts.get(symbol, args.stable_start), args.end, rate_limit_seconds)
        for symbol in stable_symbols
        for dataset in SPOT_RAW_DATASETS
    ]
    run_tasks(f"{exchange} spot stable", stable_tasks, args.workers)

    volatile_seed_tasks = [
        (exchange, symbol, dataset, args.timeframe, args.volatile_start, args.end, rate_limit_seconds)
        for symbol in volatile_seed_symbols
        for dataset in SPOT_RAW_DATASETS
    ]
    run_tasks(f"{exchange} spot volatile-seed-raw", volatile_seed_tasks, args.workers)

    membership_symbols: list[str] = []
    if not args.skip_volatile_universe:
        log(f"{exchange} building spot volatile universe membership from {args.volatile_start}")
        membership = build_volatile_universe(
            exchange,
            pd.Timestamp(args.volatile_start, tz="UTC"),
            args.end,
            "2h",
            "24h",
            20,
            10,
            30,
            5_000_000,
            store,
            market=MARKET,
        )
        membership_symbols = sorted(membership["symbol"].astype(str).drop_duplicates().tolist()) if not membership.empty else []
        log(f"{exchange} spot volatile membership symbols={len(membership_symbols)}")

    if not args.skip_panels:
        panel_symbols = sorted(set(stable_symbols) | set(membership_symbols))
        log(f"{exchange} building spot canonical panels symbols={len(panel_symbols)}")
        build_panels(exchange, panel_symbols, args.timeframe, store, market=MARKET)

    log(f"{exchange} spot research_data bootstrap complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
