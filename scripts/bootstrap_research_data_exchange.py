#!/usr/bin/env python3
"""Bootstrap one exchange into the local research_data library.

The job is sequential and checkpointed through fetch_state.parquet. It is safe
to re-run after interruption and can run concurrently with other exchanges.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.research_data.config import RAW_DATASETS
from bt.research_data.exchanges.factory import get_adapter
from bt.research_data.fetching.orchestration import fetch_backfill
from bt.research_data.instruments import write_instrument_manifest, write_stable_universe
from bt.research_data.jobs.build_panel import build_panels
from bt.research_data.jobs.build_universe import build_volatile_universe
from bt.research_data.jobs.materialize import materialize_volatile_panel
from bt.research_data.jobs.state_features import build_l7_h1_kernel_features, build_panel_state_features
from bt.research_data.storage import ResearchDataStore


def log(message: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    print(f"{now} | {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", required=True, choices=("binance", "bybit", "okx"))
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--stable-start", default="2021-01-01")
    parser.add_argument("--volatile-start", default="2025-01-01")
    parser.add_argument("--end", default="now")
    parser.add_argument("--skip-panels", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    exchange = args.exchange.lower()
    store = ResearchDataStore()
    store.ensure_layout()
    adapter = get_adapter(exchange)

    log(f"refreshing {exchange} instruments")
    instruments = adapter.fetch_usdt_perp_instruments()
    write_instrument_manifest(store, instruments)
    stable = write_stable_universe(store, instruments, exchange=exchange)
    stable_symbols = stable.loc[stable["available"], "native_symbol"].astype(str).drop_duplicates().tolist()
    all_symbols = instruments["native_symbol"].astype(str).drop_duplicates().tolist()
    volatile_seed_symbols = [symbol for symbol in all_symbols if symbol not in set(stable_symbols)]
    log(
        f"exchange={exchange} stable_symbols={len(stable_symbols)} "
        f"all_symbols={len(all_symbols)} volatile_seed_symbols={len(volatile_seed_symbols)}"
    )

    for symbol in stable_symbols:
        for dataset in RAW_DATASETS:
            log(f"{exchange} stable backfill start symbol={symbol} dataset={dataset} start={args.stable_start}")
            fetch_backfill(exchange, dataset, symbol, args.timeframe, args.stable_start, args.end, store=store)
            log(f"{exchange} stable backfill done symbol={symbol} dataset={dataset}")

    for symbol in volatile_seed_symbols:
        for dataset in RAW_DATASETS:
            log(f"{exchange} volatile raw backfill start symbol={symbol} dataset={dataset} start={args.volatile_start}")
            fetch_backfill(exchange, dataset, symbol, args.timeframe, args.volatile_start, args.end, store=store)
            log(f"{exchange} volatile raw backfill done symbol={symbol} dataset={dataset}")

    log(f"{exchange} building volatile universe membership from {args.volatile_start}")
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
    )
    membership_symbols = sorted(membership["symbol"].astype(str).drop_duplicates().tolist()) if not membership.empty else []
    log(f"{exchange} volatile membership symbols={len(membership_symbols)}")

    if not args.skip_panels:
        panel_symbols = sorted(set(stable_symbols) | set(membership_symbols))
        log(f"{exchange} building canonical panels symbols={len(panel_symbols)}")
        build_panels(exchange, panel_symbols, args.timeframe, store)
        log(f"{exchange} precomputing stable causal state features symbols={len(stable_symbols)}")
        build_panel_state_features(exchange, args.timeframe, symbols=stable_symbols, store=store)
        log(f"{exchange} precomputing volatile L7-H1 member kernel features symbols={len(membership_symbols)}")
        build_l7_h1_kernel_features(exchange, args.timeframe, symbols=membership_symbols, store=store)
        log(f"{exchange} materializing active volatile panel symbols={len(membership_symbols)}")
        path = materialize_volatile_panel(
            exchange,
            args.timeframe,
            start=args.volatile_start,
            end=args.end,
            store=store,
        )
        log(f"{exchange} materialized active volatile panel path={path}")
        log(f"{exchange} precomputing materialized volatile causal state features")
        build_panel_state_features(exchange, args.timeframe, universe="volatile-active", store=store)
        log(f"{exchange} precomputing materialized volatile L7-H1 kernel features")
        build_l7_h1_kernel_features(exchange, args.timeframe, universe="volatile-active", store=store)

    log(f"{exchange} research_data bootstrap complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
