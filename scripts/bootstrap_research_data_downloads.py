#!/usr/bin/env python3
"""Bootstrap Binance research_data downloads.

This is an operational helper for the first full data-lake bootstrap:

- stable universe: configured majors from 2021-01-01
- volatile candidate universe: all eligible Binance USDT perps from 2024-01-01

The job is intentionally sequential and checkpointed through the research_data
fetch_state manifest. It is safe to re-run after interruption.
"""
from __future__ import annotations

from pathlib import Path
import sys
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import pandas as pd

from bt.research_data.config import RAW_DATASETS
from bt.research_data.exchanges.factory import get_adapter
from bt.research_data.fetching.orchestration import fetch_backfill
from bt.research_data.instruments import write_instrument_manifest, write_stable_universe
from bt.research_data.jobs.build_panel import build_panels
from bt.research_data.jobs.build_universe import build_volatile_universe
from bt.research_data.jobs.materialize import materialize_volatile_panel
from bt.research_data.jobs.state_features import build_l7_h1_kernel_features, build_panel_state_features
from bt.research_data.storage import ResearchDataStore


EXCHANGE = "binance"
TIMEFRAME = "1m"
STABLE_START = "2021-01-01"
VOLATILE_START = "2025-01-01"
END = "now"


def log(message: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    print(f"{now} | {message}", flush=True)


def main() -> int:
    store = ResearchDataStore()
    store.ensure_layout()
    adapter = get_adapter(EXCHANGE)

    log("refreshing Binance instruments")
    instruments = adapter.fetch_usdt_perp_instruments()
    write_instrument_manifest(store, instruments)
    stable = write_stable_universe(store, instruments)
    stable_symbols = stable.loc[stable["available"], "native_symbol"].astype(str).drop_duplicates().tolist()
    all_symbols = instruments["native_symbol"].astype(str).drop_duplicates().tolist()
    volatile_seed_symbols = [symbol for symbol in all_symbols if symbol not in set(stable_symbols)]
    log(f"stable symbols={len(stable_symbols)} all_symbols={len(all_symbols)} volatile_seed_symbols={len(volatile_seed_symbols)}")

    for symbol in stable_symbols:
        for dataset in RAW_DATASETS:
            log(f"stable backfill start symbol={symbol} dataset={dataset} start={STABLE_START}")
            fetch_backfill(EXCHANGE, dataset, symbol, TIMEFRAME, STABLE_START, END, store=store)
            log(f"stable backfill done symbol={symbol} dataset={dataset}")

    for symbol in volatile_seed_symbols:
        for dataset in RAW_DATASETS:
            log(f"volatile raw backfill start symbol={symbol} dataset={dataset} start={VOLATILE_START}")
            fetch_backfill(EXCHANGE, dataset, symbol, TIMEFRAME, VOLATILE_START, END, store=store)
            log(f"volatile raw backfill done symbol={symbol} dataset={dataset}")

    log("building volatile universe membership from 2024-01-01")
    membership = build_volatile_universe(
        EXCHANGE,
        pd.Timestamp(VOLATILE_START, tz="UTC"),
        END,
        "2h",
        "24h",
        20,
        10,
        30,
        5_000_000,
        store,
    )
    membership_symbols = sorted(membership["symbol"].astype(str).drop_duplicates().tolist()) if not membership.empty else []
    log(f"volatile membership symbols={len(membership_symbols)}")

    panel_symbols = sorted(set(stable_symbols) | set(membership_symbols))
    log(f"building canonical panels symbols={len(panel_symbols)}")
    build_panels(EXCHANGE, panel_symbols, TIMEFRAME, store)
    log(f"precomputing stable causal state features symbols={len(stable_symbols)}")
    build_panel_state_features(EXCHANGE, TIMEFRAME, symbols=stable_symbols, store=store)
    log(f"precomputing volatile L7-H1 member kernel features symbols={len(membership_symbols)}")
    build_l7_h1_kernel_features(EXCHANGE, TIMEFRAME, symbols=membership_symbols, store=store)
    log(f"materializing active volatile panel symbols={len(membership_symbols)}")
    path = materialize_volatile_panel(
        EXCHANGE,
        TIMEFRAME,
        start=VOLATILE_START,
        end=END,
        store=store,
    )
    log(f"materialized active volatile panel path={path}")
    log("precomputing materialized volatile causal state features")
    build_panel_state_features(EXCHANGE, TIMEFRAME, universe="volatile-active", store=store)
    log("precomputing materialized volatile L7-H1 kernel features")
    build_l7_h1_kernel_features(EXCHANGE, TIMEFRAME, universe="volatile-active", store=store)
    log("research_data bootstrap complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
