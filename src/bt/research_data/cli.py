"""CLI for research data backfills, universes, panels, and validation."""
from __future__ import annotations

import argparse

from bt.research_data.config import DEFAULT_EXCHANGE, DEFAULT_START_TS, DEFAULT_TIMEFRAME, RAW_DATASETS, SPOT_RAW_DATASETS
from bt.research_data.exchanges.factory import get_adapter
from bt.research_data.fetching.orchestration import fetch_backfill, fetch_status, fetch_update
from bt.research_data.instruments import write_instrument_manifest
from bt.research_data.jobs.backfill import backfill, backfill_stable
from bt.research_data.jobs.build_panel import build_panels
from bt.research_data.jobs.build_universe import build_volatile_universe
from bt.research_data.jobs.coverage import build_coverage, write_coverage_dashboard
from bt.research_data.jobs.materialize import materialize_volatile_panel
from bt.research_data.jobs.state_features import (
    build_htf_context_features,
    build_l7_h1_kernel_features,
    build_panel_state_features,
    build_registered_panel_features,
)
from bt.research_data.jobs.validate import validate_all
from bt.research_data.live import aggregate_liquidations, collect_liquidations


def _csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m bt.research_data.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("backfill")
    p.add_argument("--market", default="perp", choices=["perp", "spot"])
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--symbols", required=True, type=_csv)
    p.add_argument("--start", default=str(DEFAULT_START_TS.date()))
    p.add_argument("--end", default="now")
    p.add_argument("--datasets", default=None, type=_csv)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)

    p = sub.add_parser("backfill-stable")
    p.add_argument("--market", default="perp", choices=["perp", "spot"])
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--start", default=str(DEFAULT_START_TS.date()))
    p.add_argument("--end", default="now")
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)

    p = sub.add_parser("build-volatile-universe")
    p.add_argument("--market", default="perp", choices=["perp", "spot"])
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--start", default=str(DEFAULT_START_TS.date()))
    p.add_argument("--end", default="now")
    p.add_argument("--rebalance-freq", default="2h")
    p.add_argument("--lookback", default="24h")
    p.add_argument("--top-gainers", type=int, default=20)
    p.add_argument("--top-losers", type=int, default=10)
    p.add_argument("--min-age-days", type=int, default=30)
    p.add_argument("--min-median-dollar-volume-7d", type=float, default=5_000_000)

    p = sub.add_parser("build-panel")
    p.add_argument("--market", default="perp", choices=["perp", "spot"])
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--symbols", required=True, type=_csv)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)

    p = sub.add_parser("materialize-volatile-panel")
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--membership-path", default=None)
    p.add_argument("--start", default=None)
    p.add_argument("--end", default="now")
    p.add_argument("--row-group-size", type=int, default=120_000)

    p = sub.add_parser("build-state-features")
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--symbols", default=None, type=_csv)
    p.add_argument("--universe", default="all", choices=["all", "stable", "volatile", "volatile-active"])
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)

    p = sub.add_parser("build-l7h1-kernel-features")
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--signal-timeframes", default="15m,1h", type=_csv)
    p.add_argument("--symbols", default=None, type=_csv)
    p.add_argument("--universe", default="all", choices=["all", "stable", "volatile", "volatile-active"])
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)

    p = sub.add_parser("build-htf-context-features")
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--signal-timeframes", default="5m,15m,1h", type=_csv)
    p.add_argument("--symbols", default=None, type=_csv)
    p.add_argument("--universe", default="stable", choices=["all", "stable", "volatile", "volatile-active"])
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)

    p = sub.add_parser("build-registered-features")
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--features", default="engine_state,htf_context,l7h1_csi_displacement", type=_csv)
    p.add_argument("--signal-timeframes", default="5m,15m,1h", type=_csv)
    p.add_argument("--symbols", default=None, type=_csv)
    p.add_argument("--universe", default="all", choices=["all", "stable", "volatile", "volatile-active"])
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)

    p = sub.add_parser("validate")
    p.add_argument("--exchange", default="all")
    p.add_argument("--all", action="store_true")

    p = sub.add_parser("coverage")
    p.add_argument("--exchange", default="all")
    p.add_argument("--all", action="store_true")

    sub.add_parser("dashboard")

    p = sub.add_parser("fetch-backfill")
    p.add_argument("--market", default="perp", choices=["perp", "spot"])
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--dataset", required=True, choices=tuple(sorted(set(RAW_DATASETS) | set(SPOT_RAW_DATASETS))))
    p.add_argument("--symbol", required=True)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--start", default=str(DEFAULT_START_TS.date()))
    p.add_argument("--end", default="now")

    p = sub.add_parser("fetch-update")
    p.add_argument("--market", default="perp", choices=["perp", "spot"])
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    p.add_argument("--all", action="store_true")
    p.add_argument("--symbols", type=_csv)
    p.add_argument("--datasets", type=_csv)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--end", default="now")
    p.add_argument("--fail-fast", action="store_true")

    sub.add_parser("fetch-status")

    p = sub.add_parser("refresh-instruments")
    p.add_argument("--exchange", default=DEFAULT_EXCHANGE, choices=["all", "binance", "bybit", "okx"])
    p.add_argument("--market", default="perp", choices=["perp", "spot", "all"])

    p = sub.add_parser("collect-liquidations")
    p.add_argument("--exchange", required=True, choices=["binance", "bybit", "okx"])
    p.add_argument("--symbols", required=True, type=_csv)

    p = sub.add_parser("aggregate-liquidations")
    p.add_argument("--exchange", required=True, choices=["binance", "bybit", "okx"])
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)

    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if args.command == "backfill":
        datasets = args.datasets or (list(SPOT_RAW_DATASETS) if args.market == "spot" else list(RAW_DATASETS))
        backfill(args.exchange, args.symbols, args.start, args.end, datasets, args.timeframe, market=args.market)
    elif args.command == "backfill-stable":
        backfill_stable(args.exchange, args.start, args.end, args.timeframe, market=args.market)
    elif args.command == "build-volatile-universe":
        build_volatile_universe(
            args.exchange,
            args.start,
            args.end,
            args.rebalance_freq,
            args.lookback,
            args.top_gainers,
            args.top_losers,
            args.min_age_days,
            args.min_median_dollar_volume_7d,
            market=args.market,
        )
    elif args.command == "build-panel":
        build_panels(args.exchange, args.symbols, args.timeframe, market=args.market)
    elif args.command == "materialize-volatile-panel":
        path = materialize_volatile_panel(
            args.exchange,
            args.timeframe,
            membership_path=args.membership_path,
            start=args.start,
            end=args.end,
            row_group_size=args.row_group_size,
        )
        print(str(path))
    elif args.command == "build-state-features":
        report = build_panel_state_features(
            args.exchange,
            args.timeframe,
            symbols=args.symbols,
            universe=args.universe,
            start=args.start,
            end=args.end,
        )
        print(report.to_string(index=False))
    elif args.command == "build-l7h1-kernel-features":
        report = build_l7_h1_kernel_features(
            args.exchange,
            args.timeframe,
            signal_timeframes=args.signal_timeframes,
            symbols=args.symbols,
            universe=args.universe,
            start=args.start,
            end=args.end,
        )
        print(report.to_string(index=False))
    elif args.command == "build-htf-context-features":
        report = build_htf_context_features(
            args.exchange,
            args.timeframe,
            signal_timeframes=args.signal_timeframes,
            symbols=args.symbols,
            universe=args.universe,
            start=args.start,
            end=args.end,
        )
        print(report.to_string(index=False))
    elif args.command == "build-registered-features":
        report = build_registered_panel_features(
            args.exchange,
            args.timeframe,
            features=args.features,
            feature_params={
                "htf_context": {"signal_timeframes": args.signal_timeframes},
                "l7h1_csi_displacement": {
                    "signal_timeframes": [tf for tf in args.signal_timeframes if tf in {"15m", "1h"}] or ("15m", "1h")
                },
            },
            symbols=args.symbols,
            universe=args.universe,
            start=args.start,
            end=args.end,
        )
        print(report.to_string(index=False))
    elif args.command == "validate":
        report = validate_all("all" if args.all else args.exchange)
        print(report.to_string(index=False))
    elif args.command == "coverage":
        from bt.research_data.storage import ResearchDataStore

        store = ResearchDataStore()
        report = build_coverage(store, exchange=None if args.all or args.exchange == "all" else args.exchange)
        print(report.to_string(index=False))
    elif args.command == "dashboard":
        output = write_coverage_dashboard()
        print(str(output))
    elif args.command == "fetch-backfill":
        fetch_backfill(args.exchange, args.dataset, args.symbol, args.timeframe, args.start, args.end, market=args.market)
    elif args.command == "fetch-update":
        fetch_update(
            args.exchange,
            all_symbols=args.all,
            symbols=args.symbols,
            datasets=args.datasets,
            timeframe=args.timeframe,
            end=args.end,
            market=args.market,
            continue_on_error=not args.fail_fast,
        )
    elif args.command == "fetch-status":
        report = fetch_status()
        print(report.to_string(index=False))
    elif args.command == "refresh-instruments":
        exchanges = ["binance", "bybit", "okx"] if args.exchange == "all" else [args.exchange]
        frames = []
        from bt.research_data.storage import ResearchDataStore

        store = ResearchDataStore()
        for exchange in exchanges:
            markets = ["perp", "spot"] if args.market == "all" else [args.market]
            for market in markets:
                if market == "spot" and exchange == "okx":
                    continue
                adapter = get_adapter(exchange, market=market)
                instruments = (
                    adapter.fetch_spot_instruments()
                    if market == "spot"
                    else adapter.fetch_usdt_perp_instruments()
                )
                write_instrument_manifest(store, instruments)
                frames.append(instruments)
        if frames:
            import pandas as pd

            print(pd.concat(frames, ignore_index=True).to_string(index=False))
    elif args.command == "collect-liquidations":
        collect_liquidations(args.exchange, args.symbols)
    elif args.command == "aggregate-liquidations":
        aggregated = aggregate_liquidations(args.exchange, args.timeframe)
        print(aggregated.to_string(index=False))


if __name__ == "__main__":
    main()
