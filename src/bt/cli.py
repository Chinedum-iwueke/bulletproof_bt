from __future__ import annotations

import sys

from bt.contracts.research_specs import main as research_specs_main


def main() -> int:
    if len(sys.argv) >= 3 and sys.argv[1] == "portfolio":
        return portfolio_main(sys.argv[2:])
    return research_specs_main(sys.argv[1:])


def portfolio_main(argv: list[str]) -> int:
    import argparse
    import json

    from bt.portfolio_engine.deployment import run_portfolio_demo, run_portfolio_live
    from bt.portfolio_engine.runner import report_portfolio_run, run_portfolio_backtest

    parser = argparse.ArgumentParser(prog="bt portfolio")
    sub = parser.add_subparsers(dest="command", required=True)

    backtest = sub.add_parser("backtest")
    backtest.add_argument("--config", required=True)

    report = sub.add_parser("report")
    report.add_argument("--run-id", required=True)

    demo = sub.add_parser("demo")
    demo.add_argument("--config", required=True)

    live = sub.add_parser("live")
    live.add_argument("--config", required=True)

    args = parser.parse_args(argv)
    if args.command == "backtest":
        print(run_portfolio_backtest(args.config))
        return 0
    if args.command == "report":
        print(json.dumps(report_portfolio_run(args.run_id), indent=2, sort_keys=True))
        return 0
    if args.command == "demo":
        print(run_portfolio_demo(args.config))
        return 0
    if args.command == "live":
        print(run_portfolio_live(args.config))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
