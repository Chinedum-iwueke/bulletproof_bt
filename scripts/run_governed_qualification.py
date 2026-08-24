#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.governance.qualification import build_qualification_snapshot, execute_qualification


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    snapshot = commands.add_parser("snapshot")
    snapshot.add_argument("--output", type=Path, required=True)
    snapshot.add_argument("--rows", type=int, default=720)
    execute = commands.add_parser("execute")
    execute.add_argument("--proposal", type=Path, required=True)
    execute.add_argument("--repository-root", type=Path, default=Path.cwd())
    execute.add_argument("--data", type=Path, required=True)
    execute.add_argument("--output", type=Path, required=True)
    execute.add_argument("--config", type=Path, default=Path("configs/engine.yaml"))
    args = parser.parse_args()
    if args.command == "snapshot":
        result = build_qualification_snapshot(args.output, rows=args.rows)
    else:
        result = execute_qualification(
            proposal=json.loads(args.proposal.read_text(encoding="utf-8")),
            repository_root=args.repository_root, data_root=args.data,
            output_root=args.output, config_path=args.config,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
