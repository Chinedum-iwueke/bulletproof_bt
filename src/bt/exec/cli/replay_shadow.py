from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.exec.shadow import replay_journal
from bt.logging.formatting import write_json_deterministic


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify and replay a sealed shadow journal.")
    parser.add_argument("journal", type=Path)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    report = replay_journal(args.journal)
    output = args.output or args.journal.with_name("shadow_replay_report.json")
    write_json_deterministic(output, report)
    print(json.dumps(report, sort_keys=True))
    if not report["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
