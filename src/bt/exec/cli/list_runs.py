from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.exec.logging.export_bundle import list_runs


def main() -> None:
    parser = argparse.ArgumentParser(description="List recent execution runs.")
    parser.add_argument("--run-root", default="outputs/exec_runs")
    args = parser.parse_args()
    print(json.dumps(list_runs(Path(args.run_root)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
