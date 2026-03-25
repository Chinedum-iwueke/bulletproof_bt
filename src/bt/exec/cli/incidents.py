from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.exec.observability.incidents import load_incidents


def list_incidents(run_dir: Path, limit: int = 20) -> list[dict[str, object]]:
    rows = load_incidents(run_dir / "incidents.jsonl")
    recent = rows[-limit:]
    return [
        {
            "ts": row.get("ts"),
            "severity": row.get("severity"),
            "incident_type": row.get("incident_type"),
            "message": row.get("message"),
            "taxonomy": row.get("taxonomy"),
        }
        for row in recent
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Show recent runtime incidents.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    print(json.dumps(list_incidents(Path(args.run_dir), limit=args.limit), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
