from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def build_status_view(run_dir: Path) -> dict[str, object]:
    status = _load_json(run_dir / "run_status.json")
    hb_last = None
    hb_path = run_dir / "heartbeat.jsonl"
    if hb_path.exists():
        lines = [line for line in hb_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if lines:
            hb_last = json.loads(lines[-1]).get("ts")

    rec_last = None
    rec_path = run_dir / "reconciliation.jsonl"
    if rec_path.exists():
        lines = [line for line in rec_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if lines:
            rec_last = json.loads(lines[-1]).get("ts")

    return {
        "run_id": status.get("run_id", run_dir.name),
        "mode": status.get("mode"),
        "environment": status.get("environment"),
        "frozen": status.get("frozen", False),
        "read_only": status.get("read_only", True),
        "trading_enabled": status.get("trading_enabled", False),
        "startup_gate_result": status.get("startup_gate_result"),
        "latest_heartbeat_ts": hb_last,
        "latest_reconciliation_ts": rec_last,
        "private_stream_ready": status.get("private_stream_ready"),
        "public_stream_ready": status.get("public_stream_ready"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Show concise execution status for a run.")
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    payload = build_status_view(Path(args.run_dir))
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
