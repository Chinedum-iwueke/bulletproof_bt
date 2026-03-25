from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.exec.logging.export_bundle import export_run_bundle
from bt.exec.logging.session_summary import build_session_summary, write_session_summary
from bt.exec.observability.incidents import load_incidents, summarize_incidents, write_incident_summary


def export_run(*, run_dir: Path, export_root: Path) -> Path:
    session_summary = build_session_summary(run_dir)
    write_session_summary(run_dir, session_summary)

    status_path = run_dir / "run_status.json"
    status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
    incidents = load_incidents(run_dir / "incidents.jsonl")
    incident_summary = summarize_incidents(run_id=session_summary.run_id, incidents=incidents, final_status=status)
    write_incident_summary(run_dir=run_dir, summary=incident_summary)

    return export_run_bundle(run_dir=run_dir, export_root=export_root)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create export bundle for one run.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--export-root", default="outputs/exec_exports")
    args = parser.parse_args()
    out = export_run(run_dir=Path(args.run_dir), export_root=Path(args.export_root))
    print(str(out))


if __name__ == "__main__":
    main()
