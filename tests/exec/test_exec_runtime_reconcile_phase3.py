from __future__ import annotations

import json
from pathlib import Path

from bt.exec.runtime.app import run_exec_session


def _write_sample_csv(path: Path) -> None:
    path.write_text(
        'ts,symbol,open,high,low,close,volume\n'
        '2026-01-01T00:00:00Z,BTCUSDT,100,101,99,100,1\n'
        '2026-01-01T00:01:00Z,BTCUSDT,100,102,99,101,1\n'
        '2026-01-01T00:02:00Z,BTCUSDT,101,103,100,102,1\n',
        encoding='utf-8',
    )


def test_runtime_writes_reconciliation_and_lineage(tmp_path: Path) -> None:
    data = tmp_path / "bars.csv"
    _write_sample_csv(data)
    state_db = tmp_path / "runtime.sqlite"
    override = tmp_path / "override.yaml"
    override.write_text(
        "\n".join(
            [
                "state:",
                f"  path: {state_db.as_posix()}",
                "reconcile:",
                "  enabled: true",
                "  interval_seconds: 1",
                "  policy: log_only",
            ]
        ),
        encoding="utf-8",
    )

    out_root = tmp_path / "runs"
    first_run_dir = Path(run_exec_session(config_path="configs/exec.yaml", data_path=str(data), mode="paper_simulated", out_dir=str(out_root), override_paths=["configs/exec/paper_simulated.yaml", str(override)], run_id="r1"))
    run_dir = Path(
        run_exec_session(config_path="configs/exec.yaml", data_path=str(data), mode="paper_simulated", out_dir=str(out_root), override_paths=["configs/exec/paper_simulated.yaml", str(override)], run_id="r2")
    )

    rec_lines = [line for line in (first_run_dir / "reconciliation.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rec_lines

    manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest.get("resumed_from_run_id") == "r1"
