from __future__ import annotations

import json
from pathlib import Path

from bt.exec.runtime.app import run_exec_session


def _write_sample_csv(path: Path) -> None:
    path.write_text(
        'ts,symbol,open,high,low,close,volume\n'
        '2026-01-01T00:00:00Z,BTCUSDT,100,101,99,100,1\n'
        '2026-01-01T00:01:00Z,BTCUSDT,100,102,99,101,1\n'
        '2026-01-01T00:02:00Z,BTCUSDT,101,103,100,102,1\n'
        '2026-01-01T00:03:00Z,BTCUSDT,102,104,101,103,1\n',
        encoding='utf-8',
    )


def test_exec_shadow_and_paper_smoke_artifacts(tmp_path: Path) -> None:
    data = tmp_path / 'bars.csv'
    _write_sample_csv(data)

    shadow_dir = Path(
        run_exec_session(
            config_path='configs/exec.yaml',
            data_path=str(data),
            mode='shadow',
            out_dir=str(tmp_path / 'shadow_runs'),
            override_paths=['configs/exec/shadow_simulated.yaml'],
        )
    )
    paper_dir = Path(
        run_exec_session(
            config_path='configs/exec.yaml',
            data_path=str(data),
            mode='paper_simulated',
            out_dir=str(tmp_path / 'paper_runs'),
            override_paths=['configs/exec/paper_simulated.yaml'],
        )
    )

    for run_dir in (shadow_dir, paper_dir):
        assert (run_dir / 'run_manifest.json').exists()
        assert (run_dir / 'run_status.json').exists()
        assert (run_dir / 'decisions.jsonl').exists()
        assert (run_dir / 'orders.jsonl').exists()
        assert (run_dir / 'fills.jsonl').exists()
        assert (run_dir / 'heartbeat.jsonl').exists()
        status = json.loads((run_dir / "run_status.json").read_text(encoding="utf-8"))
        for key in (
            "trading_enabled",
            "mutation_enabled",
            "read_only",
            "frozen",
            "startup_gate_result",
            "private_stream_ready",
            "public_stream_ready",
            "canary_enabled",
            "live_controls_enabled",
        ):
            assert key in status

    assert (paper_dir / 'fills.jsonl').read_text(encoding='utf-8').strip() != ''
