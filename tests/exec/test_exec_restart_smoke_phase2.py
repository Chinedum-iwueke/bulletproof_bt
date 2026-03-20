from __future__ import annotations

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


def test_restart_resume_shadow_and_paper_simulated(tmp_path: Path) -> None:
    data = tmp_path / "bars.csv"
    _write_sample_csv(data)
    state_db = tmp_path / "runtime.sqlite"
    state_override = tmp_path / "state_override.yaml"
    state_override.write_text(
        f"state:\n  path: {state_db.as_posix()}\n",
        encoding="utf-8",
    )

    for mode, override in (("shadow", "configs/exec/shadow_simulated.yaml"), ("paper_simulated", "configs/exec/paper_simulated.yaml")):
        out_root = tmp_path / mode
        run_exec_session(
            config_path="configs/exec.yaml",
            data_path=str(data),
            mode=mode,
            out_dir=str(out_root),
            override_paths=[override, str(state_override)],
            run_id=f"{mode}-first",
        )
        run_exec_session(
            config_path="configs/exec.yaml",
            data_path=str(data),
            mode=mode,
            out_dir=str(out_root),
            override_paths=[override, str(state_override)],
            run_id=f"{mode}-second",
            # use same durable sqlite store across restarts
        )

        second_fills = (out_root / f"{mode}-second" / "fills.jsonl").read_text(encoding="utf-8").strip()
        if mode == "paper_simulated":
            assert second_fills == ""

    assert state_db.exists()
