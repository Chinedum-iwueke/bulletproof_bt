from __future__ import annotations

import json
from pathlib import Path

from bt.experiments import hypothesis_runner
from bt.hypotheses.contract import HypothesisContract


def test_execute_hypothesis_variant_runs_full_postprocessing(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "slug"
    run_dir.mkdir(parents=True)
    (run_dir / "config_used.yaml").write_text("strategy:\n  name: l1_h1_vol_floor_trend\n", encoding="utf-8")
    (run_dir / "performance.json").write_text(json.dumps({"expectancy_r": 0.1, "trades": 3}), encoding="utf-8")

    called: list[str] = []

    monkeypatch.setattr(hypothesis_runner, "run_backtest", lambda **_kwargs: str(run_dir))
    monkeypatch.setattr(hypothesis_runner, "validate_run_artifacts", lambda _run_dir: called.append("validate_run_artifacts"))
    monkeypatch.setattr(hypothesis_runner, "write_per_symbol_metrics", lambda _run_dir: called.append("write_per_symbol_metrics"))
    monkeypatch.setattr(hypothesis_runner, "write_summary_txt", lambda _run_dir: called.append("write_summary_txt"))
    monkeypatch.setattr(hypothesis_runner, "build_run_segment_rollups", lambda _run_dir, hypothesis_id=None: called.append(f"build_run_segment_rollups:{hypothesis_id}"))
    monkeypatch.setattr(
        hypothesis_runner,
        "write_run_manifest",
        lambda _run_dir, *, config, data_path: called.append(f"write_run_manifest:{data_path}:{config['strategy']['name']}"),
    )
    monkeypatch.setattr(hypothesis_runner, "write_artifacts_manifest", lambda _run_dir, *, config: called.append(f"write_artifacts_manifest:{config['strategy']['name']}"))

    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    spec = contract.to_run_specs()[0]

    result = hypothesis_runner.execute_hypothesis_variant(
        contract=contract,
        spec=spec,
        tier="Tier2",
        config_path="configs/engine.yaml",
        data_path="/tmp/data",
        out_root=str(tmp_path / "runs"),
    )

    assert result["num_trades"] == 3
    assert result["run_dir"] == str(run_dir)
    assert called == [
        "validate_run_artifacts",
        "write_per_symbol_metrics",
        "write_summary_txt",
        "write_run_manifest:/tmp/data:l1_h1_vol_floor_trend",
        "build_run_segment_rollups:l1_h1_vol_floor_trend",
        "write_artifacts_manifest:l1_h1_vol_floor_trend",
    ]
