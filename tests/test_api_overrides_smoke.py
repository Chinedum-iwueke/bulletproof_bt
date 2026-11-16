from __future__ import annotations

from pathlib import Path
import json

import yaml
import pandas as pd

from bt.api import run_backtest, run_grid


def test_public_runner_preserves_smoke_callback_boundary(tmp_path: Path) -> None:
    from scripts.run_alpha_research_assignment import verify_smoke_callback_boundary

    frame = pd.read_csv("data/curated/sample.csv")
    times = sorted(pd.to_datetime(frame["ts"], utc=True).unique())
    boundary = pd.Timestamp(times[len(times) // 2]).isoformat()
    override_path = tmp_path / "smoke-boundary.yaml"
    override_path.write_text(yaml.safe_dump({"data": {"signal_generation_end_ts": boundary}}))
    run_dir = Path(run_backtest(
        config_path="configs/engine.yaml", data_path="data/curated/sample.csv",
        out_dir=str(tmp_path / "runs"), override_paths=[str(override_path)],
        run_name="smoke_boundary",
    ))
    receipt = verify_smoke_callback_boundary(run_dir, boundary)
    assert receipt["strategy_callback_count"] == len(times) // 2
    assert receipt["outcome_tail_bar_count"] == len(times) - len(times) // 2
    equity = pd.read_csv(run_dir / "equity.csv")
    assert pd.to_datetime(equity["ts"], utc=True).max() == times[-1]


def test_run_backtest_override_written_to_config_used(tmp_path: Path) -> None:
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump({"risk": {"max_positions": 2}}, sort_keys=False), encoding="utf-8")

    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "runs"),
            override_paths=[str(override_path)],
            run_name="override_smoke",
        )
    )

    cfg = yaml.safe_load((run_dir / "config_used.yaml").read_text(encoding="utf-8"))
    assert cfg["risk"]["max_positions"] == 2


def test_run_grid_override_and_outputs(tmp_path: Path) -> None:
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump({"risk": {"max_positions": 2}}, sort_keys=False), encoding="utf-8")

    experiment_path = tmp_path / "experiment.yaml"
    experiment_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "name": "smoke",
                "fixed": {"strategy": {"name": "coinflip", "p_trade": 0.0}},
                "grid": {"strategy.seed": [7]},
                "run_naming": {"template": "seed{strategy.seed}"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    exp_dir = Path(
        run_grid(
            config_path="configs/engine.yaml",
            experiment_path=str(experiment_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "grid"),
            override_paths=[str(override_path)],
        )
    )

    assert (exp_dir / "summary.csv").exists()
    assert (exp_dir / "summary.json").exists()
    assert any((exp_dir / "runs").iterdir())


def test_run_backtest_config_used_omits_legacy_risk_key_when_not_explicit(tmp_path: Path) -> None:
    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "runs"),
            run_name="canonical_risk_smoke",
        )
    )

    cfg = yaml.safe_load((run_dir / "config_used.yaml").read_text(encoding="utf-8"))
    assert cfg["risk"]["r_per_trade"] == 0.005
    assert "risk_per_trade_pct" not in cfg["risk"]
    assert "risk_per_trade_pct" not in cfg


def test_run_backtest_wires_resolved_risk_buffers_without_hidden_defaults(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, float | int] = {}

    from bt.risk import risk_engine as risk_engine_module

    original_init = risk_engine_module.RiskEngine.__init__

    def _capturing_init(self, *args, **kwargs):
        captured["margin_buffer_tier"] = kwargs["margin_buffer_tier"]
        captured["slippage_k_proxy"] = kwargs["slippage_k_proxy"]
        return original_init(self, *args, **kwargs)

    monkeypatch.setattr(risk_engine_module.RiskEngine, "__init__", _capturing_init)

    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "runs"),
            run_name="risk_buffer_defaults_smoke",
        )
    )

    assert captured["margin_buffer_tier"] == 1
    assert captured["slippage_k_proxy"] == 0.0

    cfg = yaml.safe_load((run_dir / "config_used.yaml").read_text(encoding="utf-8"))
    assert cfg["risk"]["margin_buffer_tier"] == 1
    assert cfg["risk"]["slippage_k_proxy"] == 0.0

    fill_lines = (run_dir / "fills.jsonl").read_text(encoding="utf-8").strip().splitlines()
    if fill_lines:
        first_fill = json.loads(fill_lines[0])
        assert first_fill["metadata"]["margin_fee_buffer"] > 0.0
        assert first_fill["metadata"]["margin_slippage_buffer"] > 0.0
