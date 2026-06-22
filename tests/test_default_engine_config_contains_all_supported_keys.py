from __future__ import annotations

from pathlib import Path

import yaml

from bt.api import run_backtest


def test_default_engine_config_contains_supported_keys_and_smoke_runs(tmp_path: Path) -> None:
    config_path = Path("configs/engine.yaml")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    # Top-level sections expected in the default config.
    for section in ("data", "execution", "risk", "benchmark", "metrics", "outputs", "strategy"):
        assert section in config

    data = config["data"]
    for key in ("mode", "symbols_subset", "max_symbols", "date_range", "row_limit_per_symbol", "chunksize"):
        assert key in data

    execution = config["execution"]
    for key in ("intrabar_mode", "spread_mode"):
        assert key in execution

    risk = config["risk"]
    for key in (
        "mode",
        "r_per_trade",
        "max_positions",
        "max_leverage",
        "stop_resolution",
        "margin_buffer_tier",
        "slippage_k_proxy",
        "min_stop_distance_pct",
        "max_notional_pct_equity",
        "max_gross_notional_pct_equity",
        "maintenance_free_margin_pct",
        "stop",
    ):
        assert key in risk
    assert isinstance(risk["stop"], dict)

    benchmark = config["benchmark"]
    for key in ("enabled", "symbol", "price_field", "initial_equity", "fee_model"):
        assert key in benchmark

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "runs"),
        )
    )

    assert run_dir.exists()
    for artifact in (
        "config_used.yaml",
        "decisions.jsonl",
        "fills.jsonl",
        "trades.csv",
        "equity.csv",
        "performance.json",
        "sanity.json",
        "run_status.json",
    ):
        assert (run_dir / artifact).exists()
