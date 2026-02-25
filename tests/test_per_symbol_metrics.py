from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.metrics.per_symbol import write_per_symbol_metrics


def test_write_per_symbol_metrics_creates_symbol_folders(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)

    trades = pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "pnl_net": 10.0, "pnl_price": 11.0, "fees_paid": 1.0, "slippage": 0.2},
            {"symbol": "BTCUSDT", "pnl_net": -5.0, "pnl_price": -4.5, "fees_paid": 0.5, "slippage": 0.1},
            {"symbol": "ETHUSDT", "pnl_net": 7.0, "pnl_price": 7.4, "fees_paid": 0.4, "slippage": 0.05},
        ]
    )
    trades.to_csv(run_dir / "trades.csv", index=False)

    out_path = write_per_symbol_metrics(run_dir)

    manifest = json.loads((out_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert manifest["total_symbols"] == 2

    btc_metrics = json.loads((out_path / "BTCUSDT" / "metrics.json").read_text(encoding="utf-8"))
    assert btc_metrics["total_trades"] == 2
    assert btc_metrics["net_pnl"] == 5.0
    assert btc_metrics["fee_total"] == 1.5

    eth_metrics = json.loads((out_path / "ETHUSDT" / "metrics.json").read_text(encoding="utf-8"))
    assert eth_metrics["total_trades"] == 1
    assert eth_metrics["net_pnl"] == 7.0


def test_write_per_symbol_metrics_handles_empty_trades(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "trades.csv").write_text("symbol,pnl_net\n", encoding="utf-8")

    out_path = write_per_symbol_metrics(run_dir)
    manifest = json.loads((out_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["symbols"] == []
    assert manifest["total_symbols"] == 0
