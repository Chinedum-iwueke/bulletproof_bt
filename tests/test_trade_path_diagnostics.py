from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Trade
from bt.logging.trades import TradesCsvWriter


def test_trade_writer_populates_mfe_mae_r_and_reach_flags(tmp_path: Path) -> None:
    writer = TradesCsvWriter(tmp_path / "trades.csv", run_id="run_1", hypothesis_id="L1-H1B")
    trade = Trade(
        symbol="BTCUSDT",
        side=Side.BUY,
        entry_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        exit_ts=pd.Timestamp("2024-01-01T01:00:00Z"),
        entry_price=100.0,
        exit_price=103.0,
        qty=1.0,
        pnl=3.0,
        fees=0.5,
        slippage=0.1,
        mae_price=99.0,
        mfe_price=110.0,
        metadata={
            "risk_amount": 2.0,
            "entry_stop_distance": 2.0,
            "entry_stop_price": 98.0,
            "path_favorable_price": 110.0,
            "path_adverse_price": 99.0,
            "exit_reason": "stop_chandelier",
            "trail_activated": True,
            "trail_activation_mode": "bars",
            "holding_period_bars_signal": 4,
            "time_to_mfe_bars_signal": 2,
        },
    )
    writer.write_trade(trade)
    writer.close()

    with (tmp_path / "trades.csv").open("r", encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))

    assert row["run_id"] == "run_1"
    assert row["hypothesis_id"] == "L1-H1B"
    assert float(row["mfe_r"]) == 5.0
    assert float(row["mae_r"]) == 0.5
    assert row["reached_1r"] == "True"
    assert row["reached_2r"] == "True"
    assert row["reached_3r"] == "True"
