from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pandas as pd
import yaml

from bt.core.enums import Side
from bt.core.types import Trade
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import (
    TradesCsvWriter,
    make_run_id,
    prepare_run_dir,
    write_config_used,
)


def test_make_run_id_prefix_and_timestamp() -> None:
    run_id = make_run_id("test")
    assert run_id.startswith("test_")
    assert len(run_id.split("_")) == 3
    assert run_id.split("_")[1].isdigit()
    assert run_id.split("_")[2].isdigit()


def test_prepare_run_dir_creates_directory(tmp_path: Path) -> None:
    run_dir = prepare_run_dir(tmp_path, "run_20240101_000000")
    assert run_dir.exists()
    assert run_dir.is_dir()


def test_write_config_used_writes_yaml(tmp_path: Path) -> None:
    run_dir = prepare_run_dir(tmp_path, "run_20240101_000000")
    config = {"alpha": 1, "nested": {"beta": "two"}}
    write_config_used(run_dir, config)
    content = (run_dir / "config_used.yaml").read_text(encoding="utf-8")
    assert yaml.safe_load(content) == config


def test_jsonl_writer_writes_records(tmp_path: Path) -> None:
    class Example(Enum):
        ALPHA = 1

    @dataclass
    class Payload:
        name: str
        count: int

    path = tmp_path / "events.jsonl"
    writer = JsonlWriter(path)
    timestamp = pd.Timestamp("2024-01-01", tz="UTC")
    writer.write({"ts": timestamp, "side": Example.ALPHA, "payload": Payload("x", 2)})
    writer.write({"value": 2})
    writer.close()

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    record = json.loads(lines[0])
    assert record["ts"] == timestamp.isoformat()
    assert record["side"] == "ALPHA"
    assert record["payload"]["name"] == "x"


def test_trades_csv_writer_writes_trade(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path)
    trade = Trade(
        symbol="AAPL",
        side=Side.BUY,
        entry_ts=pd.Timestamp("2024-01-01T00:00:00", tz="UTC"),
        exit_ts=pd.Timestamp("2024-01-01T01:00:00", tz="UTC"),
        entry_price=100.0,
        exit_price=110.0,
        qty=2.0,
        pnl=20.0,
        fees=1.0,
        slippage=0.5,
        mae_price=None,
        mfe_price=None,
    )
    writer.write_trade(trade)
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == TradesCsvWriter._columns
    parsed = dict(zip(rows[0], rows[1], strict=True))
    assert parsed["entry_ts"] == trade.entry_ts.isoformat()
    assert parsed["exit_ts"] == trade.exit_ts.isoformat()
    assert parsed["symbol"] == "AAPL"
    assert parsed["side"] == "BUY"


def test_trades_csv_writer_expands_dynamic_columns_rectangularly(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path)
    base_trade = Trade(
        symbol="AAPL",
        side=Side.BUY,
        entry_ts=pd.Timestamp("2024-01-01T00:00:00", tz="UTC"),
        exit_ts=pd.Timestamp("2024-01-01T01:00:00", tz="UTC"),
        entry_price=100.0,
        exit_price=101.0,
        qty=1.0,
        pnl=1.0,
        fees=0.1,
        slippage=0.0,
        mae_price=None,
        mfe_price=None,
    )
    writer.write_trade(base_trade)
    writer.write_trade(
        Trade(
            symbol="MSFT",
            side=Side.BUY,
            entry_ts=pd.Timestamp("2024-01-02T00:00:00", tz="UTC"),
            exit_ts=pd.Timestamp("2024-01-02T01:00:00", tz="UTC"),
            entry_price=200.0,
            exit_price=202.0,
            qty=1.0,
            pnl=2.0,
            fees=0.1,
            slippage=0.0,
            mae_price=None,
            mfe_price=None,
            metadata={"entry_state_csi_pctile": 0.75},
        )
    )
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert "entry_state_csi_pctile" in rows[0]
    assert all(len(row) == len(rows[0]) for row in rows[1:])


def test_trades_csv_writer_maps_l1_entry_context_metadata(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path, run_id="run_1", hypothesis_id="L1-H1")
    writer.write_trade(
        Trade(
            symbol="BTCUSDT",
            side=Side.BUY,
            entry_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
            exit_ts=pd.Timestamp("2024-01-01T01:00:00Z"),
            entry_price=100.0,
            exit_price=104.0,
            qty=1.0,
            pnl=4.0,
            fees=0.5,
            slippage=0.1,
            mae_price=99.0,
            mfe_price=106.0,
            metadata={
                "strategy": "l1_h1_vol_floor_trend",
                "risk_amount": 10.0,
                "entry_stop_distance": 10.0,
                "entry_stop_price": 90.0,
                "rv_t": 0.012,
                "vol_pct_t": 0.91,
                "gate_pass": True,
                "trend_dir_t": 1,
                "atr_entry": 5.0,
                "tp_enabled": True,
                "tp_price": 110.0,
                "tp_distance": 10.0,
                "signal_bars_held": 48,
                "vwap_t": 101.5,
                "custom_quality_score": 0.77,
            },
        )
    )
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))

    assert row["identity_strategy_id"] == "l1_h1_vol_floor_trend"
    assert float(row["rv_t"]) == 0.012
    assert float(row["vol_pct_t"]) == 0.91
    assert row["gate_pass"] == "True"
    assert row["trend_dir_t"] == "1"
    assert float(row["atr_entry"]) == 5.0
    assert row["tp_enabled"] == "True"
    assert float(row["tp_price"]) == 110.0
    assert float(row["tp_distance"]) == 10.0
    assert float(row["execution_take_profit_price_initial"]) == 110.0
    assert row["holding_period_bars_signal"] == "48"
    assert row["path_bars_held"] == "48"
    assert float(row["vwap_t"]) == 101.5
    assert float(row["custom_quality_score"]) == 0.77


def test_trades_csv_writer_uses_contract_identity_and_tier_defaults(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path, run_id="run_1", hypothesis_id="L1-H1C", tier="Tier2")
    writer.write_trade(
        Trade(
            symbol="BTCUSDT",
            side=Side.BUY,
            entry_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
            exit_ts=pd.Timestamp("2024-01-01T01:00:00Z"),
            entry_price=100.0,
            exit_price=101.0,
            qty=1.0,
            pnl=1.0,
            fees=0.1,
            slippage=0.0,
            mae_price=99.0,
            mfe_price=102.0,
            metadata={"strategy": "volfloor_ema_pullback"},
        )
    )
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))

    assert row["hypothesis_id"] == "L1-H1C"
    assert row["identity_hypothesis_id"] == "L1-H1C"
    assert row["identity_strategy_id"] == "volfloor_ema_pullback"
    assert row["identity_tier"] == "Tier2"


def test_trades_writer_emits_unique_identity_and_single_risk_truth_column(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(
        path,
        run_id="run_1",
        hypothesis_id="L1-H1C",
        parameter_set_id="params-abc",
        tier="Tier2",
    )
    for minute in range(2):
        writer.write_trade(
            Trade(
                symbol="BTCUSDT",
                side=Side.BUY,
                entry_ts=pd.Timestamp("2024-01-01T00:00:00Z") + pd.Timedelta(minutes=minute),
                exit_ts=pd.Timestamp("2024-01-01T01:00:00Z") + pd.Timedelta(minutes=minute),
                entry_price=100.0,
                exit_price=101.0,
                qty=1.0,
                pnl=1.0,
                fees=0.1,
                slippage=0.05,
                mae_price=99.0,
                mfe_price=102.0,
                metadata={
                    "risk_amount": 2.0,
                    "entry_stop_distance": 2.0,
                    "intrabar_mode": "worst_case",
                },
            )
        )
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        assert reader.fieldnames is not None
        assert reader.fieldnames.count("risk_amount") == 1
    assert [row["identity_trade_id"] for row in rows] == ["run_1_t00001", "run_1_t00002"]
    assert {row["identity_parameter_set_id"] for row in rows} == {"params-abc"}
    assert {row["execution_intrabar_assumption"] for row in rows} == {"worst_case"}
    assert {float(row["counterfactual_fee_drag_r"]) for row in rows} == {0.05}
    assert {float(row["counterfactual_slippage_drag_r"]) for row in rows} == {0.025}


def test_trades_csv_writer_derives_exit_reason_for_close_only_signal(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path)
    writer.write_trade(
        Trade(
            symbol="BTCUSDT",
            side=Side.BUY,
            entry_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
            exit_ts=pd.Timestamp("2024-01-01T01:00:00Z"),
            entry_price=100.0,
            exit_price=101.0,
            qty=1.0,
            pnl=1.0,
            fees=0.1,
            slippage=0.0,
            mae_price=99.0,
            mfe_price=102.0,
            metadata={
                "close_only": True,
                "stop_resolution_skipped": True,
                "stop_resolution_skip_reason": "exit_signal",
                "exit_type": "ema_trend_end",
            },
        )
    )
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))

    assert row["exit_reason"] == "ema_trend_end"
