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
