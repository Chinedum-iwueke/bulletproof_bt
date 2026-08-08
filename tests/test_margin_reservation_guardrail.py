from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.core.engine import BacktestEngine
from bt.core.enums import Side
from bt.core.types import Signal
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy
from bt.universe.universe import UniverseEngine


class DualSignalStrategy(Strategy):
    def __init__(self) -> None:
        self._emitted = False

    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
        if self._emitted:
            return []
        self._emitted = True
        return [
            Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={"stop_price": 99.0}),
            Signal(ts=ts, symbol="ETH", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={"stop_price": 99.0}),
        ]


class DelayedDualSignalStrategy(Strategy):
    def __init__(self) -> None:
        self._seen = 0

    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
        self._seen += 1
        if self._seen == 1:
            return [Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={"stop_price": 99.0})]
        if self._seen == 2:
            return [Signal(ts=ts, symbol="ETH", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={"stop_price": 99.0})]
        return []


def _multi_symbol_rows(symbols: list[str], periods: int) -> pd.DataFrame:
    ts_index = pd.date_range("2024-01-01", periods=periods, freq="min", tz="UTC")
    rows = []
    for symbol in symbols:
        for ts in ts_index:
            rows.append(
                {
                    "ts": ts,
                    "symbol": symbol,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1_000.0,
                }
            )
    return pd.DataFrame(rows)


def test_reservation_blocks_second_signal_same_timestamp(tmp_path: Path) -> None:
    datafeed = HistoricalDataFeed(_multi_symbol_rows(["BTC", "ETH"], periods=1))
    engine = BacktestEngine(
        datafeed=datafeed,
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=DualSignalStrategy(),
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}}),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=10_000.0, max_leverage=10.0),
        decisions_writer=JsonlWriter(tmp_path / "decisions.jsonl"),
        fills_writer=JsonlWriter(tmp_path / "fills.jsonl"),
        trades_writer=TradesCsvWriter(tmp_path / "trades.csv"),
        equity_path=tmp_path / "equity.csv",
        config={},
    )

    engine.run()

    decisions = [json.loads(line) for line in (tmp_path / "decisions.jsonl").read_text(encoding="utf-8").splitlines()]
    approved = [d for d in decisions if d.get("approved")]
    rejected = [d for d in decisions if not d.get("approved")]
    fills = [json.loads(line) for line in (tmp_path / "fills.jsonl").read_text(encoding="utf-8").splitlines()]

    assert len(approved) == 1
    assert len(rejected) == 1
    assert rejected[0]["reason"] == "risk_rejected:max_positions_reached"
    assert len(fills) == 2
    assert sum(1 for fill in fills if fill.get("metadata", {}).get("forced_liquidation") is True) == 0
    assert any(fill.get("metadata", {}).get("reason") == "end_of_run_flatten" for fill in fills)


def test_delayed_entry_reservation_blocks_next_bar_overbooking(tmp_path: Path) -> None:
    datafeed = HistoricalDataFeed(_multi_symbol_rows(["BTC", "ETH"], periods=3))
    engine = BacktestEngine(
        datafeed=datafeed,
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=DelayedDualSignalStrategy(),
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}}),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=1,
        ),
        portfolio=Portfolio(initial_cash=10_000.0, max_leverage=10.0),
        decisions_writer=JsonlWriter(tmp_path / "decisions.jsonl"),
        fills_writer=JsonlWriter(tmp_path / "fills.jsonl"),
        trades_writer=TradesCsvWriter(tmp_path / "trades.csv"),
        equity_path=tmp_path / "equity.csv",
        config={},
    )

    engine.run()

    decisions = [json.loads(line) for line in (tmp_path / "decisions.jsonl").read_text(encoding="utf-8").splitlines()]
    approved = [d for d in decisions if d.get("approved")]
    rejected = [d for d in decisions if not d.get("approved")]
    fills = [json.loads(line) for line in (tmp_path / "fills.jsonl").read_text(encoding="utf-8").splitlines()]

    assert [d["symbol"] for d in approved] == ["BTC"]
    assert len(rejected) == 1
    assert rejected[0]["symbol"] == "ETH"
    assert rejected[0]["reason"] == "risk_rejected:max_positions_reached"
    assert {fill["symbol"] for fill in fills} == {"BTC"}
    assert sum(1 for fill in fills if fill.get("metadata", {}).get("forced_liquidation") is True) == 0
