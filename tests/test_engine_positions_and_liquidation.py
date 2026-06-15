from __future__ import annotations

import pytest

import json
from pathlib import Path

import pandas as pd

from bt.core.engine import BacktestEngine
from bt.core.enums import Side
from bt.core.types import Bar, Signal
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


def _bars_df(periods: int = 4) -> pd.DataFrame:
    ts_index = pd.date_range("2024-01-01", periods=periods, freq="D", tz="UTC")
    return pd.DataFrame(
        {
            "ts": ts_index,
            "symbol": ["AAA"] * periods,
            "open": [100.0 + idx for idx in range(periods)],
            "high": [101.0 + idx for idx in range(periods)],
            "low": [99.0 + idx for idx in range(periods)],
            "close": [100.5 + idx for idx in range(periods)],
            "volume": [1000.0 + idx for idx in range(periods)],
        }
    )


class CaptureCtxStrategy(Strategy):
    def __init__(self) -> None:
        self.captured_positions: dict | None = None
        self._entered = False

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list[Signal]:
        if self._entered:
            self.captured_positions = ctx.get("positions")
            return []
        self._entered = True
        return [Signal(ts=ts, symbol="AAA", side=Side.BUY, signal_type="entry", confidence=1.0, metadata={"stop_price": 95.0})]


class EntryOnlyStrategy(Strategy):
    def __init__(self) -> None:
        self._entered = False

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list[Signal]:
        if self._entered:
            return []
        self._entered = True
        return [Signal(ts=ts, symbol="AAA", side=Side.BUY, signal_type="entry", confidence=1.0, metadata={"stop_price": 95.0})]


def _make_engine(tmp_path: Path, strategy: Strategy, periods: int = 4) -> tuple[BacktestEngine, Path, Path, Path]:
    datafeed = HistoricalDataFeed(_bars_df(periods=periods))
    universe = UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0)
    risk = RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}})
    execution = ExecutionModel(
        fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
        slippage_model=SlippageModel(k=0.0),
        delay_bars=0,
    )
    portfolio = Portfolio(initial_cash=10000.0, max_leverage=2.0)

    decisions_path = tmp_path / "decisions.jsonl"
    fills_path = tmp_path / "fills.jsonl"
    trades_path = tmp_path / "trades.csv"
    equity_path = tmp_path / "equity.csv"

    engine = BacktestEngine(
        datafeed=datafeed,
        universe=universe,
        strategy=strategy,
        risk=risk,
        execution=execution,
        portfolio=portfolio,
        decisions_writer=JsonlWriter(decisions_path),
        fills_writer=JsonlWriter(fills_path),
        trades_writer=TradesCsvWriter(trades_path),
        equity_path=equity_path,
        config={},
    )
    return engine, fills_path, trades_path, equity_path


def test_engine_injects_positions_into_strategy_context(tmp_path: Path) -> None:
    strategy = CaptureCtxStrategy()
    engine, _, _, _ = _make_engine(tmp_path, strategy)

    engine.run()

    assert strategy.captured_positions is not None
    assert "AAA" in strategy.captured_positions
    assert strategy.captured_positions["AAA"]["side"] == "buy"
    assert strategy.captured_positions["AAA"]["qty"] > 0


def test_engine_end_of_run_flatten_is_not_classified_as_forced_liquidation(tmp_path: Path) -> None:
    engine, fills_path, trades_path, _ = _make_engine(tmp_path, EntryOnlyStrategy())

    engine.run()

    final_position = engine._portfolio.position_book.get("AAA")
    assert final_position.qty == 0.0

    fill_rows = [json.loads(line) for line in fills_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(
        isinstance(row.get("metadata"), dict)
        and row["metadata"].get("forced_liquidation") is False
        and row["metadata"].get("close_only") is True
        and row["metadata"].get("reason") == "end_of_run_flatten"
        and row["metadata"].get("exit_reason") == "end_of_run_flatten"
        and row["metadata"].get("liquidation_reason") == "liquidation:end_of_run"
        for row in fill_rows
    )

    trades_lines = trades_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(trades_lines) >= 2


class StressLongStrategy(Strategy):
    def __init__(self) -> None:
        self._entered = False

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list[Signal]:
        if self._entered:
            return []
        self._entered = True
        return [Signal(ts=ts, symbol="AAA", side=Side.BUY, signal_type="entry", confidence=1.0, metadata={"stop_price": 99.0})]


def test_engine_forced_liquidation_on_negative_free_margin(tmp_path: Path) -> None:
    from bt.logging.sanity import SanityCounters, write_sanity_json

    ts_index = pd.date_range("2024-01-01", periods=2, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "ts": ts_index,
            "symbol": ["AAA", "AAA"],
            "open": [100.0, 100.0],
            "high": [100.0, 100.0],
            "low": [100.0, 90.0],
            "close": [100.0, 90.0],
            "volume": [1000.0, 1000.0],
        }
    )

    datafeed = HistoricalDataFeed(df)
    universe = UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0)
    risk = RiskEngine(
        max_positions=1,
        config={
            "risk": {
                "mode": "r_fixed",
                "r_per_trade": 2.0,
                "stop": {},
                "max_notional_pct_equity": 5.0,
                "maintenance_free_margin_pct": 0.0,
                "may_liquidate": True,
            }
        },
    )
    execution = ExecutionModel(
        fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
        slippage_model=SlippageModel(k=0.0),
        delay_bars=0,
    )
    portfolio = Portfolio(initial_cash=100000.0, max_leverage=2.0)
    counters = SanityCounters(run_id="forced-liquidation")

    fills_path = tmp_path / "fills.jsonl"
    engine = BacktestEngine(
        datafeed=datafeed,
        universe=universe,
        strategy=StressLongStrategy(),
        risk=risk,
        execution=execution,
        portfolio=portfolio,
        decisions_writer=JsonlWriter(tmp_path / "decisions.jsonl"),
        fills_writer=JsonlWriter(fills_path),
        trades_writer=TradesCsvWriter(tmp_path / "trades.csv"),
        equity_path=tmp_path / "equity.csv",
        config={},
        sanity_counters=counters,
    )

    engine.run()

    final_position = engine._portfolio.position_book.get("AAA")
    assert final_position.qty == 0.0

    fill_rows = [json.loads(line) for line in fills_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(
        isinstance(row.get("metadata"), dict)
        and row["metadata"].get("forced_liquidation") is True
        and row["metadata"].get("exit_reason") == "forced_liquidation"
        and row["metadata"].get("liquidation_reason") == "liquidation:negative_free_margin"
        for row in fill_rows
    )

    write_sanity_json(tmp_path, counters)
    sanity = json.loads((tmp_path / "sanity.json").read_text(encoding="utf-8"))
    assert int(sanity.get("forced_liquidations", 0)) >= 1


def test_engine_defaults_to_forced_liquidation_on_negative_free_margin(tmp_path: Path) -> None:
    ts_index = pd.date_range("2024-01-01", periods=2, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "ts": ts_index,
            "symbol": ["AAA", "AAA"],
            "open": [100.0, 100.0],
            "high": [100.0, 100.0],
            "low": [100.0, 90.0],
            "close": [100.0, 90.0],
            "volume": [1000.0, 1000.0],
        }
    )

    engine = BacktestEngine(
        datafeed=HistoricalDataFeed(df),
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=StressLongStrategy(),
        risk=RiskEngine(
            max_positions=1,
            config={
                "risk": {
                    "mode": "r_fixed",
                    "r_per_trade": 2.0,
                    "stop": {},
                    "max_notional_pct_equity": 5.0,
                    "maintenance_free_margin_pct": 0.01,
                }
            },
        ),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=100000.0, max_leverage=2.0),
        decisions_writer=JsonlWriter(tmp_path / "decisions.jsonl"),
        fills_writer=JsonlWriter(tmp_path / "fills.jsonl"),
        trades_writer=TradesCsvWriter(tmp_path / "trades.csv"),
        equity_path=tmp_path / "equity.csv",
        config={},
    )

    engine.run()

    fill_rows = [json.loads(line) for line in (tmp_path / "fills.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(
        isinstance(row.get("metadata"), dict)
        and row["metadata"].get("forced_liquidation") is True
        and row["metadata"].get("liquidation_reason") == "liquidation:negative_free_margin"
        for row in fill_rows
    )
    entry = next(row for row in fill_rows if not row["metadata"].get("close_only"))
    assert float(entry["metadata"]["free_margin_post"]) >= 0.0
    assert float(entry["metadata"]["mark_price_used_for_margin"]) == pytest.approx(100.0)
