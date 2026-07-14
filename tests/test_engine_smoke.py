"""Smoke test for backtest engine end-to-end wiring."""
from __future__ import annotations

from pathlib import Path
import json

import pandas as pd

from bt.core.engine import BacktestEngine
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.coinflip import CoinFlipStrategy
from bt.universe.universe import UniverseEngine


def _make_bars_df() -> pd.DataFrame:
    ts_index = pd.date_range("2024-01-01", periods=5, freq="D", tz="UTC")
    rows = []
    for symbol in ["AAA", "BBB"]:
        for idx, ts in enumerate(ts_index):
            base = 100 + idx
            rows.append(
                {
                    "ts": ts,
                    "symbol": symbol,
                    "open": base,
                    "high": base + 1,
                    "low": base - 1,
                    "close": base + 0.5,
                    "volume": 1000.0 + idx,
                }
            )
    return pd.DataFrame(rows)


def test_engine_smoke(tmp_path: Path) -> None:
    bars_df = _make_bars_df()
    datafeed = HistoricalDataFeed(bars_df)

    universe = UniverseEngine(
        min_history_bars=1,
        lookback_bars=1,
        min_avg_volume=0.0,
        lag_bars=0,
    )

    strategy = CoinFlipStrategy(seed=7, p_trade=1.0, cooldown_bars=0)
    risk = RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}})

    fee_model = FeeModel(maker_fee_bps=1.0, taker_fee_bps=2.0)
    slippage_model = SlippageModel(k=0.01)
    execution = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=1,
    )

    portfolio = Portfolio(initial_cash=10000.0, max_leverage=2.0)

    decisions_path = tmp_path / "decisions.jsonl"
    fills_path = tmp_path / "fills.jsonl"
    trades_path = tmp_path / "trades.csv"
    equity_path = tmp_path / "equity.csv"

    decisions_writer = JsonlWriter(decisions_path)
    fills_writer = JsonlWriter(fills_path)
    trades_writer = TradesCsvWriter(trades_path)

    engine = BacktestEngine(
        datafeed=datafeed,
        universe=universe,
        strategy=strategy,
        risk=risk,
        execution=execution,
        portfolio=portfolio,
        decisions_writer=decisions_writer,
        fills_writer=fills_writer,
        trades_writer=trades_writer,
        equity_path=equity_path,
        config={},
    )
    engine.run()

    assert decisions_path.exists()
    assert fills_path.exists()
    assert trades_path.exists()
    assert equity_path.exists()

    decisions_lines = decisions_path.read_text(encoding="utf-8").strip().splitlines()
    fills_lines = fills_path.read_text(encoding="utf-8").strip().splitlines()
    equity_lines = equity_path.read_text(encoding="utf-8").strip().splitlines()

    assert len(decisions_lines) >= 1
    assert len(fills_lines) >= 1
    assert len(equity_lines) >= 2


def test_engine_warmup_suppresses_pre_start_trading_and_equity(tmp_path: Path) -> None:
    bars_df = _make_bars_df()
    datafeed = HistoricalDataFeed(bars_df)
    official_start = pd.Timestamp("2024-01-03T00:00:00Z")

    engine = BacktestEngine(
        datafeed=datafeed,
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=CoinFlipStrategy(seed=7, p_trade=1.0, cooldown_bars=0),
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}}),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=10000.0, max_leverage=2.0),
        decisions_writer=JsonlWriter(tmp_path / "decisions.jsonl"),
        fills_writer=JsonlWriter(tmp_path / "fills.jsonl"),
        trades_writer=TradesCsvWriter(tmp_path / "trades.csv"),
        equity_path=tmp_path / "equity.csv",
        config={"data": {"analysis_start_ts": official_start.isoformat()}},
    )

    engine.run()

    decisions = [
        json.loads(line)
        for line in (tmp_path / "decisions.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert decisions
    assert all(pd.Timestamp(row["ts"]) >= official_start for row in decisions)

    equity = pd.read_csv(tmp_path / "equity.csv")
    assert not equity.empty
    assert pd.to_datetime(equity["ts"], utc=True).min() >= official_start
