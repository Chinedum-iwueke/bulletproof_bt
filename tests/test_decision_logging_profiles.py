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


class _StateLogAndTradeStrategy(Strategy):
    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
        bar = bars_by_symbol["BTCUSDT"]
        signals = [
            Signal(
                ts=ts,
                symbol="BTCUSDT",
                side=None,
                signal_type="diagnostic_state",
                confidence=0.0,
                metadata={"state_log_only": True, "entry_reason": "diagnostic_state"},
            )
        ]
        if ts == pd.Timestamp("2025-01-01T00:02:00Z"):
            signals.append(
                Signal(
                    ts=ts,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    signal_type="entry",
                    confidence=1.0,
                    metadata={"stop_price": bar.close - 1.0, "stop_distance": 1.0},
                )
            )
        if ts == pd.Timestamp("2025-01-01T00:04:00Z"):
            signals.append(
                Signal(
                    ts=ts,
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    signal_type="exit",
                    confidence=1.0,
                    metadata={"close_only": True},
                )
            )
        return signals


def _bars() -> pd.DataFrame:
    rows = []
    for i in range(8):
        ts = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
        px = 100.0 + i
        rows.append(
            {
                "ts": ts,
                "symbol": "BTCUSDT",
                "open": px,
                "high": px + 1.0,
                "low": px - 1.0,
                "close": px,
                "volume": 1000.0,
            }
        )
    return pd.DataFrame(rows)


def _run(tmp_path: Path, profile: str) -> Path:
    run_dir = tmp_path / profile
    engine = BacktestEngine(
        datafeed=HistoricalDataFeed(_bars()),
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=_StateLogAndTradeStrategy(),
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}}),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=10_000.0, max_leverage=2.0),
        decisions_writer=JsonlWriter(run_dir / "decisions.jsonl"),
        fills_writer=JsonlWriter(run_dir / "fills.jsonl"),
        trades_writer=TradesCsvWriter(run_dir / "trades.csv"),
        equity_path=run_dir / "equity.csv",
        config={"outputs": {"decision_logging_profile": profile}},
    )
    engine.run()
    return run_dir


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_research_sparse_skips_state_log_only_decisions_without_changing_truth_artifacts(tmp_path: Path) -> None:
    full = _run(tmp_path, "full")
    sparse = _run(tmp_path, "research_sparse")

    full_decisions = _jsonl(full / "decisions.jsonl")
    sparse_decisions = _jsonl(sparse / "decisions.jsonl")

    assert len(sparse_decisions) < len(full_decisions)
    assert all(not row.get("signal", {}).get("metadata", {}).get("state_log_only") for row in sparse_decisions)
    assert any(row.get("approved") for row in sparse_decisions)

    assert (full / "trades.csv").read_text(encoding="utf-8") == (sparse / "trades.csv").read_text(encoding="utf-8")
    assert (full / "equity.csv").read_text(encoding="utf-8") == (sparse / "equity.csv").read_text(encoding="utf-8")
    assert (full / "fills.jsonl").read_text(encoding="utf-8") == (sparse / "fills.jsonl").read_text(encoding="utf-8")

    summary = json.loads((sparse / "decision_logging_summary.json").read_text(encoding="utf-8"))
    assert summary["profile"] == "research_sparse"
    assert summary["skipped"] > 0
    assert summary["written"] == len(sparse_decisions)
