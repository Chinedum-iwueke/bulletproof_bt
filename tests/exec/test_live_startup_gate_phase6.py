from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.reconcile import ReconciliationPolicy, ReconciliationScope
from bt.exec.runtime.app import _run_live_startup_gate


@dataclass
class _FakePortfolio:
    class _Book:
        def all_positions(self):
            return {}

    position_book = _Book()


@dataclass
class _FakePortfolioRunner:
    portfolio: _FakePortfolio = _FakePortfolio()


@dataclass
class _FakeRouter:
    def current_open_orders(self):
        return []

    def local_fills(self):
        return []


@dataclass
class _FakeAdapter:
    private_ready: bool

    def private_stream_ready(self) -> bool:
        return self.private_ready

    def fetch_open_orders(self):
        return []

    def fetch_completed_orders(self):
        return []

    def fetch_recent_fills_or_executions(self):
        return []

    def fetch_positions(self):
        return []

    def fetch_balances(self):
        return BalanceSnapshot(ts=pd.Timestamp.now(tz="UTC"), balances={"USDT": 1000.0})


def test_startup_gate_blocks_on_private_stream() -> None:
    ok, reason = _run_live_startup_gate(
        adapter=_FakeAdapter(private_ready=False),
        execution_router=_FakeRouter(),
        portfolio_runner=_FakePortfolioRunner(),
        rid="run-1",
        reconcile_scope=ReconciliationScope(True, True, True, True),
        fill_tol=0.0,
        pos_tol=0.0,
        bal_tol=0.0,
        policy=ReconciliationPolicy("warn"),
        require_private_stream_ready=True,
    )
    assert not ok
    assert reason == "private_stream_not_ready"


def test_startup_gate_allows_when_conditions_pass() -> None:
    ok, reason = _run_live_startup_gate(
        adapter=_FakeAdapter(private_ready=True),
        execution_router=_FakeRouter(),
        portfolio_runner=_FakePortfolioRunner(),
        rid="run-1",
        reconcile_scope=ReconciliationScope(True, True, True, True),
        fill_tol=0.0,
        pos_tol=0.0,
        bal_tol=0.0,
        policy=ReconciliationPolicy("warn"),
        require_private_stream_ready=True,
    )
    assert ok
    assert reason is None
