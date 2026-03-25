from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest

from bt.exec.adapters.base import AdapterHealth, AdapterHealthStatus, BalanceSnapshot
from bt.exec.adapters.bybit.errors import BybitAdapterError
from bt.exec.cli import doctor


@dataclass
class _FakeAdapter:
    fail: bool = False

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None

    def fetch_balances(self) -> BalanceSnapshot:
        if self.fail:
            raise BybitAdapterError("auth bad")
        return BalanceSnapshot(ts=pd.Timestamp.now(tz="UTC"), balances={"USDT": 1.0})

    def fetch_positions(self):
        return []

    def fetch_open_orders(self):
        return []

    def fetch_recent_fills_or_executions(self, limit: int = 50):
        _ = limit
        return []

    def get_instrument(self, symbol: str):
        return {"symbol": symbol}

    def get_health(self) -> AdapterHealth:
        return AdapterHealth(source="bybit", ts=pd.Timestamp.now(tz="UTC"), status=AdapterHealthStatus.HEALTHY)


def _cfg() -> dict[str, object]:
    return {
        "broker": {
            "venue": "bybit",
            "environment": "demo",
            "category": "linear",
            "symbols": ["BTCUSDT"],
            "auth": {"api_key_env": "BYBIT_API_KEY", "api_secret_env": "BYBIT_API_SECRET"},
        }
    }


def test_doctor_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BYBIT_API_KEY", "k")
    monkeypatch.setenv("BYBIT_API_SECRET", "s")
    monkeypatch.setenv("BYBIT_DEMO_API_KEY", "k")
    monkeypatch.setenv("BYBIT_DEMO_API_SECRET", "s")
    monkeypatch.setattr(doctor, "BybitBrokerAdapter", lambda **_kwargs: _FakeAdapter())
    summary = doctor.run_doctor_diagnosis(config=_cfg(), check_ws=False)
    assert summary.ok
    assert summary.checks["rest_auth"]
    assert summary.readiness == "healthy_demo"


def test_doctor_failure_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BYBIT_API_KEY", "k")
    monkeypatch.setenv("BYBIT_API_SECRET", "s")
    monkeypatch.setenv("BYBIT_DEMO_API_KEY", "k")
    monkeypatch.setenv("BYBIT_DEMO_API_SECRET", "s")
    monkeypatch.setattr(doctor, "BybitBrokerAdapter", lambda **_kwargs: _FakeAdapter(fail=True))
    with pytest.raises(BybitAdapterError):
        doctor.run_doctor_diagnosis(config=_cfg(), check_ws=False)


def test_doctor_live_readiness_marks_wrong_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BYBIT_API_KEY", "k")
    monkeypatch.setenv("BYBIT_API_SECRET", "s")
    monkeypatch.setenv("BYBIT_DEMO_API_KEY", "k")
    monkeypatch.setenv("BYBIT_DEMO_API_SECRET", "s")
    monkeypatch.setattr(doctor, "BybitBrokerAdapter", lambda **_kwargs: _FakeAdapter())
    summary = doctor.run_doctor_diagnosis(config=_cfg(), check_ws=False, live_readiness=True)
    assert not summary.ok
    assert summary.checks["live_environment"] is False
    assert summary.readiness == "healthy_demo"
