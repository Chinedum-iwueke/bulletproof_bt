from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Signal
from bt.exec.events.runtime_events import RuntimeHeartbeatEvent
from bt.exec.logging.heartbeats import heartbeat_record
from bt.exec.logging.schemas import DecisionArtifactRecord, FillArtifactRecord, HeartbeatArtifactRecord


def test_artifact_schema_dataclasses_jsonable() -> None:
    ts = pd.Timestamp("2026-01-01T00:00:00Z")
    signal = Signal(ts=ts, symbol="BTCUSDT", side=Side.BUY, signal_type="entry", confidence=1.0)
    decision = DecisionArtifactRecord(ts=ts, symbol="BTCUSDT", signal=signal, approved=True, reason="ok")
    assert decision.symbol == "BTCUSDT"

    hb = heartbeat_record(RuntimeHeartbeatEvent(ts=ts, sequence=1), healthy=True, stale_seconds=0.0)
    typed_hb = HeartbeatArtifactRecord(**hb)
    assert typed_hb.healthy is True

    fill = FillArtifactRecord(ts=ts, symbol="BTCUSDT", order_id="sim-1", side="buy", qty=1.0, price=100.0, fee=0.0, slippage=0.0)
    assert fill.order_id == "sim-1"
