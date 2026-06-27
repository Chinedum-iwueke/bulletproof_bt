from __future__ import annotations

from bt.contracts.trade_memory import build_memory_assessment, validate_state_snapshot, validate_trade_episode


def snapshot(identifier: str, value: float, at: str = "2026-01-01T00:00:00Z") -> dict:
    return {"schema_version": "decision_state_snapshot_v1", "state_snapshot_id": identifier, "program_id": "p", "stage": "backtest", "decision_at": at, "captured_at": at, "features": {"volatility": value, "trend": value / 2}, "feature_timestamps": {"volatility": at, "trend": at}, "missing_features": [], "future_enriched": False}


def episode(index: int, pnl: float, strategy: str = "strategy-a") -> dict:
    return {"schema_version": "trade_episode_v1", "episode_id": f"e{index}", "program_id": "p", "stage": "backtest", "strategy_spec_hash": strategy, "product_type": "perpetual", "symbol": "BTCUSDT", "side": "buy", "opened_at": "2026-01-01T00:00:00Z", "closed_at": "2026-01-02T00:00:00Z", "entry_price": 100, "exit_price": 101, "quantity": 1, "gross_pnl": pnl, "fees": 0, "net_pnl": pnl, "status": "closed", "decision_state_snapshot_id": f"s{index}", "source_event_ids": [f"event-{index}"]}


def test_causal_state_snapshots_reject_future_features() -> None:
    current = snapshot("current", 1)
    current["feature_timestamps"]["trend"] = "2026-01-01T00:01:00Z"
    assert "state_snapshot_future_feature:trend" in validate_state_snapshot(current)
    assert validate_trade_episode(episode(1, 1)) == []


def test_memory_assessment_separates_strategy_support_and_is_advisory() -> None:
    episodes = [episode(index, 2 if index < 8 else -1, "strategy-a" if index < 6 else "strategy-b") for index in range(10)]
    snapshots = {f"s{index}": snapshot(f"s{index}", 1 + index * 0.01) for index in range(10)}
    result = build_memory_assessment(assessment_id="a", account_id="tenant-a", program_id="p", strategy_spec_hash="strategy-a", current_snapshot=snapshot("current", 1.02, "2026-01-03T00:00:00Z"), episodes=episodes, snapshots_by_id=snapshots, now="2026-01-03T00:00:00Z", min_support=8)
    assert result["support_count"] == 10
    assert result["strategy_support_count"] == 6
    assert result["cross_strategy_support_count"] == 4
    assert result["advisory_only"] is True
    assert result["may_increase_risk"] is False
    assert len(result["uncertainty_interval"]) == 2


def test_memory_assessment_refuses_sparse_or_missing_state() -> None:
    result = build_memory_assessment(assessment_id="a", account_id="tenant-a", program_id="p", strategy_spec_hash="strategy-a", current_snapshot={**snapshot("current", 1), "features": {}, "feature_timestamps": {}}, episodes=[], snapshots_by_id={}, now="2026-01-03T00:00:00Z")
    assert result["assessment"] == "insufficient_evidence"
    assert "current_state_features_missing" in result["reason_codes"]
