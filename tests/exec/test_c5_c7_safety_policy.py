from bt.exec.services.live_canary import CanaryOrderIntent, CanaryRiskPolicy, CanaryState, evaluate_canary_order
from bt.exec.services.memory_policy import MemoryPolicy, MemoryPolicyInput, evaluate_memory_policy


def policy() -> CanaryRiskPolicy:
    return CanaryRiskPolicy(frozenset({"BTCUSDT"}), 0.01, 1000, 1500, 2, 1, 100, 150)


def state(**changes: object) -> CanaryState:
    values = dict(environment="live", connector_healthy=True, connector_checked_age_seconds=1, reconciliation_healthy=True, unresolved_critical_incidents=0, demo_qualified=True, kill_switch_tested=True, open_orders=0, open_positions=0, gross_notional_usd=0, daily_pnl_usd=0, session_pnl_usd=0, frozen=False)
    values.update(changes)
    return CanaryState(**values)  # type: ignore[arg-type]


def test_live_canary_fails_closed_on_every_hard_boundary() -> None:
    intent = CanaryOrderIntent("BTCUSDT", 0.01, 50_000)
    assert evaluate_canary_order(policy=policy(), state=state(), intent=intent).allowed
    assert "demo_qualification_required" in evaluate_canary_order(policy=policy(), state=state(demo_qualified=False), intent=intent).reason_codes
    assert "daily_loss_limit" in evaluate_canary_order(policy=policy(), state=state(daily_pnl_usd=-100), intent=intent).reason_codes
    assert "symbol_not_allowed" in evaluate_canary_order(policy=policy(), state=state(), intent=CanaryOrderIntent("ETHUSDT", .01, 100)).reason_codes


def test_frozen_runtime_only_allows_reduce_only_exit() -> None:
    blocked = evaluate_canary_order(policy=policy(), state=state(frozen=True), intent=CanaryOrderIntent("BTCUSDT", .01, 100))
    exit_decision = evaluate_canary_order(policy=policy(), state=state(frozen=True), intent=CanaryOrderIntent("BTCUSDT", .01, 100, reduce_only=True))
    assert not blocked.allowed
    assert exit_decision.allowed


def test_memory_policy_shadows_before_explicit_enforcement_and_never_increases_risk() -> None:
    value = MemoryPolicyInput("block", 30, 1.0, "calibrated", 0.01, True)
    shadow = evaluate_memory_policy(policy=MemoryPolicy(mode="shadow"), value=value)
    enforced = evaluate_memory_policy(policy=MemoryPolicy(mode="enforced", approved=True), value=value)
    assert shadow.would_block and not shadow.applied_block and shadow.effective_quantity == .01
    assert enforced.applied_block and enforced.effective_quantity == 0
    assert enforced.risk_increase_forbidden


def test_pine_or_alert_evidence_cannot_authorize() -> None:
    decision = evaluate_memory_policy(policy=MemoryPolicy(mode="enforced", approved=True), value=MemoryPolicyInput("supportive", 50, 1.0, "calibrated", .01, False))
    assert decision.applied_block
    assert "non_trade_evidence_cannot_authorize" in decision.reason_codes
