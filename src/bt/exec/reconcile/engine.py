from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

import pandas as pd

from bt.core.types import Fill, Order, Position
from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.lifecycle.fills import canonical_fill_keys
from bt.exec.reconcile.policies import ReconciliationDecision, ReconciliationPolicy, decide_policy_action


class MismatchSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    MATERIAL = "material"


@dataclass(frozen=True)
class ReconciliationMismatch:
    category: str
    key: str
    local_value: object
    adapter_value: object
    severity: MismatchSeverity
    details: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ReconciliationScope:
    compare_orders: bool
    compare_fills: bool
    compare_positions: bool
    compare_balances: bool


@dataclass(frozen=True)
class ReconciliationResult:
    ts: pd.Timestamp
    scope: ReconciliationScope
    mismatches: list[ReconciliationMismatch]
    material_mismatch_count: int
    decision: ReconciliationDecision


@dataclass(frozen=True)
class ReconciliationInputs:
    run_id: str
    ts: pd.Timestamp
    local_open_orders: list[Order]
    adapter_open_orders: list[Order]
    adapter_completed_orders: list[Order]
    local_fills: list[Fill]
    adapter_fills: list[Fill]
    local_positions: list[Position]
    adapter_positions: list[Position]
    local_balances: BalanceSnapshot | None
    adapter_balances: BalanceSnapshot | None
    scope: ReconciliationScope
    material_fill_qty_tolerance: float
    material_position_qty_tolerance: float
    material_balance_tolerance: float


class ReconciliationEngine:
    def reconcile(self, *, inputs: ReconciliationInputs, policy: ReconciliationPolicy) -> ReconciliationResult:
        mismatches: list[ReconciliationMismatch] = []
        if inputs.scope.compare_orders:
            mismatches.extend(_compare_orders(inputs.local_open_orders, inputs.adapter_open_orders, inputs.adapter_completed_orders))
        if inputs.scope.compare_fills:
            mismatches.extend(_compare_fills(inputs.local_fills, inputs.adapter_fills, tolerance=inputs.material_fill_qty_tolerance))
        if inputs.scope.compare_positions:
            mismatches.extend(_compare_positions(inputs.local_positions, inputs.adapter_positions, tolerance=inputs.material_position_qty_tolerance))
        if inputs.scope.compare_balances:
            mismatches.extend(_compare_balances(inputs.local_balances, inputs.adapter_balances, tolerance=inputs.material_balance_tolerance))

        material_count = sum(1 for m in mismatches if m.severity == MismatchSeverity.MATERIAL)
        simulated_safe_only = all(m.category in {"order_presence"} for m in mismatches) if mismatches else False
        decision = decide_policy_action(policy=policy, has_material_mismatch=material_count > 0, simulated_safe_only=simulated_safe_only)
        return ReconciliationResult(ts=inputs.ts, scope=inputs.scope, mismatches=mismatches, material_mismatch_count=material_count, decision=decision)


def _compare_orders(local_open: list[Order], adapter_open: list[Order], adapter_completed: list[Order]) -> list[ReconciliationMismatch]:
    mismatches: list[ReconciliationMismatch] = []
    local_ids = {o.id for o in local_open}
    adapter_open_ids = {o.id for o in adapter_open}
    adapter_completed_ids = {o.id for o in adapter_completed}
    for missing in sorted(local_ids - adapter_open_ids - adapter_completed_ids):
        mismatches.append(
            ReconciliationMismatch(
                category="order_presence",
                key=missing,
                local_value="open",
                adapter_value="missing",
                severity=MismatchSeverity.WARNING,
            )
        )
    for unknown in sorted(adapter_open_ids - local_ids):
        mismatches.append(
            ReconciliationMismatch(
                category="order_presence",
                key=unknown,
                local_value="missing",
                adapter_value="open",
                severity=MismatchSeverity.WARNING,
            )
        )
    return mismatches


def _compare_fills(local: list[Fill], adapter: list[Fill], *, tolerance: float) -> list[ReconciliationMismatch]:
    mismatches: list[ReconciliationMismatch] = []
    local_keys = canonical_fill_keys(local)
    adapter_keys = canonical_fill_keys(adapter)
    for key in sorted(local_keys - adapter_keys):
        mismatches.append(ReconciliationMismatch(category="fill_presence", key=key, local_value="present", adapter_value="missing", severity=MismatchSeverity.MATERIAL))
    for key in sorted(adapter_keys - local_keys):
        mismatches.append(ReconciliationMismatch(category="fill_presence", key=key, local_value="missing", adapter_value="present", severity=MismatchSeverity.MATERIAL))

    local_qty = sum(f.qty for f in local)
    adapter_qty = sum(f.qty for f in adapter)
    if abs(local_qty - adapter_qty) > tolerance:
        mismatches.append(
            ReconciliationMismatch(
                category="fill_qty",
                key="aggregate",
                local_value=local_qty,
                adapter_value=adapter_qty,
                severity=MismatchSeverity.MATERIAL,
            )
        )
    return mismatches


def _compare_positions(local: list[Position], adapter: list[Position], *, tolerance: float) -> list[ReconciliationMismatch]:
    mismatches: list[ReconciliationMismatch] = []
    local_map = {p.symbol: p for p in local}
    adapter_map = {p.symbol: p for p in adapter}
    for symbol in sorted(set(local_map) | set(adapter_map)):
        lp = local_map.get(symbol)
        ap = adapter_map.get(symbol)
        if lp is None or ap is None:
            mismatches.append(ReconciliationMismatch(category="position_presence", key=symbol, local_value=lp.qty if lp else None, adapter_value=ap.qty if ap else None, severity=MismatchSeverity.WARNING))
            continue
        if abs(lp.qty - ap.qty) > tolerance:
            mismatches.append(ReconciliationMismatch(category="position_qty", key=symbol, local_value=lp.qty, adapter_value=ap.qty, severity=MismatchSeverity.MATERIAL))
    return mismatches


def _compare_balances(local: BalanceSnapshot | None, adapter: BalanceSnapshot | None, *, tolerance: float) -> list[ReconciliationMismatch]:
    mismatches: list[ReconciliationMismatch] = []
    if local is None or adapter is None:
        return mismatches
    for asset in sorted(set(local.balances) | set(adapter.balances)):
        lv = local.balances.get(asset)
        av = adapter.balances.get(asset)
        if lv is None or av is None:
            mismatches.append(ReconciliationMismatch(category="balance_presence", key=asset, local_value=lv, adapter_value=av, severity=MismatchSeverity.WARNING))
            continue
        if abs(lv - av) > tolerance:
            mismatches.append(ReconciliationMismatch(category="balance_qty", key=asset, local_value=lv, adapter_value=av, severity=MismatchSeverity.MATERIAL))
    return mismatches
