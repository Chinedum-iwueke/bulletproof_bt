"""RISK-001..002 native path, tail, reverse-stress and venue-rule producers."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import Any

import numpy as np

from .receipt import ProducerReceipt, build_receipt, digest


class RiskProducerError(ValueError):
    """Risk evidence or venue rules are incomplete, stale or internally inconsistent."""


def stress_dossier_receipt(
    *,
    returns: np.ndarray,
    scenarios: dict[str, Iterable[float]],
    scenario_limit: float,
    tail_probability: float,
    dataset_digest: str,
    run_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    values = np.asarray(returns, dtype=np.float64)
    required = {
        "price_gap",
        "correlation_break",
        "liquidity_freeze",
        "model_failure",
        "prolonged_drawdown",
    }
    if values.ndim != 1 or len(values) < 30 or not np.isfinite(values).all():
        raise RiskProducerError("at least 30 finite path returns are required")
    if (
        set(scenarios) != required
        or not 0 < tail_probability < 0.5
        or not 0 < scenario_limit < 1
    ):
        raise RiskProducerError("scenario pack or risk limits are invalid")
    wealth = np.cumprod(1 + values)
    peak = np.maximum.accumulate(wealth)
    underwater = wealth / peak - 1
    maximum_drawdown = float(-np.min(underwater))
    duration, maximum_duration = 0, 0
    for item in underwater:
        duration = duration + 1 if item < 0 else 0
        maximum_duration = max(maximum_duration, duration)
    cutoff = float(np.quantile(values, tail_probability))
    tail = values[values <= cutoff]
    expected_shortfall = float(-np.mean(tail))
    scenario_results: dict[str, Any] = {}
    for name, path in sorted(scenarios.items()):
        shocks = np.asarray(list(path), dtype=np.float64)
        if shocks.ndim != 1 or not len(shocks) or not np.isfinite(shocks).all():
            raise RiskProducerError("scenario path is malformed")
        losses = -np.minimum.accumulate(np.cumsum(shocks))
        maximum_loss = float(np.max(losses))
        breaches = np.flatnonzero(losses > scenario_limit)
        scenario_results[name] = {
            "maximum_loss": maximum_loss,
            "first_breaching_step": int(breaches[0]) if len(breaches) else None,
            "breached": bool(len(breaches)),
        }
    failures = []
    if maximum_drawdown > scenario_limit:
        failures.append("drawdown_limit_breached")
    if expected_shortfall > scenario_limit:
        failures.append("tail_limit_breached")
    if any(item["breached"] for item in scenario_results.values()):
        failures.append("scenario_limit_breached")
    result = {
        "schema_version": "risk001-stress-dossier-v1.0.0",
        "run_digest": run_digest,
        "maximum_drawdown": maximum_drawdown,
        "maximum_drawdown_duration": maximum_duration,
        "var": -cutoff,
        "expected_shortfall": expected_shortfall,
        "tail_probability": tail_probability,
        "scenarios": scenario_results,
        "failures": failures,
        "admissible": not failures,
    }
    return build_receipt(
        milestone="RISK-001",
        producer="bt.institutional.risk.stress_dossier_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "returns": values.tolist(),
            "scenarios": {key: list(value) for key, value in scenarios.items()},
        },
        dataset_digest=dataset_digest,
        configuration={
            "scenario_limit": scenario_limit,
            "tail_probability": tail_probability,
        },
        artifacts=result,
        result=result,
    )


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def venue_rule_receipt(
    *,
    stress_receipt: ProducerReceipt,
    rule_pack: dict[str, Any],
    position: dict[str, Any],
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    if (
        stress_receipt.milestone != "RISK-001"
        or not stress_receipt.result["admissible"]
    ):
        raise RiskProducerError("RISK-002 requires an admissible RISK-001 receipt")
    notional = _decimal(position["quantity"]) * _decimal(position["mark_price"])
    tiers = sorted(
        rule_pack["margin_tiers"], key=lambda item: _decimal(item["notional_floor"])
    )
    tier = next(
        (
            item
            for item in tiers
            if _decimal(item["notional_floor"])
            <= notional
            < _decimal(item["notional_cap"])
        ),
        None,
    )
    failures = []
    if tier is None:
        failures.append("notional_outside_margin_tiers")
        tier = tiers[-1]
    leverage = _decimal(position["requested_leverage"])
    if leverage > _decimal(tier["maximum_leverage"]):
        failures.append("leverage_limit_breached")
    quantity_increment, price_increment = (
        _decimal(rule_pack["quantity_increment"]),
        _decimal(rule_pack["price_increment"]),
    )
    if _decimal(position["quantity"]) % quantity_increment:
        failures.append("quantity_increment_violation")
    if _decimal(position["entry_price"]) % price_increment:
        failures.append("price_increment_violation")
    mark, index, entry = (
        _decimal(position["mark_price"]),
        _decimal(position["index_price"]),
        _decimal(position["entry_price"]),
    )
    mark_deviation = abs(mark - index) / index
    if mark_deviation > _decimal(rule_pack["maximum_mark_deviation"]):
        failures.append("mark_price_deviation_breached")
    funding_rate = abs(_decimal(position["accrued_funding"])) / notional
    if funding_rate > _decimal(rule_pack["maximum_abs_funding_rate"]):
        failures.append("funding_limit_breached")
    direction = Decimal(1) if position["side"] == "long" else Decimal(-1)
    pnl = direction * _decimal(position["quantity"]) * (mark - entry)
    equity = (
        _decimal(position["collateral"])
        + pnl
        - _decimal(position["accrued_funding"])
        - _decimal(position["fee_reserve"])
    )
    maintenance = max(
        Decimal(0),
        notional * _decimal(tier["maintenance_margin_rate"])
        - _decimal(tier["maintenance_amount"]),
    )
    buffer = equity - maintenance
    if buffer < _decimal(rule_pack["minimum_liquidation_buffer"]):
        failures.append("liquidation_buffer_breached")
    result = {
        "schema_version": "risk002-venue-rule-receipt-v1.0.0",
        "stress_receipt_digest": stress_receipt.receipt_digest,
        "rule_pack_digest": digest(rule_pack),
        "position_state_digest": digest(position),
        "selected_margin_tier": tier["tier"],
        "notional": str(notional),
        "initial_margin_required": str(notional / leverage),
        "maintenance_margin": str(maintenance),
        "equity_after_funding_and_fees": str(equity),
        "liquidation_buffer": str(buffer),
        "mark_deviation": str(mark_deviation),
        "effective_funding_rate": str(funding_rate),
        "failures": failures,
        "allowed": not failures,
    }
    return build_receipt(
        milestone="RISK-002",
        producer="bt.institutional.risk.venue_rule_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"rule_pack": rule_pack, "position": position},
        dataset_digest=dataset_digest,
        configuration={"effective_rule_version": rule_pack["version"]},
        artifacts=result,
        result=result,
    )
