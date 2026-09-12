"""RISK-003 dynamic risk-budget and regime-scaling producer."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any, Iterable

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

RISK_BUDGET_SCHEMA_VERSION = "risk003-dynamic-risk-budget-v1.0.0"
RISK_BUDGET_SPECIFICATION = {
    "schema_version": RISK_BUDGET_SCHEMA_VERSION,
    "clock": "only state available_at <= known_at may affect a budget",
    "dependencies": ["RISK-001", "RISK-002", "ML-004", "PORT-004"],
    "controls": [
        "volatility_target",
        "regime_multiplier",
        "calibrated_confidence",
        "uncertainty_penalty",
        "hysteresis",
        "delayed_bounded_recovery",
        "expiry",
        "capacity_cap",
    ],
    "failure_policy": "static conservative limit with no dynamic increase",
    "authority": {"allocation": False, "capital": False, "orders": False, "promotion": False},
}


class DynamicRiskBudgetError(ValueError):
    """Risk-budget evidence cannot support a truthful bounded recommendation."""


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise DynamicRiskBudgetError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DynamicRiskBudgetError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _number(value: Any, field: str, *, maximum: float | None = None, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise DynamicRiskBudgetError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise DynamicRiskBudgetError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or result < 0 or (positive and result <= 0) or (maximum is not None and result > maximum):
        raise DynamicRiskBudgetError(f"{field} is outside its declared bounds")
    return result


def _configuration(raw: dict[str, Any]) -> dict[str, Any]:
    config = {
        "base_risk_fraction": _number(raw.get("base_risk_fraction"), "base_risk_fraction", positive=True, maximum=1),
        "minimum_risk_fraction": _number(raw.get("minimum_risk_fraction"), "minimum_risk_fraction", maximum=1),
        "maximum_risk_fraction": _number(raw.get("maximum_risk_fraction"), "maximum_risk_fraction", positive=True, maximum=1),
        "static_conservative_fraction": _number(raw.get("static_conservative_fraction"), "static_conservative_fraction", maximum=1),
        "target_annualized_volatility": _number(raw.get("target_annualized_volatility"), "target_annualized_volatility", positive=True),
        "uncertainty_penalty": _number(raw.get("uncertainty_penalty"), "uncertainty_penalty", maximum=1),
        "confidence_floor": _number(raw.get("confidence_floor"), "confidence_floor", maximum=1),
        "hysteresis_fraction": _number(raw.get("hysteresis_fraction"), "hysteresis_fraction", maximum=1),
        "maximum_upward_step": _number(raw.get("maximum_upward_step"), "maximum_upward_step", maximum=1),
        "expiry_seconds": _number(raw.get("expiry_seconds"), "expiry_seconds", positive=True),
    }
    recovery = raw.get("recovery_observations")
    if isinstance(recovery, bool) or not isinstance(recovery, int) or recovery < 1:
        raise DynamicRiskBudgetError("recovery_observations must be a positive integer")
    config["recovery_observations"] = recovery
    multipliers = raw.get("regime_multipliers")
    if not isinstance(multipliers, dict) or not multipliers:
        raise DynamicRiskBudgetError("regime_multipliers must be non-empty")
    config["regime_multipliers"] = {
        str(key): _number(value, f"regime_multipliers.{key}", maximum=1)
        for key, value in sorted(multipliers.items())
    }
    if not (
        config["minimum_risk_fraction"]
        <= config["static_conservative_fraction"]
        <= config["base_risk_fraction"]
        <= config["maximum_risk_fraction"]
    ):
        raise DynamicRiskBudgetError("risk floors, fallback, base and cap are not ordered")
    return config


def _dependency(value: ProducerReceipt | dict[str, Any], milestone: str, result_key: str) -> dict[str, Any]:
    receipt = value.as_dict() if isinstance(value, ProducerReceipt) else value
    if not verify_receipt(receipt) or receipt.get("milestone") != milestone:
        raise DynamicRiskBudgetError(f"RISK-003 requires an exact {milestone} receipt")
    if not receipt["result"].get(result_key, False):
        raise DynamicRiskBudgetError(f"{milestone} is not qualified for RISK-003")
    return receipt


def _state(raw: dict[str, Any], known_at: datetime) -> dict[str, Any] | None:
    required = {
        "state_id", "observed_at", "available_at", "regime", "annualized_volatility",
        "model_confidence", "uncertainty", "source_digest",
    }
    missing = sorted(key for key in required if raw.get(key) in (None, ""))
    if missing:
        raise DynamicRiskBudgetError(f"risk state is missing {', '.join(missing)}")
    observed = _time(raw["observed_at"], "observed_at")
    available = _time(raw["available_at"], "available_at")
    if available < observed:
        raise DynamicRiskBudgetError("available_at precedes observed_at")
    if available > known_at:
        return None
    source_digest = str(raw["source_digest"])
    if len(source_digest) != 64 or any(char not in "0123456789abcdef" for char in source_digest):
        raise DynamicRiskBudgetError("source_digest must be lowercase sha256")
    normalized = {
        "state_id": str(raw["state_id"]),
        "observed_at": observed.isoformat(),
        "available_at": available.isoformat(),
        "regime": str(raw["regime"]),
        "annualized_volatility": _number(raw["annualized_volatility"], "annualized_volatility", positive=True),
        "model_confidence": _number(raw["model_confidence"], "model_confidence", maximum=1),
        "uncertainty": _number(raw["uncertainty"], "uncertainty", maximum=1),
        "source_digest": source_digest,
    }
    normalized["state_digest"] = digest(normalized)
    return normalized


def dynamic_risk_budget(
    *,
    states: Iterable[dict[str, Any]],
    known_at: datetime,
    prior_risk_fraction: float,
    requested_capital: float,
    capacity_supported_notional: float,
    configuration: dict[str, Any],
) -> dict[str, Any]:
    """Calculate a causal, anti-procyclical risk-budget trajectory."""

    known = _time(known_at, "known_at")
    config = _configuration(configuration)
    prior = _number(prior_risk_fraction, "prior_risk_fraction", maximum=1)
    capital = _number(requested_capital, "requested_capital", positive=True)
    capacity = _number(capacity_supported_notional, "capacity_supported_notional")
    if prior > config["maximum_risk_fraction"]:
        raise DynamicRiskBudgetError("prior_risk_fraction exceeds the declared cap")

    identities: dict[str, str] = {}
    accepted: list[dict[str, Any]] = []
    future_excluded = 0
    for raw in states:
        item = _state(raw, known)
        if item is None:
            future_excluded += 1
            continue
        prior_digest = identities.get(item["state_id"])
        if prior_digest and prior_digest != item["state_digest"]:
            raise DynamicRiskBudgetError("state identity was reused with different content")
        identities[item["state_id"]] = item["state_digest"]
        accepted.append(item)
    accepted.sort(key=lambda item: (item["available_at"], item["state_id"]))

    failures: list[str] = []
    if not accepted:
        failures.append("risk_state_unavailable")
    if capacity <= 0:
        failures.append("capacity_unavailable")
    if accepted:
        age = (known - _time(accepted[-1]["observed_at"], "observed_at")).total_seconds()
        if age < 0:
            failures.append("future_risk_state")
        if age > config["expiry_seconds"]:
            failures.append("risk_state_expired")

    budget = min(prior, config["static_conservative_fraction"] if failures else config["maximum_risk_fraction"])
    trajectory: list[dict[str, Any]] = []
    recovery_streak = 0
    for item in accepted:
        regime_multiplier = config["regime_multipliers"].get(item["regime"])
        if regime_multiplier is None:
            failures.append(f"unknown_regime:{item['regime']}")
            regime_multiplier = 0.0
        volatility_scale = min(1.0, config["target_annualized_volatility"] / item["annualized_volatility"])
        uncertainty_scale = 1.0 - config["uncertainty_penalty"] * item["uncertainty"]
        confidence_scale = max(0.0, item["model_confidence"] - config["confidence_floor"]) / max(1e-12, 1.0 - config["confidence_floor"])
        desired = config["base_risk_fraction"] * min(
            volatility_scale, regime_multiplier, uncertainty_scale, confidence_scale
        )
        desired = min(config["maximum_risk_fraction"], max(config["minimum_risk_fraction"], desired))
        action = "hold_hysteresis"
        if desired < budget - config["hysteresis_fraction"]:
            budget = desired
            recovery_streak = 0
            action = "reduce_immediately"
        elif desired > budget + config["hysteresis_fraction"]:
            recovery_streak += 1
            if recovery_streak >= config["recovery_observations"]:
                budget = min(desired, budget + config["maximum_upward_step"])
                action = "bounded_recovery"
                recovery_streak = 0
            else:
                action = "hold_pending_recovery"
        else:
            recovery_streak = 0
        trajectory.append(
            {
                **item,
                "volatility_scale": round(volatility_scale, 12),
                "regime_scale": round(regime_multiplier, 12),
                "uncertainty_scale": round(uncertainty_scale, 12),
                "confidence_scale": round(confidence_scale, 12),
                "desired_risk_fraction": round(desired, 12),
                "effective_risk_fraction": round(budget, 12),
                "action": action,
            }
        )

    if failures:
        budget = min(budget, prior, config["static_conservative_fraction"])
    supported_capital = min(capital, capacity)
    result = {
        "schema_version": RISK_BUDGET_SCHEMA_VERSION,
        "known_at": known.isoformat(),
        "future_states_excluded": future_excluded,
        "requested_capital": capital,
        "capacity_supported_notional": capacity,
        "budget_capital": round(supported_capital, 12),
        "prior_risk_fraction": prior,
        "budget_trajectory": trajectory,
        "effective_risk_fraction": round(budget, 12),
        "effective_risk_notional": round(supported_capital * budget, 12),
        "failures": sorted(set(failures)),
        "qualified": not failures,
        "decision": "dynamic_budget_qualified" if not failures else "static_conservative_fallback",
        "authority": RISK_BUDGET_SPECIFICATION["authority"],
    }
    result["budget_digest"] = digest(result)
    return result


def dynamic_risk_budget_receipt(
    *,
    risk001_receipt: ProducerReceipt | dict[str, Any],
    risk002_receipt: ProducerReceipt | dict[str, Any],
    ml004_receipt: ProducerReceipt | dict[str, Any],
    port004_receipt: ProducerReceipt | dict[str, Any],
    states: Iterable[dict[str, Any]],
    known_at: datetime,
    prior_risk_fraction: float,
    requested_capital: float,
    configuration: dict[str, Any],
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    """Bind qualified upstream evidence into an immutable RISK-003 receipt."""

    dependencies = {
        "risk001": _dependency(risk001_receipt, "RISK-001", "admissible"),
        "risk002": _dependency(risk002_receipt, "RISK-002", "allowed"),
        "ml004": _dependency(ml004_receipt, "ML-004", "qualified"),
        "port004": _dependency(port004_receipt, "PORT-004", "qualified"),
    }
    if any(item["dataset_digest"] != dataset_digest for item in dependencies.values()):
        raise DynamicRiskBudgetError("dependency dataset digests do not match RISK-003")
    state_values = list(states)
    result = dynamic_risk_budget(
        states=state_values,
        known_at=known_at,
        prior_risk_fraction=prior_risk_fraction,
        requested_capital=requested_capital,
        capacity_supported_notional=dependencies["port004"]["result"]["capacity_supported_notional"],
        configuration=configuration,
    )
    result["dependency_receipts"] = {key: value["receipt_digest"] for key, value in dependencies.items()}
    result["risk_budget_schema_digest"] = digest(RISK_BUDGET_SPECIFICATION)
    result["claim"] = "bounded risk-budget evidence only; no allocation, capital, order or promotion authority"
    return build_receipt(
        milestone="RISK-003",
        producer="bt.institutional.risk_budget.dynamic_risk_budget_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"dependencies": dependencies, "states": state_values},
        dataset_digest=dataset_digest,
        configuration={
            "known_at": _time(known_at, "known_at").isoformat(),
            "prior_risk_fraction": prior_risk_fraction,
            "requested_capital": requested_capital,
            **configuration,
        },
        artifacts={"budget_digest": result["budget_digest"], "budget_trajectory": result["budget_trajectory"]},
        result=result,
    )
