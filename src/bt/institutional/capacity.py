"""PORT-004 turnover, cost, capacity and liquidity producer."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any, Iterable

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

CAPACITY_SCHEMA_VERSION = "port004-capacity-liquidity-v1.0.0"
CAPACITY_SPECIFICATION = {
    "schema_version": CAPACITY_SCHEMA_VERSION,
    "clock": "only liquidity available_at <= known_at may support a claim",
    "turnover": ["gross_l1", "one_way"],
    "costs": ["execution_shortfall", "explicit_fee", "scale_penalty", "liquidity_shock"],
    "capacity_constraints": ["participation", "depth", "cost", "position_liquidation"],
    "strata": ["venue_id", "order_type", "size_bucket", "regime"],
    "unsupported_action": "zero executable capacity and abstain",
    "authority": {"allocation": False, "capital": False, "orders": False, "promotion": False},
}


class PortfolioCapacityError(ValueError):
    """Portfolio capacity inputs cannot support a truthful claim."""


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise PortfolioCapacityError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PortfolioCapacityError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise PortfolioCapacityError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioCapacityError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or result < 0 or (positive and result <= 0):
        qualifier = "positive" if positive else "non-negative"
        raise PortfolioCapacityError(f"{field} must be {qualifier} and finite")
    return result


def _rounded(value: float) -> float:
    return round(float(value), 12)


def _snapshot(raw: dict[str, Any], known_at: datetime) -> dict[str, Any] | None:
    required = {
        "snapshot_id",
        "candidate_id",
        "venue_id",
        "listing_id",
        "order_type",
        "size_bucket",
        "regime",
        "observed_at",
        "available_at",
        "average_daily_dollar_volume",
        "executable_depth_notional",
        "reference_trade_notional",
        "spread_bps",
        "source_digest",
    }
    missing = sorted(field for field in required if raw.get(field) in (None, ""))
    if missing:
        raise PortfolioCapacityError(f"liquidity snapshot is missing {', '.join(missing)}")
    observed = _time(raw["observed_at"], "observed_at")
    available = _time(raw["available_at"], "available_at")
    if available < observed:
        raise PortfolioCapacityError("available_at precedes observed_at")
    if available > known_at:
        return None
    source_digest = str(raw["source_digest"])
    if len(source_digest) != 64 or any(char not in "0123456789abcdef" for char in source_digest):
        raise PortfolioCapacityError("source_digest must be lowercase sha256")
    normalized = {
        "snapshot_id": str(raw["snapshot_id"]),
        "candidate_id": str(raw["candidate_id"]),
        "venue_id": str(raw["venue_id"]),
        "listing_id": str(raw["listing_id"]),
        "order_type": str(raw["order_type"]),
        "size_bucket": str(raw["size_bucket"]),
        "regime": str(raw["regime"]),
        "observed_at": observed.isoformat(),
        "available_at": available.isoformat(),
        "average_daily_dollar_volume": _number(
            raw["average_daily_dollar_volume"], "average_daily_dollar_volume", positive=True
        ),
        "executable_depth_notional": _number(
            raw["executable_depth_notional"], "executable_depth_notional", positive=True
        ),
        "reference_trade_notional": _number(
            raw["reference_trade_notional"], "reference_trade_notional", positive=True
        ),
        "spread_bps": _number(raw["spread_bps"], "spread_bps"),
        "source_digest": source_digest,
    }
    normalized["snapshot_digest"] = digest(normalized)
    return normalized


def _configuration(raw: dict[str, Any]) -> dict[str, Any]:
    numeric = {
        "stale_after_seconds": True,
        "rebalance_horizon_days": True,
        "maximum_participation_rate": True,
        "maximum_depth_fraction": True,
        "maximum_liquidation_days": True,
        "maximum_cost_bps": True,
        "maximum_portfolio_cost_bps": True,
        "explicit_fee_bps": False,
        "stress_volume_haircut": False,
        "stress_spread_multiplier": True,
        "stress_impact_multiplier": True,
    }
    config = {key: _number(raw.get(key), key, positive=positive) for key, positive in numeric.items()}
    for key in ("maximum_participation_rate", "maximum_depth_fraction", "stress_volume_haircut"):
        if config[key] > 1:
            raise PortfolioCapacityError(f"{key} must not exceed one")
    scales = [_number(value, "scale_grid", positive=True) for value in raw.get("scale_grid", [])]
    if not scales or scales != sorted(set(scales)):
        raise PortfolioCapacityError("scale_grid must be non-empty, unique and increasing")
    config["scale_grid"] = scales
    return config


def _calibration_cost(stratum: dict[str, Any]) -> float:
    holdout = stratum.get("holdout", {}).get("implementation_shortfall_bps", {})
    interval = holdout.get("mean_interval_95") or {}
    values = [stratum.get("pessimistic_cost_bps"), holdout.get("p95"), interval.get("upper")]
    return max(_number(value, "calibrated execution cost") for value in values if value is not None)


def _cost_bps(
    *, trade_notional: float, snapshot: dict[str, Any], execution_cost_bps: float, config: dict[str, Any], stressed: bool
) -> float:
    ratio = trade_notional / snapshot["reference_trade_notional"] if trade_notional else 0.0
    impact = execution_cost_bps * max(1.0, ratio)
    if stressed:
        impact *= config["stress_impact_multiplier"]
        impact += snapshot["spread_bps"] * (config["stress_spread_multiplier"] - 1.0)
    return _rounded(config["explicit_fee_bps"] + impact)


def _candidate_capacity(
    *, snapshot: dict[str, Any], execution_cost_bps: float, config: dict[str, Any], stressed: bool
) -> dict[str, float]:
    volume = snapshot["average_daily_dollar_volume"]
    depth = snapshot["executable_depth_notional"]
    if stressed:
        volume *= 1.0 - config["stress_volume_haircut"]
        depth *= 1.0 - config["stress_volume_haircut"]
    participation = volume * config["maximum_participation_rate"] * config["rebalance_horizon_days"]
    depth_limit = depth * config["maximum_depth_fraction"]
    fixed = config["explicit_fee_bps"]
    adjusted_execution = execution_cost_bps * (config["stress_impact_multiplier"] if stressed else 1.0)
    if stressed:
        fixed += snapshot["spread_bps"] * (config["stress_spread_multiplier"] - 1.0)
    if fixed + adjusted_execution > config["maximum_cost_bps"] or adjusted_execution <= 0:
        cost_limit = 0.0
    else:
        multiple = max(1.0, (config["maximum_cost_bps"] - fixed) / adjusted_execution)
        cost_limit = snapshot["reference_trade_notional"] * multiple
    trade = min(participation, depth_limit, cost_limit)
    position = volume * config["maximum_participation_rate"] * config["maximum_liquidation_days"]
    return {
        "participation_notional": _rounded(participation),
        "depth_notional": _rounded(depth_limit),
        "cost_notional": _rounded(cost_limit),
        "trade_notional": _rounded(max(0.0, trade)),
        "position_notional": _rounded(max(0.0, position)),
    }


def portfolio_capacity(
    *,
    target_weights: dict[str, float],
    prior_weights: dict[str, float],
    liquidity_snapshots: Iterable[dict[str, Any]],
    execution_calibration: dict[str, Any],
    known_at: datetime,
    requested_capital: float,
    configuration: dict[str, Any],
) -> dict[str, Any]:
    """Produce deterministic base/stress capacity and cost curves."""

    known = _time(known_at, "known_at")
    capital = _number(requested_capital, "requested_capital", positive=True)
    config = _configuration(configuration)
    candidates = sorted(target_weights)
    if not candidates or set(prior_weights) != set(candidates):
        raise PortfolioCapacityError("target and prior weights must share non-empty candidates")
    target = {key: _number(target_weights[key], f"target weight {key}") for key in candidates}
    prior = {key: _number(prior_weights[key], f"prior weight {key}") for key in candidates}
    if abs(sum(target.values()) - 1.0) > 1e-9 or abs(sum(prior.values()) - 1.0) > 1e-9:
        raise PortfolioCapacityError("target and prior weights must each sum to one")

    snapshots: dict[str, dict[str, Any]] = {}
    identities: dict[str, str] = {}
    future_excluded = 0
    for raw in liquidity_snapshots:
        item = _snapshot(raw, known)
        if item is None:
            future_excluded += 1
            continue
        prior_digest = identities.get(item["snapshot_id"])
        if prior_digest and prior_digest != item["snapshot_digest"]:
            raise PortfolioCapacityError("snapshot identity was reused with different content")
        identities[item["snapshot_id"]] = item["snapshot_digest"]
        candidate = item["candidate_id"]
        existing = snapshots.get(candidate)
        if existing and existing["snapshot_digest"] != item["snapshot_digest"]:
            raise PortfolioCapacityError(f"multiple point-in-time snapshots supplied for {candidate}")
        snapshots[candidate] = item

    strata = execution_calibration.get("strata", [])
    failures: list[str] = []
    rows: list[dict[str, Any]] = []
    base_limits: list[float] = []
    stress_limits: list[float] = []
    for candidate in candidates:
        snapshot = snapshots.get(candidate)
        if snapshot is None:
            failures.append(f"missing_liquidity:{candidate}")
            continue
        age = (known - _time(snapshot["observed_at"], "observed_at")).total_seconds()
        if age < 0:
            failures.append(f"future_observation:{candidate}")
        if age > config["stale_after_seconds"]:
            failures.append(f"stale_liquidity:{candidate}")
        matches = [
            item
            for item in strata
            if (
                item.get("venue_id"),
                item.get("order_type"),
                item.get("size_bucket"),
                item.get("regime"),
            )
            == (
                snapshot["venue_id"],
                snapshot["order_type"],
                snapshot["size_bucket"],
                snapshot["regime"],
            )
        ]
        if len(matches) != 1:
            failures.append(f"execution_stratum_unavailable:{candidate}")
            continue
        execution_cost = _calibration_cost(matches[0])
        base = _candidate_capacity(snapshot=snapshot, execution_cost_bps=execution_cost, config=config, stressed=False)
        stress = _candidate_capacity(snapshot=snapshot, execution_cost_bps=execution_cost, config=config, stressed=True)
        delta = abs(target[candidate] - prior[candidate])
        candidate_base_limits = [base["position_notional"] / target[candidate]] if target[candidate] else []
        candidate_stress_limits = [stress["position_notional"] / target[candidate]] if target[candidate] else []
        if delta:
            candidate_base_limits.append(base["trade_notional"] / delta)
            candidate_stress_limits.append(stress["trade_notional"] / delta)
        base_capital = min(candidate_base_limits, default=math.inf)
        stress_capital = min(candidate_stress_limits, default=math.inf)
        base_limits.append(base_capital)
        stress_limits.append(stress_capital)
        rows.append(
            {
                "candidate_id": candidate,
                "venue_id": snapshot["venue_id"],
                "listing_id": snapshot["listing_id"],
                "target_weight": target[candidate],
                "prior_weight": prior[candidate],
                "trade_weight": _rounded(delta),
                "liquidity_age_seconds": _rounded(age),
                "execution_cost_bps": _rounded(execution_cost),
                "base": base | {"capital_limit": _rounded(base_capital)},
                "stress": stress | {"capital_limit": _rounded(stress_capital)},
                "snapshot_digest": snapshot["snapshot_digest"],
            }
        )

    if not execution_calibration.get("qualified", False):
        failures.append("execution_calibration_unqualified")
    base_capacity = min(base_limits, default=0.0) if not failures else 0.0
    stress_capacity = min(stress_limits, default=0.0) if not failures else 0.0
    gross_l1 = sum(abs(target[key] - prior[key]) for key in candidates)
    curves = []
    for scale in config["scale_grid"]:
        scaled_capital = capital * scale
        base_cost = 0.0
        stress_cost = 0.0
        for row in rows:
            snapshot = snapshots[row["candidate_id"]]
            trade = row["trade_weight"] * scaled_capital
            base_cost += trade * _cost_bps(
                trade_notional=trade,
                snapshot=snapshot,
                execution_cost_bps=row["execution_cost_bps"],
                config=config,
                stressed=False,
            ) / 10_000
            stress_cost += trade * _cost_bps(
                trade_notional=trade,
                snapshot=snapshot,
                execution_cost_bps=row["execution_cost_bps"],
                config=config,
                stressed=True,
            ) / 10_000
        curves.append(
            {
                "scale": scale,
                "capital": _rounded(scaled_capital),
                "turnover_notional": _rounded(gross_l1 * scaled_capital),
                "base_cost": _rounded(base_cost),
                "base_cost_bps_of_capital": _rounded(base_cost / scaled_capital * 10_000),
                "stress_cost": _rounded(stress_cost),
                "stress_cost_bps_of_capital": _rounded(stress_cost / scaled_capital * 10_000),
                "within_base_capacity": bool(not failures and scaled_capital <= base_capacity + 1e-12),
                "within_stress_capacity": bool(not failures and scaled_capital <= stress_capacity + 1e-12),
            }
        )
    requested = next((row for row in curves if abs(row["scale"] - 1.0) <= 1e-12), None)
    if requested is None:
        raise PortfolioCapacityError("scale_grid must contain 1.0 for requested-capital adjudication")
    if requested["stress_cost_bps_of_capital"] > config["maximum_portfolio_cost_bps"]:
        failures.append("portfolio_cost_limit_breached")
    qualified = not failures and requested["within_base_capacity"] and requested["within_stress_capacity"]
    if not requested["within_base_capacity"]:
        failures.append("base_capacity_breached")
    if not requested["within_stress_capacity"]:
        failures.append("stress_capacity_breached")
    expected_return = float(execution_calibration.get("expected_return", 0.0))
    gross_expected = capital * expected_return
    result = {
        "schema_version": CAPACITY_SCHEMA_VERSION,
        "known_at": known.isoformat(),
        "requested_capital": capital,
        "future_snapshots_excluded": future_excluded,
        "turnover": {
            "gross_l1": _rounded(gross_l1),
            "one_way": _rounded(gross_l1 / 2),
            "requested_trade_notional": _rounded(_rounded(gross_l1) * capital),
        },
        "candidate_constraints": rows,
        "cost_capacity_curve": curves,
        "base_capital_capacity": _rounded(base_capacity),
        "stressed_capital_capacity": _rounded(stress_capacity),
        "requested_economics": {
            "expected_return_before_cost": _rounded(expected_return),
            "gross_expected_return": _rounded(gross_expected),
            "base_rebalance_cost": requested["base_cost"],
            "stress_rebalance_cost": requested["stress_cost"],
            "base_net_expected_return": _rounded(gross_expected - requested["base_cost"]),
            "stress_net_expected_return": _rounded(gross_expected - requested["stress_cost"]),
        },
        "failures": sorted(set(failures)),
        "qualified": qualified,
        "decision": "capacity_qualified" if qualified else "abstain_zero_executable_capacity",
        "capacity_supported_notional": _rounded(capital if qualified else 0.0),
        "authority": CAPACITY_SPECIFICATION["authority"],
    }
    result["capacity_digest"] = digest(result)
    return result


def capacity_dossier_receipt(
    *,
    port003_receipt: ProducerReceipt | dict[str, Any],
    exec005_receipt: ProducerReceipt | dict[str, Any],
    prior_weights: dict[str, float],
    liquidity_snapshots: Iterable[dict[str, Any]],
    known_at: datetime,
    requested_capital: float,
    configuration: dict[str, Any],
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    """Bind PORT-003 construction and EXEC-005 calibration into PORT-004 evidence."""

    construction = port003_receipt.as_dict() if isinstance(port003_receipt, ProducerReceipt) else port003_receipt
    calibration = exec005_receipt.as_dict() if isinstance(exec005_receipt, ProducerReceipt) else exec005_receipt
    if not verify_receipt(construction) or construction["milestone"] != "PORT-003":
        raise PortfolioCapacityError("PORT-004 requires an admissible PORT-003 receipt")
    if not construction["result"].get("valid"):
        raise PortfolioCapacityError("PORT-003 construction is not valid")
    if not verify_receipt(calibration) or calibration["milestone"] != "EXEC-005":
        raise PortfolioCapacityError("PORT-004 requires an admissible EXEC-005 receipt")
    snapshots = list(liquidity_snapshots)
    target = construction["result"]["selection"]["weights"]
    calibration_result = dict(calibration["result"]["calibration"])
    calibration_result["expected_return"] = construction["result"]["selection"]["metrics"]["expected_return"]
    result = portfolio_capacity(
        target_weights=target,
        prior_weights=prior_weights,
        liquidity_snapshots=snapshots,
        execution_calibration=calibration_result,
        known_at=known_at,
        requested_capital=requested_capital,
        configuration=configuration,
    )
    result["dependency_receipts"] = {
        "port003": construction["receipt_digest"],
        "exec005": calibration["receipt_digest"],
    }
    result["capacity_schema_digest"] = digest(CAPACITY_SPECIFICATION)
    result["claim"] = "turnover, cost, capacity and liquidity evidence only; no allocation, capital or order authority"
    return build_receipt(
        milestone="PORT-004",
        producer="bt.institutional.capacity.capacity_dossier_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "port003_receipt": construction,
            "exec005_receipt": calibration,
            "prior_weights": prior_weights,
            "liquidity_snapshots": snapshots,
        },
        dataset_digest=dataset_digest,
        configuration={
            "known_at": _time(known_at, "known_at").isoformat(),
            "requested_capital": requested_capital,
            **configuration,
        },
        artifacts={"capacity_digest": result["capacity_digest"], "cost_capacity_curve": result["cost_capacity_curve"]},
        result=result,
    )
