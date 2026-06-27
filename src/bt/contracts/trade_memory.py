"""Canonical, causal cross-stage trade memory and advisory assessment."""
from __future__ import annotations

from datetime import datetime, timezone
import math
from statistics import mean
from typing import Any


STAGES = {"backtest", "demo", "live_canary", "live"}
PRODUCT_TYPES = {"spot", "perpetual"}
ASSESSMENTS = {"supportive", "neutral", "caution", "block", "insufficient_evidence"}


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("memory_timestamp_must_be_utc")
    return parsed.astimezone(timezone.utc)


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def validate_state_snapshot(snapshot: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != "decision_state_snapshot_v1":
        errors.append("state_snapshot_schema_invalid")
    if snapshot.get("stage") not in STAGES:
        errors.append("state_snapshot_stage_invalid")
    if not isinstance(snapshot.get("features"), dict):
        errors.append("state_snapshot_features_required")
        return errors
    try:
        decision_at = _time(str(snapshot.get("decision_at", "")))
        captured_at = _time(str(snapshot.get("captured_at", "")))
        if captured_at > decision_at:
            errors.append("state_snapshot_captured_after_decision")
        timestamps = snapshot.get("feature_timestamps", {})
        if not isinstance(timestamps, dict):
            errors.append("state_snapshot_feature_timestamps_required")
        else:
            for key in snapshot["features"]:
                if key not in timestamps:
                    errors.append(f"state_snapshot_feature_timestamp_missing:{key}")
                    continue
                if _time(str(timestamps[key])) > decision_at:
                    errors.append(f"state_snapshot_future_feature:{key}")
    except (ValueError, TypeError):
        errors.append("state_snapshot_timestamp_invalid")
    for key, value in snapshot.get("features", {}).items():
        if _finite(value) is None:
            errors.append(f"state_snapshot_feature_non_finite:{key}")
    if snapshot.get("future_enriched") is True:
        errors.append("state_snapshot_future_enrichment_forbidden")
    return errors


def validate_trade_episode(episode: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if episode.get("schema_version") != "trade_episode_v1":
        errors.append("trade_episode_schema_invalid")
    if episode.get("stage") not in STAGES:
        errors.append("trade_episode_stage_invalid")
    if episode.get("product_type") not in PRODUCT_TYPES:
        errors.append("trade_episode_product_type_invalid")
    for key in ("episode_id", "program_id", "strategy_spec_hash", "symbol", "side", "opened_at", "status"):
        if episode.get(key) in {None, ""}:
            errors.append(f"trade_episode_missing_{key}")
    if episode.get("status") == "closed":
        for key in ("closed_at", "entry_price", "exit_price", "net_pnl"):
            if episode.get(key) is None:
                errors.append(f"trade_episode_closed_missing_{key}")
    try:
        opened = _time(str(episode.get("opened_at", "")))
        if episode.get("closed_at") and _time(str(episode["closed_at"])) < opened:
            errors.append("trade_episode_close_before_open")
    except (ValueError, TypeError):
        errors.append("trade_episode_timestamp_invalid")
    return errors


def _distance(current: dict[str, float], historical: dict[str, float], scales: dict[str, float]) -> tuple[float, int]:
    shared = sorted(set(current) & set(historical))
    if not shared:
        return math.inf, 0
    squared = [((current[key] - historical[key]) / max(scales.get(key, 1.0), 1e-12)) ** 2 for key in shared]
    return math.sqrt(sum(squared) / len(squared)), len(shared)


def build_memory_assessment(*, assessment_id: str, account_id: str, program_id: str, strategy_spec_hash: str, current_snapshot: dict[str, Any], episodes: list[dict[str, Any]], snapshots_by_id: dict[str, dict[str, Any]], now: str, min_support: int = 8, max_distance: float = 2.5, half_life_days: float = 90.0) -> dict[str, Any]:
    snapshot_errors = validate_state_snapshot(current_snapshot)
    current = {key: value for key, raw in current_snapshot.get("features", {}).items() if (value := _finite(raw)) is not None}
    if snapshot_errors or not current:
        return _insufficient(assessment_id, account_id, program_id, strategy_spec_hash, now, snapshot_errors or ["current_state_features_missing"])

    candidates: list[tuple[dict[str, Any], dict[str, float]]] = []
    for episode in episodes:
        if validate_trade_episode(episode) or episode.get("status") != "closed":
            continue
        linked = snapshots_by_id.get(str(episode.get("decision_state_snapshot_id", "")))
        if not linked or validate_state_snapshot(linked):
            continue
        features = {key: value for key, raw in linked.get("features", {}).items() if (value := _finite(raw)) is not None}
        if features:
            candidates.append((episode, features))
    if not candidates:
        return _insufficient(assessment_id, account_id, program_id, strategy_spec_hash, now, ["no_causal_historical_episodes"])

    scales: dict[str, float] = {}
    for key in current:
        values = [features[key] for _, features in candidates if key in features]
        if len(values) >= 2:
            center = mean(values)
            scales[key] = max(math.sqrt(mean([(item - center) ** 2 for item in values])), 1e-9)
    now_dt = _time(now)
    comparable: list[dict[str, Any]] = []
    for episode, features in candidates:
        distance, shared = _distance(current, features, scales)
        if shared < max(1, math.ceil(len(current) * 0.5)) or distance > max_distance:
            continue
        age_days = max(0.0, (now_dt - _time(str(episode["closed_at"]))).total_seconds() / 86400.0)
        weight = math.exp(-math.log(2) * age_days / max(half_life_days, 1.0)) * math.exp(-distance)
        comparable.append({"episode": episode, "distance": distance, "shared_features": shared, "weight": weight})
    comparable.sort(key=lambda item: (item["distance"], -item["weight"], str(item["episode"]["episode_id"])))
    comparable = comparable[:100]
    if len(comparable) < min_support:
        result = _insufficient(assessment_id, account_id, program_id, strategy_spec_hash, now, ["comparable_support_below_minimum"])
        result.update({"support_count": len(comparable), "required_support_count": min_support, "source_episode_ids": [item["episode"]["episode_id"] for item in comparable]})
        return result

    total_weight = sum(item["weight"] for item in comparable)
    pnl = [float(item["episode"]["net_pnl"]) for item in comparable]
    expected = sum(value * item["weight"] for value, item in zip(pnl, comparable, strict=True)) / total_weight
    downside = sorted(pnl)[max(0, math.ceil(len(pnl) * 0.1) - 1)]
    wins = sum(item["weight"] for item in comparable if float(item["episode"]["net_pnl"]) > 0) / total_weight
    standard_error = math.sqrt(max(wins * (1 - wins), 0.0) / len(comparable))
    interval = [max(0.0, wins - 1.96 * standard_error), min(1.0, wins + 1.96 * standard_error)]
    same_strategy = [item for item in comparable if item["episode"].get("strategy_spec_hash") == strategy_spec_hash]
    recent_distance = mean([item["distance"] for item in comparable[: min(10, len(comparable))]])
    all_distance = mean([item["distance"] for item in comparable])
    drift = recent_distance / max(all_distance, 1e-12)
    assessment = "neutral"
    reasons: list[str] = []
    if drift > 1.5:
        assessment, reasons = "caution", ["state_drift_elevated"]
    elif interval[1] < 0.45 or expected < 0 and downside < 0:
        assessment, reasons = "block", ["comparable_outcomes_adverse"]
    elif interval[0] > 0.55 and expected > 0:
        assessment, reasons = "supportive", ["comparable_outcomes_positive"]
    else:
        reasons = ["comparable_outcomes_mixed"]
    return {
        "schema_version": "memory_assessment_v1", "assessment_id": assessment_id, "account_id": account_id,
        "program_id": program_id, "strategy_spec_hash": strategy_spec_hash, "assessment": assessment,
        "reason_codes": reasons, "support_count": len(comparable), "strategy_support_count": len(same_strategy),
        "cross_strategy_support_count": len(comparable) - len(same_strategy), "state_similarity_score": 1 / (1 + all_distance),
        "drift_ratio": drift, "expected_net_pnl": expected, "downside_p10_net_pnl": downside,
        "empirical_positive_rate": wins, "uncertainty_interval": interval, "calibration": {"status": "pending_outcome"},
        "source_episode_ids": [item["episode"]["episode_id"] for item in comparable],
        "missing_state_features": list(current_snapshot.get("missing_features", [])), "advisory_only": True,
        "may_increase_risk": False, "created_at": now,
    }


def _insufficient(assessment_id: str, account_id: str, program_id: str, strategy_spec_hash: str, now: str, reasons: list[str]) -> dict[str, Any]:
    return {"schema_version": "memory_assessment_v1", "assessment_id": assessment_id, "account_id": account_id, "program_id": program_id, "strategy_spec_hash": strategy_spec_hash, "assessment": "insufficient_evidence", "reason_codes": reasons, "support_count": 0, "strategy_support_count": 0, "cross_strategy_support_count": 0, "source_episode_ids": [], "advisory_only": True, "may_increase_risk": False, "created_at": now}
