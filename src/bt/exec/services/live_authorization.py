"""Fail-closed authorization contract for capital-bearing micro-live sessions."""

from __future__ import annotations

import hashlib
import json
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "live-canary-authorization-bundle-v1.0.0"


class LiveAuthorizationError(ValueError):
    """A live session lacks current, scoped, independently approved authority."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _is_digest(value: object) -> bool:
    text = str(value)
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _time(value: object, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise LiveAuthorizationError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LiveAuthorizationError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _positive(value: object, *, field: str) -> float:
    result = float(value)
    if result <= 0:
        raise LiveAuthorizationError(f"{field} must be positive")
    return result


def finalize_live_authorization_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    """Content-address a prepared bundle; this does not create an approval."""
    result = json.loads(json.dumps(bundle))
    result.pop("bundle_digest", None)
    plan = result.get("plan") or {}
    plan.pop("plan_digest", None)
    plan["plan_digest"] = _digest(plan)
    result["plan"] = plan
    approval = result.get("approval") or {}
    approval["plan_digest"] = plan["plan_digest"]
    result["approval"] = approval
    result["bundle_digest"] = _digest(result)
    return result


def validate_live_authorization_bundle(
    bundle: dict[str, Any],
    config: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    if bundle.get("schema_version") != SCHEMA_VERSION:
        raise LiveAuthorizationError("unsupported live authorization schema")
    supplied_bundle_digest = bundle.get("bundle_digest")
    bundle_core = {key: value for key, value in bundle.items() if key != "bundle_digest"}
    if not _is_digest(supplied_bundle_digest) or supplied_bundle_digest != _digest(
        bundle_core
    ):
        raise LiveAuthorizationError("live authorization bundle digest mismatch")
    if "secret" in _canonical(bundle).decode("ascii").lower() or any(
        key in _canonical(bundle).decode("ascii").lower()
        for key in ("api_key", "private_key", "password")
    ):
        raise LiveAuthorizationError("authorization bundle must not contain credentials")

    plan = bundle.get("plan") or {}
    supplied_plan_digest = plan.get("plan_digest")
    plan_core = {key: value for key, value in plan.items() if key != "plan_digest"}
    if not _is_digest(supplied_plan_digest) or supplied_plan_digest != _digest(plan_core):
        raise LiveAuthorizationError("live canary plan digest mismatch")
    if plan.get("environment") != "live":
        raise LiveAuthorizationError("canary plan environment must be live")
    if plan.get("rollback") != "cancel_open_orders_flatten_and_reconcile":
        raise LiveAuthorizationError("canary plan requires the canonical rollback")
    if plan.get("scale_authority") is not False:
        raise LiveAuthorizationError("micro-live plan cannot grant scale authority")

    broker = config.get("broker") if isinstance(config.get("broker"), dict) else {}
    canary = config.get("canary") if isinstance(config.get("canary"), dict) else {}
    live_controls = (
        config.get("live_controls")
        if isinstance(config.get("live_controls"), dict)
        else {}
    )
    if live_controls.get("enabled") is not True or live_controls.get("canary_mode") is not True:
        raise LiveAuthorizationError("capital-bearing live mode requires canary controls")
    if str(broker.get("environment")) != "live":
        raise LiveAuthorizationError("runtime broker environment is not live")
    if str(broker.get("venue")) != str(plan.get("venue")):
        raise LiveAuthorizationError("runtime venue does not match approved plan")
    configured_symbols = sorted(str(value) for value in broker.get("symbols", []))
    if configured_symbols != sorted(str(value) for value in plan.get("symbols", [])):
        raise LiveAuthorizationError("runtime symbols do not match approved plan")

    limits = plan.get("limits") or {}
    comparisons = {
        "max_order_quantity": "max_order_qty",
        "max_order_notional_usd": "max_notional_usd",
        "max_open_orders": "max_open_orders_total",
        "max_open_positions": "max_total_open_positions",
        "max_orders_per_session": "max_orders_per_hour",
    }
    for approved_field, config_field in comparisons.items():
        approved = _positive(limits.get(approved_field), field=approved_field)
        configured = _positive(canary.get(config_field), field=config_field)
        if configured > approved:
            raise LiveAuthorizationError(
                f"runtime {config_field} exceeds approved {approved_field}"
            )
    for field in (
        "max_gross_notional_usd",
        "max_daily_loss_usd",
        "max_session_loss_usd",
        "max_duration_seconds",
        "max_market_data_age_seconds",
    ):
        approved = _positive(limits.get(field), field=field)
        configured = _positive(canary.get(field), field=field)
        if configured > approved:
            raise LiveAuthorizationError(f"runtime {field} exceeds approved plan")

    evidence = plan.get("evidence") or {}
    required_evidence = {
        "portfolio_candidate": ("candidate", False),
        "demo_qualification": ("qualified", None),
        "live_connector_certification": ("certified", None),
        "operational_readiness": ("ready", None),
        "kill_and_rollback_rehearsal": ("passed", None),
    }
    for name, (status, allocated) in required_evidence.items():
        item = evidence.get(name) or {}
        if not _is_digest(item.get("digest")) or item.get("status") != status:
            raise LiveAuthorizationError(f"{name} evidence is not current and {status}")
        if allocated is not None and item.get("allocated") is not allocated:
            raise LiveAuthorizationError("portfolio candidate evidence exceeded authority")

    approval = bundle.get("approval") or {}
    if approval.get("status") != "approved":
        raise LiveAuthorizationError("live canary approval is not approved")
    if approval.get("plan_digest") != supplied_plan_digest:
        raise LiveAuthorizationError("approval does not bind the live canary plan")
    for field in ("approval_id", "approved_by"):
        if not str(approval.get(field, "")).strip():
            raise LiveAuthorizationError(f"approval requires {field}")
    if not _is_digest(approval.get("approval_signature")):
        raise LiveAuthorizationError("approval signature must be a digest")
    approved_at = _time(approval.get("approved_at"), field="approved_at")
    expires_at = _time(approval.get("expires_at"), field="expires_at")
    current = now.astimezone(UTC)
    if not approved_at <= current < expires_at:
        raise LiveAuthorizationError("live canary approval is not currently valid")
    if _time(plan.get("starts_at"), field="starts_at") > current:
        raise LiveAuthorizationError("live canary window has not started")
    if current >= _time(plan.get("expires_at"), field="plan.expires_at"):
        raise LiveAuthorizationError("live canary plan has expired")

    return {
        "schema_version": "live-canary-authorization-receipt-v1.0.0",
        "bundle_digest": supplied_bundle_digest,
        "plan_digest": supplied_plan_digest,
        "approval_id": approval["approval_id"],
        "approval_signature": approval["approval_signature"],
        "venue": plan["venue"],
        "symbols": sorted(plan["symbols"]),
        "expires_at": min(str(plan["expires_at"]), str(approval["expires_at"])),
        "scale_authority": False,
    }


def load_live_authorization(
    path: str | Path,
    config: dict[str, Any],
    *,
    now: datetime | None = None,
    required_owner_uid: int = 0,
) -> dict[str, Any]:
    bundle_path = Path(path)
    file_stat = bundle_path.stat()
    if file_stat.st_uid != required_owner_uid:
        raise LiveAuthorizationError("live authorization bundle has the wrong owner")
    if stat.S_IMODE(file_stat.st_mode) != 0o600:
        raise LiveAuthorizationError("live authorization bundle must have mode 0600")
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    if not isinstance(bundle, dict):
        raise LiveAuthorizationError("live authorization bundle must be an object")
    return validate_live_authorization_bundle(
        bundle, config, now=now or datetime.now(tz=UTC)
    )
