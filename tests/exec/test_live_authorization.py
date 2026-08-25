from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import UTC, datetime

import pandas as pd
import pytest

from bt.core.enums import OrderType, Side
from bt.core.types import OrderIntent
from bt.exec.services.live_authorization import (
    LiveAuthorizationError,
    finalize_live_authorization_bundle,
    load_live_authorization,
    validate_live_authorization_bundle,
)
from bt.exec.services.live_controls import CanaryGuard, load_canary_policy

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)


def config() -> dict:
    return {
        "broker": {
            "venue": "bybit",
            "environment": "live",
            "symbols": ["BTCUSDT"],
        },
        "live_controls": {"enabled": True, "canary_mode": True},
        "canary": {
            "max_order_qty": 0.001,
            "max_notional_usd": 100,
            "max_open_orders_total": 1,
            "max_total_open_positions": 1,
            "max_orders_per_hour": 2,
            "max_gross_notional_usd": 100,
            "max_daily_loss_usd": 10,
            "max_session_loss_usd": 10,
            "max_duration_seconds": 900,
            "max_market_data_age_seconds": 10,
        },
    }


def bundle() -> dict:
    return finalize_live_authorization_bundle(
        {
            "schema_version": "live-canary-authorization-bundle-v1.0.0",
            "plan": {
                "environment": "live",
                "venue": "bybit",
                "account_profile": "micro-live-no-withdrawal",
                "symbols": ["BTCUSDT"],
                "order_types": ["market"],
                "starts_at": "2026-08-25T11:55:00Z",
                "expires_at": "2026-08-25T12:15:00Z",
                "limits": {
                    "max_order_quantity": 0.001,
                    "max_order_notional_usd": 100,
                    "max_open_orders": 1,
                    "max_open_positions": 1,
                    "max_orders_per_session": 2,
                    "max_gross_notional_usd": 100,
                    "max_daily_loss_usd": 10,
                    "max_session_loss_usd": 10,
                    "max_duration_seconds": 900,
                    "max_market_data_age_seconds": 10,
                },
                "evidence": {
                    "portfolio_candidate": {
                        "digest": "1" * 64,
                        "status": "candidate",
                        "allocated": False,
                    },
                    "demo_qualification": {
                        "digest": "2" * 64,
                        "status": "qualified",
                    },
                    "live_connector_certification": {
                        "digest": "3" * 64,
                        "status": "certified",
                    },
                    "operational_readiness": {
                        "digest": "4" * 64,
                        "status": "ready",
                    },
                    "kill_and_rollback_rehearsal": {
                        "digest": "5" * 64,
                        "status": "passed",
                    },
                },
                "rollback": "cancel_open_orders_flatten_and_reconcile",
                "scale_authority": False,
            },
            "approval": {
                "approval_id": "founder-live-001",
                "approved_by": "founder-operator",
                "approved_at": "2026-08-25T11:58:00Z",
                "expires_at": "2026-08-25T12:15:00Z",
                "approval_signature": "a" * 64,
                "status": "approved",
            },
        }
    )


def test_authorization_binds_plan_evidence_limits_and_approval() -> None:
    first = bundle()
    second = bundle()
    assert first == second
    receipt = validate_live_authorization_bundle(first, config(), now=NOW)
    assert receipt["plan_digest"] == first["plan"]["plan_digest"]
    assert receipt["scale_authority"] is False


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (("approval", "status", "pending"), "not approved"),
        (("approval", "expires_at", "2026-08-25T11:59:00Z"), "not currently valid"),
        (("plan", "scale_authority", True), "scale authority"),
        (("plan", "rollback", "none"), "canonical rollback"),
    ],
)
def test_authorization_mutations_fail_closed(
    mutation: tuple[str, str, object], message: str
) -> None:
    changed = bundle()
    changed[mutation[0]][mutation[1]] = mutation[2]
    changed = finalize_live_authorization_bundle(changed)
    with pytest.raises(LiveAuthorizationError, match=message):
        validate_live_authorization_bundle(changed, config(), now=NOW)


def test_runtime_limits_cannot_exceed_the_signed_plan() -> None:
    changed_config = deepcopy(config())
    changed_config["canary"]["max_notional_usd"] = 101
    with pytest.raises(LiveAuthorizationError, match="exceeds approved"):
        validate_live_authorization_bundle(bundle(), changed_config, now=NOW)


def test_authorization_requires_enabled_canary_controls() -> None:
    changed_config = deepcopy(config())
    changed_config["live_controls"]["canary_mode"] = False
    with pytest.raises(LiveAuthorizationError, match="requires canary controls"):
        validate_live_authorization_bundle(bundle(), changed_config, now=NOW)


def test_bundle_file_requires_exact_owner_and_mode(tmp_path) -> None:
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(bundle()), encoding="utf-8")
    path.chmod(0o600)
    receipt = load_live_authorization(
        path, config(), now=NOW, required_owner_uid=os.getuid()
    )
    assert receipt["approval_id"] == "founder-live-001"
    path.chmod(0o644)
    with pytest.raises(LiveAuthorizationError, match="mode 0600"):
        load_live_authorization(path, config(), now=NOW, required_owner_uid=os.getuid())


def test_canary_rejects_stale_future_loss_and_gross_risk() -> None:
    guard = CanaryGuard(load_canary_policy(config()), session_started_at=NOW)
    intent = OrderIntent(
        ts=pd.Timestamp("2026-08-25T12:00:00Z"),
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=0.001,
        order_type=OrderType.MARKET,
        limit_price=None,
        reason="live-001-test",
    )
    kwargs = {
        "intent": intent,
        "open_orders": [],
        "positions": [],
        "current_price": 50_000.0,
        "current_equity": 1_000.0,
        "starting_equity": 1_000.0,
        "gross_notional_usd": 0.0,
        "wall_clock": NOW,
    }
    assert guard.validate_intent(
        **kwargs, bar_ts=pd.Timestamp("2026-08-25T11:59:55Z")
    ) is None
    assert guard.validate_intent(
        **kwargs, bar_ts=pd.Timestamp("2026-08-25T11:00:00Z")
    ) == "market_data_not_wall_clock_fresh"
    assert guard.validate_intent(
        **(kwargs | {"current_equity": 989.0}),
        bar_ts=pd.Timestamp("2026-08-25T11:59:55Z"),
    ) == "max_daily_loss_usd_exceeded"
    assert guard.validate_intent(
        **(kwargs | {"gross_notional_usd": 75.0}),
        bar_ts=pd.Timestamp("2026-08-25T11:59:55Z"),
    ) == "max_gross_notional_usd_exceeded"


def test_canary_rejects_an_expired_session() -> None:
    guard = CanaryGuard(
        load_canary_policy(config()),
        session_started_at=datetime(2026, 8, 25, 11, 40, tzinfo=UTC),
    )
    intent = OrderIntent(
        ts=pd.Timestamp("2026-08-25T11:59:55Z"),
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=0.001,
        order_type=OrderType.MARKET,
        limit_price=None,
        reason="live-001-duration-test",
    )
    assert guard.validate_intent(
        intent=intent,
        open_orders=[],
        positions=[],
        current_price=50_000.0,
        current_equity=1_000.0,
        starting_equity=1_000.0,
        gross_notional_usd=0.0,
        bar_ts=pd.Timestamp("2026-08-25T11:59:55Z"),
        wall_clock=NOW,
    ) == "max_duration_seconds_exceeded"
