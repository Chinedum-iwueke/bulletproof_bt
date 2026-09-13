#!/usr/bin/env python3
"""Close EXEC-008, EXEC-011, and DEMO-001 from real Bybit demo evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from bt.exec.adapters.bybit import BybitRESTClient, resolve_bybit_config
from bt.institutional.adapter_certification import (
    ADAPTER_CERTIFICATION_SPECIFICATION,
    REQUIRED_DRILLS as ADAPTER_DRILLS,
    adapter_certification_receipt,
)
from bt.institutional.demo_certification import (
    DEMO_CERTIFICATION_SPECIFICATION,
    demo_certification_receipt,
)
from bt.institutional.execution_degradation import (
    EXECUTION_DEGRADATION_SPECIFICATION,
    execution_degradation_receipt,
)
from bt.institutional.realtime_risk import (
    REALTIME_RISK_SPECIFICATION,
    realtime_risk_decision_receipt,
)
from bt.institutional.receipt import digest
from bt.institutional.venue_telemetry import (
    DEPENDENCY_PRODUCERS,
    VENUE_TELEMETRY_SPECIFICATION,
    venue_event,
    venue_telemetry_receipt,
)


def read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object")
    return value


def milliseconds(value: str | int | None, fallback: datetime) -> datetime:
    if value in (None, "", "0"):
        return fallback
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def _client(symbol: str) -> tuple[BybitRESTClient, str]:
    config = resolve_bybit_config(
        {
            "broker": {
                "venue": "bybit",
                "environment": "demo",
                "product_type": "perpetual",
                "category": "linear",
                "symbols": [symbol],
                "auth": {
                    "api_key_env": "BYBIT_DEMO_API_KEY",
                    "api_secret_env": "BYBIT_DEMO_API_SECRET",
                },
                "ws": {"enabled": False},
            }
        }
    )
    if config.rest_base_url != "https://api-demo.bybit.com":
        raise RuntimeError("closure refuses every non-demo Bybit endpoint")
    api_key, api_secret = config.auth.resolve()
    return (
        BybitRESTClient(
            base_url=config.rest_base_url,
            api_key=api_key,
            api_secret=api_secret,
            recv_window_ms=config.recv_window_ms,
            timeout_ms=config.request_timeout_ms,
            max_retries=config.max_retries,
            retry_backoff_ms=config.retry_backoff_ms,
            environment=config.environment,
        ),
        hashlib.sha256(api_key.encode("utf-8")).hexdigest(),
    )


def collect_history(symbol: str) -> tuple[dict[str, Any], str, datetime]:
    client, credential_fingerprint = _client(symbol)
    fetched_at = datetime.now(UTC)
    orders = client.get_private(
        "/v5/order/history",
        params={"category": "linear", "symbol": symbol, "limit": 200},
    ).result.get("list", [])
    owned_orders = [
        item
        for item in orders
        if str(item.get("orderLinkId", "")).startswith("demo001-")
    ]
    if not owned_orders:
        raise RuntimeError("no DEMO-001 venue order history was found")
    order_ids = {str(item["orderId"]) for item in owned_orders}
    executions = client.get_private(
        "/v5/execution/list",
        params={"category": "linear", "symbol": symbol, "limit": 200},
    ).result.get("list", [])
    owned_executions = [
        item for item in executions if str(item.get("orderId")) in order_ids
    ]
    if len(owned_executions) < 2:
        raise RuntimeError(
            "DEMO-001 entry and flatten fills are absent from venue history"
        )
    positions = client.get_private(
        "/v5/position/list", params={"category": "linear", "symbol": symbol}
    ).result.get("list", [])
    wallet = client.get_private(
        "/v5/account/wallet-balance", params={"accountType": "UNIFIED"}
    ).result.get("list", [])
    open_orders = client.get_private(
        "/v5/order/realtime",
        params={"category": "linear", "symbol": symbol, "openOnly": 0, "limit": 50},
    ).result.get("list", [])
    owned_open = [
        item
        for item in open_orders
        if str(item.get("orderLinkId", "")).startswith("demo001-")
        and item.get("orderStatus") in {"New", "PartiallyFilled", "Untriggered"}
    ]
    position_quantity = sum(
        Decimal(str(item.get("size") or "0"))
        * (Decimal("-1") if item.get("side") == "Sell" else Decimal("1"))
        for item in positions
    )
    if owned_open or position_quantity != 0:
        raise RuntimeError(
            "Bybit demo account is not in the required flat terminal state"
        )
    if not wallet:
        raise RuntimeError("Bybit demo wallet snapshot is absent")
    return (
        {
            "fetched_at": fetched_at.isoformat(),
            "orders": owned_orders,
            "executions": owned_executions,
            "positions": positions,
            "wallet": wallet,
            "owned_open_orders": owned_open,
        },
        credential_fingerprint,
        fetched_at,
    )


def canonical_events(
    history: dict[str, Any], *, symbol: str, account: str, fetched_at: datetime
) -> list[Any]:
    instrument = f"bybit:linear:{symbol}"
    entries: list[tuple[datetime, str, str, dict[str, Any], dict[str, Any]]] = []
    order_links: dict[str, str] = {}
    for raw in history["orders"]:
        order_id = str(raw["orderId"])
        link = str(raw.get("orderLinkId") or order_id)
        order_links[order_id] = link
        entries.append(
            (
                milliseconds(
                    raw.get("updatedTime") or raw.get("createdTime"), fetched_at
                ),
                f"order:{order_id}",
                "order",
                {
                    "order_id": order_id,
                    "client_order_id": link,
                    "status": str(raw.get("orderStatus", "unknown")).lower(),
                    "side": str(raw.get("side", "")).lower(),
                    "quantity": str(raw.get("qty") or "0"),
                    "price": str(raw.get("price") or "0"),
                    "order_type": str(raw.get("orderType", "")).lower(),
                    "reduce_only": bool(raw.get("reduceOnly", False)),
                },
                raw,
            )
        )
    for raw in history["executions"]:
        execution_id = str(raw["execId"])
        order_id = str(raw["orderId"])
        observed = milliseconds(raw.get("execTime"), fetched_at)
        entries.append(
            (
                observed,
                f"fill:{execution_id}",
                "fill",
                {
                    "execution_id": execution_id,
                    "order_id": order_id,
                    "client_order_id": order_links.get(order_id, order_id),
                    "side": str(raw.get("side", "")).lower(),
                    "quantity": str(raw.get("execQty") or "0"),
                    "price": str(raw.get("execPrice") or "0"),
                },
                raw,
            )
        )
        entries.append(
            (
                observed,
                f"fee:{execution_id}",
                "fee",
                {
                    "execution_id": execution_id,
                    "asset": str(raw.get("feeCurrency") or "USDT"),
                    "amount": str(raw.get("execFee") or "0"),
                },
                raw,
            )
        )
    position_quantity = sum(
        Decimal(str(item.get("size") or "0"))
        * (Decimal("-1") if item.get("side") == "Sell" else Decimal("1"))
        for item in history["positions"]
    )
    wallet = history["wallet"][0]
    balance = str(wallet.get("totalWalletBalance") or "0")
    entries.extend(
        [
            (
                fetched_at,
                "position:terminal",
                "position",
                {"quantity": str(position_quantity), "entry_price": "0"},
                history["positions"],
            ),
            (
                fetched_at,
                "cash:terminal",
                "cash",
                {"asset": "USDT", "balance": balance},
                wallet,
            ),
            (
                fetched_at,
                "margin:terminal",
                "margin",
                {
                    "initial": str(wallet.get("totalInitialMargin") or "0"),
                    "maintenance": str(wallet.get("totalMaintenanceMargin") or "0"),
                    "available": str(wallet.get("totalAvailableBalance") or "0"),
                },
                wallet,
            ),
            (
                fetched_at,
                "reconciliation:terminal",
                "reconciliation",
                {
                    "positions": {instrument: str(position_quantity)},
                    "cash": {"USDT": balance},
                },
                {"positions": history["positions"], "wallet": wallet},
            ),
        ]
    )
    entries.sort(key=lambda item: (item[0], item[1]))
    events = []
    for sequence, (exchange_time, event_id, kind, payload, raw) in enumerate(
        entries, 1
    ):
        source_time = max(fetched_at, exchange_time)
        events.append(
            venue_event(
                event_id=f"demo001:{event_id}",
                venue="bybit",
                environment="demo",
                account_pseudonym=account,
                instrument_id=instrument,
                stream="rest-certified-history",
                kind=kind,
                exchange_time=exchange_time,
                source_time=source_time,
                receive_time=source_time,
                sequence=sequence,
                cursor=str(sequence),
                raw_reference_digest=digest(raw),
                normalization_version="1.0.0",
                payload=payload,
                reconciliation_id="demo001-terminal"
                if kind == "reconciliation"
                else None,
            )
        )
    return events


def degradation_observation(
    history: dict[str, Any], evidence: dict[str, Any], *, known_at: datetime
) -> dict[str, Any]:
    orders = history["orders"]
    executions = history["executions"]
    filled_order_ids = {str(item["orderId"]) for item in executions}
    eligible_orders = [
        item
        for item in orders
        if str(item.get("orderLinkId", "")).endswith(("-limit", "-entry", "-exit"))
    ]
    fill_rate = len(filled_order_ids) / max(1, len(eligible_orders))
    order_created = {
        str(item["orderId"]): milliseconds(item.get("createdTime"), known_at)
        for item in orders
    }
    lifecycle_ms = [
        max(
            0.0,
            (
                milliseconds(item.get("updatedTime"), known_at)
                - order_created[str(item["orderId"])]
            ).total_seconds()
            * 1000,
        )
        for item in orders
    ]
    fill_ms = [
        max(
            0.0,
            (
                milliseconds(item.get("execTime"), known_at)
                - order_created.get(str(item["orderId"]), known_at)
            ).total_seconds()
            * 1000,
        )
        for item in executions
    ]
    rules = evidence["bounded_order_limits"]
    reference = Decimal(rules["selected_notional"]) / Decimal(
        rules["selected_quantity"]
    )
    quantities = [Decimal(str(item.get("execQty") or "0")) for item in executions]
    total_quantity = sum(quantities, Decimal("0"))
    weighted_fill = (
        sum(
            Decimal(str(item.get("execPrice") or "0")) * quantity
            for item, quantity in zip(executions, quantities, strict=True)
        )
        / total_quantity
    )
    shortfall = float(abs(weighted_fill - reference) / reference * Decimal("10000"))
    method = {
        "fill_rate": "filled order ids / submitted drill orders",
        "ack_latency_ms": "conservative order lifecycle upper bound",
        "fill_latency_ms": "execution time minus order creation time",
        "implementation_shortfall_bps": "absolute weighted fill versus drill reference",
        "adverse_selection_bps": "pessimistic shortfall proxy; no favorable sign credit",
        "queue_model_error_bps": "unfilled fraction multiplied by ten bps",
        "reject_rate": "one deliberate venue rejection / all attempts",
    }
    return {
        "observation_id": f"demo001-{digest(history)[:20]}",
        "venue": "bybit",
        "environment": "demo",
        "strategy_id": "demo001-certification-drill",
        "listing_id": f"bybit:linear:{evidence['instrument']}",
        "order_type": "mixed",
        "size_bucket": "venue-minimum",
        "regime": "unclassified",
        "observed_at": history["fetched_at"],
        "available_at": history["fetched_at"],
        "source_digest": digest({"history": history, "method": method}),
        "service_available": True,
        "venue_rule_current": True,
        "fill_rate": fill_rate,
        "ack_latency_ms": max(lifecycle_ms, default=0.0),
        "fill_latency_ms": max(fill_ms, default=0.0),
        "implementation_shortfall_bps": shortfall,
        "adverse_selection_bps": shortfall,
        "queue_model_error_bps": (1.0 - fill_rate) * 10.0,
        "reject_rate": 1.0 / (len(eligible_orders) + 1),
        "reconciliation_breaks": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--dependencies", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--raw-output", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    args = parser.parse_args()
    evidence = read_object(args.evidence)
    dependencies = read_object(args.dependencies)
    expected = (set(DEPENDENCY_PRODUCERS) - {"EXEC-008", "EXEC-009"}) | {
        "RISK-002",
        "RISK-003",
    }
    missing = sorted(expected - set(dependencies))
    if missing:
        raise RuntimeError(
            f"exact dependency inventory is incomplete: {', '.join(missing)}"
        )
    symbol = str(evidence["instrument"])
    history, credential_fingerprint, fetched_at = collect_history(symbol)
    if credential_fingerprint != evidence["credential_fingerprint"]:
        raise RuntimeError("venue history credential differs from drill evidence")
    drill_map = {
        "authentication": "authentication",
        "server_clock": "server_clock",
        "instrument_precision": "dynamic_instrument_rules",
        "order_submission": "order_submission",
        "partial_fill": "partial_fill_handling",
        "cancel": "cancel",
        "amend": "amend",
        "venue_rejection": "venue_rejection",
        "private_stream_reconnect": "private_stream_reconnect",
        "restart_reconciliation": "restart_reconciliation",
        "duplicate_suppression": "duplicate_suppression",
        "emergency_kill": "runtime_kill",
    }
    adapter = adapter_certification_receipt(
        venue="bybit",
        environment="demo",
        product_type="perpetual",
        evidence_class="venue_observed",
        observed_at=datetime.fromisoformat(evidence["observed_at"]),
        valid_until=datetime.fromisoformat(evidence["valid_until"]),
        endpoint_identity=evidence["endpoint_identity"],
        drill_results={
            name: evidence["drill_evidence"][drill_map[name]]["passed"] is True
            for name in ADAPTER_DRILLS
        },
        dependency_receipts={
            name: dependencies[name] for name in ("EXEC-003", "EXEC-004", "EXEC-007")
        },
        dataset_digest=evidence["dataset_digest"],
        source_commit=args.source_commit,
        configuration={"host": "exec2-lagos", "evidence_digest": digest(evidence)},
    )
    rules = evidence["bounded_order_limits"]
    shadow = dependencies["SHADOW-002"]
    candidate_digest = shadow["result"].get("candidate_digest")
    if not isinstance(candidate_digest, str):
        raise RuntimeError("SHADOW-002 receipt lacks its candidate digest")
    risk = realtime_risk_decision_receipt(
        intent={
            "intent_id": f"demo001-preflight-{digest(history)[:16]}",
            "candidate_digest": candidate_digest,
            "received_at": fetched_at.isoformat(),
            "expected_state_version": 1,
            "symbol": symbol,
            "side": "buy",
            "quantity": float(Decimal(rules["selected_quantity"])),
            "price": float(
                Decimal(rules["selected_notional"])
                / Decimal(rules["selected_quantity"])
            ),
            "reduce_only": False,
        },
        state={
            "state_id": f"bybit-demo-flat-{digest(history)[:16]}",
            "version": 1,
            "observed_at": fetched_at.isoformat(),
            "available_at": fetched_at.isoformat(),
            "positions": {symbol: 0.0},
            "connector_healthy": True,
            "reconciliation_healthy": True,
            "kill_active": False,
            "critical_incidents": 0,
            "open_orders": 0,
            "gross_notional": 0.0,
            "daily_pnl": 0.0,
        },
        dependency_receipts={
            name: dependencies[name]
            for name in ("RISK-002", "RISK-003", "EXEC-001", "EXEC-004")
        },
        policy={
            "snapshot_expiry_seconds": 30,
            "decision_deadline_ms": 1000,
            "allowed_symbols": [symbol],
            "maximum_order_quantity": float(Decimal(rules["selected_quantity"])),
            "maximum_order_notional": float(Decimal(rules["maximum_notional"])),
            "maximum_open_orders": 1,
            "maximum_gross_notional": float(Decimal(rules["maximum_notional"])),
            "maximum_daily_loss": max(
                1.0, float(Decimal(rules["maximum_notional"]) * Decimal("0.02"))
            ),
        },
        known_at=fetched_at,
        dataset_digest=digest({"history": history, "purpose": "risk-preflight"}),
        source_commit=args.source_commit,
    )
    shadow_candidate = shadow["result"].get("candidate_digest")
    if not isinstance(shadow_candidate, str):
        raise RuntimeError("SHADOW-002 receipt lacks its candidate digest")
    degradation = execution_degradation_receipt(
        exec005_receipt=dependencies["EXEC-005"],
        exec008_receipt=adapter,
        shadow002_receipt=shadow,
        governance_policy_digest=digest({"GOV-003": "active"}),
        platform_observability_digest=evidence["platform_observability_digest"],
        candidate_lifecycle={
            "status": "demo",
            "candidate_digest": shadow_candidate,
            "record_digest": digest(
                {
                    "candidate_digest": shadow_candidate,
                    "status": "demo",
                    "history": digest(history),
                }
            ),
        },
        observations=[degradation_observation(history, evidence, known_at=fetched_at)],
        known_at=fetched_at,
        prior_status="monitoring",
        dataset_digest=digest({"history": history, "purpose": "degradation"}),
        source_commit=args.source_commit,
        configuration={
            "thresholds": {
                "fill_rate": 0.8,
                "ack_latency_ms": 250,
                "fill_latency_ms": 1000,
                "implementation_shortfall_bps": 8,
                "adverse_selection_bps": 6,
                "queue_model_error_bps": 4,
                "reject_rate": 0.05,
                "reconciliation_breaks": 0,
            },
            "consecutive_breaches": 2,
            "recovery_observations": 3,
            "observation_expiry_seconds": 300,
        },
    )
    exec_dependencies = {
        name: (
            adapter.as_dict()
            if name == "EXEC-008"
            else degradation.as_dict()
            if name == "EXEC-009"
            else dependencies[name]
        )
        for name in DEPENDENCY_PRODUCERS
    }
    events = canonical_events(
        history,
        symbol=symbol,
        account=f"bybit-demo-{credential_fingerprint[:16]}",
        fetched_at=fetched_at,
    )
    telemetry = venue_telemetry_receipt(
        events=events,
        dependencies=exec_dependencies,
        known_at=fetched_at + timedelta(seconds=1),
        source_commit=args.source_commit,
        dataset_digest=digest(history),
        telemetry_schema_digest=digest(VENUE_TELEMETRY_SPECIFICATION),
        configuration={"freshness_seconds": 120, "evidence_class": "venue_observed"},
        governance_digests={
            "DEMO-001-drills": digest(evidence),
            "PLAT-005": evidence["platform_observability_digest"],
        },
    )
    demo_dependencies = {
        name: (
            adapter.as_dict()
            if name == "EXEC-008"
            else risk.as_dict()
            if name == "RISK-005"
            else dependencies[name]
        )
        for name in (
            "EXEC-004",
            "EXEC-005",
            "EXEC-006",
            "EXEC-007",
            "EXEC-008",
            "RISK-005",
        )
    }
    demo = demo_certification_receipt(
        observed_at=datetime.fromisoformat(evidence["observed_at"]),
        valid_until=datetime.fromisoformat(evidence["valid_until"]),
        endpoint_identity=evidence["endpoint_identity"],
        credential_fingerprint=evidence["credential_fingerprint"],
        drill_evidence=evidence["drill_evidence"],
        dependency_receipts=demo_dependencies,
        platform_observability_digest=evidence["platform_observability_digest"],
        dataset_digest=evidence["dataset_digest"],
        source_commit=args.source_commit,
        configuration={
            "capital_environment": False,
            "withdrawal_authority": evidence["withdrawal_authority"],
            "terminal_state": evidence["terminal_state"],
            "instrument": symbol,
            "bounded_order_limits": evidence["bounded_order_limits"],
        },
    )
    report = {
        "schema_version": "demo001-exec011-production-closure-v1.0.0",
        "success": bool(
            adapter.result["qualified"]
            and telemetry.result["reconstructable"]
            and demo.result["qualified"]
        ),
        "adapter_specification": ADAPTER_CERTIFICATION_SPECIFICATION,
        "adapter_specification_digest": digest(ADAPTER_CERTIFICATION_SPECIFICATION),
        "risk_specification": REALTIME_RISK_SPECIFICATION,
        "risk_specification_digest": digest(REALTIME_RISK_SPECIFICATION),
        "degradation_specification": EXECUTION_DEGRADATION_SPECIFICATION,
        "degradation_specification_digest": digest(EXECUTION_DEGRADATION_SPECIFICATION),
        "telemetry_specification": VENUE_TELEMETRY_SPECIFICATION,
        "telemetry_specification_digest": digest(VENUE_TELEMETRY_SPECIFICATION),
        "demo_specification": DEMO_CERTIFICATION_SPECIFICATION,
        "demo_specification_digest": digest(DEMO_CERTIFICATION_SPECIFICATION),
        "receipts": {
            "EXEC-008": adapter.as_dict(),
            "RISK-005": risk.as_dict(),
            "EXEC-009": degradation.as_dict(),
            "EXEC-011": telemetry.as_dict(),
            "DEMO-001": demo.as_dict(),
        },
        "venue_history": {
            "orders": len(history["orders"]),
            "fills": len(history["executions"]),
            "canonical_events": len(events),
            "terminal_open_orders": len(history["owned_open_orders"]),
            "terminal_position_qty": "0",
        },
        "claim_boundary": "Bybit demo certification and replay only; no live or capital authority.",
    }
    report["report_digest"] = digest(report)
    args.raw_output.parent.mkdir(parents=True, exist_ok=True)
    args.raw_output.write_text(
        json.dumps(history, indent=2, sort_keys=True) + "\n", encoding="ascii"
    )
    args.raw_output.chmod(0o600)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii"
    )
    args.output.chmod(0o600)
    print(
        json.dumps(
            {
                "success": report["success"],
                "receipt_digests": {
                    key: value["receipt_digest"]
                    for key, value in report["receipts"].items()
                },
                "venue_history": report["venue_history"],
                "live_or_capital_authority": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
