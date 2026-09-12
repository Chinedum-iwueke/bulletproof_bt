"""EXEC-011 canonical venue telemetry, deterministic replay, and publication receipt."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .receipt import ProducerReceipt, build_receipt, digest, is_sha256, verify_receipt

VENUE_TELEMETRY_SCHEMA_VERSION = "exec011-venue-telemetry-v1.0.0"
VENUE_TELEMETRY_PRODUCER = "bt.institutional.venue_telemetry.venue_telemetry_receipt"
EVENT_KINDS = {
    "decision",
    "intent",
    "order",
    "fill",
    "position",
    "cash",
    "fee",
    "funding",
    "margin",
    "incident",
    "reconciliation",
}
ENVIRONMENTS = {"shadow", "demo", "live"}
DEPENDENCY_PRODUCERS = {
    "EXEC-001": "bt.institutional.execution.execution_journal_receipt",
    "EXEC-002": "bt.institutional.microstructure.microstructure_state_receipt",
    "EXEC-003": "bt.institutional.venue.venue_identity_receipt",
    "EXEC-004": "bt.institutional.oms.oms_reconciliation_receipt",
    "EXEC-005": "bt.institutional.execution_calibration.execution_calibration_receipt",
    "EXEC-006": "bt.institutional.execution_scheduler.execution_schedule_receipt",
    "EXEC-007": "bt.institutional.runtime_safety.runtime_safety_receipt",
    "EXEC-008": "bt.institutional.adapter_certification.adapter_certification_receipt",
    "EXEC-009": "bt.institutional.execution_degradation.execution_degradation_receipt",
    "SHADOW-002": "bt.institutional.shadow_monitoring.shadow_monitoring_receipt",
}
SECRET_KEYS = {
    "api_key",
    "apikey",
    "secret",
    "token",
    "password",
    "private_key",
    "signature",
}

VENUE_TELEMETRY_SPECIFICATION = {
    "schema_version": VENUE_TELEMETRY_SCHEMA_VERSION,
    "dependencies": [
        *DEPENDENCY_PRODUCERS,
        "SHADOW-001",
        "BT-003",
        "BT-005",
        "BT-008",
        "PLAT-005",
        "PLAT-007",
        "UI-001",
        "UI-007",
    ],
    "venues": ["binance", "bybit"],
    "environments": sorted(ENVIRONMENTS),
    "event_kinds": sorted(EVENT_KINDS),
    "required_identity": ["venue", "environment", "account_pseudonym", "instrument_id"],
    "required_clocks": ["exchange_time", "source_time", "receive_time"],
    "raw_data_policy": "digest_reference_only",
    "replay": "point_in_time_deterministic_correction_aware",
    "secrets": "forbidden",
    "authority": {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    },
}


class VenueTelemetryError(ValueError):
    """Venue telemetry violates identity, causality, secrecy, or replay invariants."""


def _utc(value: str | datetime, field: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise VenueTelemetryError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise VenueTelemetryError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _decimal(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise VenueTelemetryError(f"{field} must be decimal-compatible") from exc
    if not result.is_finite():
        raise VenueTelemetryError(f"{field} must be finite")
    return result


def _assert_no_secrets(value: Any, path: str = "payload") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in SECRET_KEYS or any(
                part in normalized for part in ("credential", "passphrase")
            ):
                raise VenueTelemetryError(
                    f"secret-bearing field is forbidden at {path}.{key}"
                )
            _assert_no_secrets(nested, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _assert_no_secrets(nested, f"{path}[{index}]")


@dataclass(frozen=True)
class VenueTelemetryEvent:
    schema_version: str
    event_id: str
    venue: str
    environment: str
    account_pseudonym: str
    instrument_id: str
    stream: str
    kind: str
    exchange_time: str
    source_time: str
    receive_time: str
    sequence: int
    cursor: str
    raw_reference_digest: str
    normalization_version: str
    correction_of: str | None
    reconciliation_id: str | None
    payload: dict[str, Any]
    payload_digest: str
    event_digest: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def venue_event(
    *,
    event_id: str,
    venue: str,
    environment: str,
    account_pseudonym: str,
    instrument_id: str,
    stream: str,
    kind: str,
    exchange_time: str | datetime,
    source_time: str | datetime,
    receive_time: str | datetime,
    sequence: int,
    cursor: str,
    raw_reference_digest: str,
    normalization_version: str,
    payload: dict[str, Any],
    correction_of: str | None = None,
    reconciliation_id: str | None = None,
    maximum_future_drift_seconds: float = 5.0,
) -> VenueTelemetryEvent:
    venue_name = venue.lower()
    if venue_name not in {"binance", "bybit"}:
        raise VenueTelemetryError("venue must be binance or bybit")
    if environment not in ENVIRONMENTS:
        raise VenueTelemetryError("environment must be shadow, demo, or live")
    if kind not in EVENT_KINDS:
        raise VenueTelemetryError("event kind is not canonical")
    fields = (
        event_id,
        account_pseudonym,
        instrument_id,
        stream,
        cursor,
        normalization_version,
    )
    if not all(isinstance(value, str) and value.strip() for value in fields):
        raise VenueTelemetryError("identity and provenance fields must be non-empty")
    if len(account_pseudonym) > 120 or any(
        marker in account_pseudonym.lower() for marker in ("@", "key", "secret")
    ):
        raise VenueTelemetryError(
            "account_pseudonym is not a safe pseudonymous identifier"
        )
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        raise VenueTelemetryError("sequence must be a non-negative integer")
    if not is_sha256(raw_reference_digest):
        raise VenueTelemetryError("raw_reference_digest must be lowercase sha256")
    _assert_no_secrets(payload)
    exchange = _utc(exchange_time, "exchange_time")
    source = _utc(source_time, "source_time")
    received = _utc(receive_time, "receive_time")
    if source < exchange:
        raise VenueTelemetryError("source_time precedes exchange_time")
    if received < source:
        raise VenueTelemetryError("receive_time precedes source_time")
    if (exchange - received).total_seconds() > maximum_future_drift_seconds:
        raise VenueTelemetryError("exchange_time exceeds receive clock drift limit")
    payload_hash = digest(payload)
    core = {
        "schema_version": VENUE_TELEMETRY_SCHEMA_VERSION,
        "event_id": event_id,
        "venue": venue_name,
        "environment": environment,
        "account_pseudonym": account_pseudonym,
        "instrument_id": instrument_id,
        "stream": stream,
        "kind": kind,
        "exchange_time": exchange.isoformat(),
        "source_time": source.isoformat(),
        "receive_time": received.isoformat(),
        "sequence": sequence,
        "cursor": cursor,
        "raw_reference_digest": raw_reference_digest,
        "normalization_version": normalization_version,
        "correction_of": correction_of,
        "reconciliation_id": reconciliation_id,
        "payload": payload,
        "payload_digest": payload_hash,
    }
    return VenueTelemetryEvent(**core, event_digest=digest(core))


def verify_venue_event(event: VenueTelemetryEvent | dict[str, Any]) -> bool:
    document = (
        event.as_dict() if isinstance(event, VenueTelemetryEvent) else dict(event)
    )
    event_hash = document.pop("event_digest", None)
    if document.get("schema_version") != VENUE_TELEMETRY_SCHEMA_VERSION:
        return False
    if document.get("payload_digest") != digest(document.get("payload")):
        return False
    try:
        _assert_no_secrets(document.get("payload"))
    except VenueTelemetryError:
        return False
    return event_hash == digest(document)


def replay_venue_telemetry(
    events: Iterable[VenueTelemetryEvent | dict[str, Any]],
    *,
    known_at: str | datetime,
    freshness_seconds: int = 120,
) -> dict[str, Any]:
    """Reduce immutable events to one deterministic, correction-aware venue view."""
    known = _utc(known_at, "known_at")
    documents: list[dict[str, Any]] = []
    identities: dict[tuple[str, str, str, str], str] = {}
    duplicate_count = 0
    for raw in events:
        item = raw.as_dict() if isinstance(raw, VenueTelemetryEvent) else dict(raw)
        if not verify_venue_event(item):
            raise VenueTelemetryError("event digest does not match content")
        if _utc(item["receive_time"], "receive_time") > known:
            continue
        identity = (
            item["venue"],
            item["environment"],
            item["stream"],
            item["event_id"],
        )
        prior = identities.get(identity)
        if prior:
            if prior != item["event_digest"]:
                raise VenueTelemetryError(
                    "event identity was reused with different content"
                )
            duplicate_count += 1
            continue
        identities[identity] = item["event_digest"]
        documents.append(item)
    documents.sort(
        key=lambda item: (
            item["receive_time"],
            item["venue"],
            item["stream"],
            item["sequence"],
            item["event_digest"],
        )
    )
    by_id = {item["event_id"]: item for item in documents}
    superseded: set[str] = set()
    for item in documents:
        target = item.get("correction_of")
        if target:
            if target not in by_id:
                raise VenueTelemetryError("correction target is absent from replay")
            if by_id[target]["stream"] != item["stream"]:
                raise VenueTelemetryError("correction target belongs to another stream")
            superseded.add(target)
    active = [item for item in documents if item["event_id"] not in superseded]
    sequence_gaps: list[dict[str, Any]] = []
    streams: dict[tuple[str, str, str], set[int]] = {}
    for item in documents:
        streams.setdefault(
            (item["venue"], item["environment"], item["stream"]), set()
        ).add(item["sequence"])
    for (venue, environment, stream), values in sorted(streams.items()):
        ordered = sorted(values)
        for left, right in zip(ordered, ordered[1:]):
            if right > left + 1:
                sequence_gaps.append(
                    {
                        "venue": venue,
                        "environment": environment,
                        "stream": stream,
                        "after": left,
                        "before": right,
                    }
                )

    orders: dict[str, dict[str, Any]] = {}
    fills: dict[str, dict[str, Any]] = {}
    positions: dict[str, dict[str, Any]] = {}
    cash: dict[str, str] = {}
    margins: dict[str, dict[str, Any]] = {}
    incidents: list[dict[str, Any]] = []
    reconciliation: dict[str, Any] | None = None
    fee_total = Decimal("0")
    funding_total = Decimal("0")
    for item in active:
        payload = item["payload"]
        if item["kind"] == "order":
            orders[str(payload["client_order_id"])] = payload
        elif item["kind"] == "fill":
            fills[str(payload["execution_id"])] = payload
        elif item["kind"] == "position":
            positions[item["instrument_id"]] = payload
        elif item["kind"] == "cash":
            cash[str(payload["asset"])] = str(
                _decimal(payload["balance"], "cash.balance")
            )
        elif item["kind"] == "margin":
            margins[item["instrument_id"]] = payload
        elif item["kind"] == "fee":
            fee_total += _decimal(payload["amount"], "fee.amount")
        elif item["kind"] == "funding":
            funding_total += _decimal(payload["amount"], "funding.amount")
        elif item["kind"] == "incident":
            incidents.append(payload)
        elif item["kind"] == "reconciliation":
            reconciliation = payload

    episodes: list[dict[str, Any]] = []
    inventory: dict[str, dict[str, Decimal | str]] = {}
    for item in active:
        if item["kind"] != "fill":
            continue
        fill = item["payload"]
        instrument = item["instrument_id"]
        qty = _decimal(fill["quantity"], "fill.quantity")
        signed = qty if fill["side"] == "buy" else -qty
        price = _decimal(fill["price"], "fill.price")
        current = inventory.setdefault(
            instrument,
            {
                "quantity": Decimal("0"),
                "cost": Decimal("0"),
                "opened_at": item["exchange_time"],
            },
        )
        old_qty = current["quantity"]
        assert isinstance(old_qty, Decimal)
        if old_qty == 0 or old_qty * signed > 0:
            current["cost"] = current["cost"] + signed * price
            current["quantity"] = old_qty + signed
        else:
            close_qty = min(abs(old_qty), abs(signed))
            average = abs(current["cost"] / old_qty)
            pnl = (
                close_qty
                * (price - average)
                * (Decimal("1") if old_qty > 0 else Decimal("-1"))
            )
            remaining = old_qty + signed
            episodes.append(
                {
                    "instrument_id": instrument,
                    "opened_at": current["opened_at"],
                    "closed_at": item["exchange_time"],
                    "side": "long" if old_qty > 0 else "short",
                    "quantity": str(close_qty),
                    "entry_price": str(average),
                    "exit_price": str(price),
                    "gross_pnl": str(pnl),
                    "status": "closed" if remaining == 0 else "partial",
                }
            )
            current["quantity"] = remaining
            current["cost"] = Decimal("0") if remaining == 0 else remaining * price
            if old_qty * remaining < 0:
                current["opened_at"] = item["exchange_time"]

    discrepancies: list[str] = []
    if reconciliation:
        for instrument, expected in reconciliation.get("positions", {}).items():
            actual = positions.get(instrument, {}).get("quantity")
            if actual is None or _decimal(actual, "position.quantity") != _decimal(
                expected, "reconciliation.position"
            ):
                discrepancies.append(f"position:{instrument}")
        for asset, expected in reconciliation.get("cash", {}).items():
            if asset not in cash or _decimal(cash[asset], "cash.balance") != _decimal(
                expected, "reconciliation.cash"
            ):
                discrepancies.append(f"cash:{asset}")
    latest = max(
        (_utc(item["receive_time"], "receive_time") for item in active), default=None
    )
    stale = latest is None or (known - latest).total_seconds() > freshness_seconds
    status = (
        "degraded"
        if sequence_gaps or discrepancies or incidents
        else "stale"
        if stale
        else "current"
    )
    projection = {
        "schema_version": VENUE_TELEMETRY_SCHEMA_VERSION,
        "known_at": known.isoformat(),
        "status": status,
        "stale": stale,
        "event_count": len(documents),
        "active_event_count": len(active),
        "duplicate_event_count": duplicate_count,
        "sequence_gaps": sequence_gaps,
        "reconciliation_discrepancies": discrepancies,
        "orders": [orders[key] for key in sorted(orders)],
        "fills": [fills[key] for key in sorted(fills)],
        "positions": [
            {"instrument_id": key, **positions[key]} for key in sorted(positions)
        ],
        "cash": [{"asset": key, "balance": cash[key]} for key in sorted(cash)],
        "margins": [{"instrument_id": key, **margins[key]} for key in sorted(margins)],
        "fees": str(fee_total),
        "funding": str(funding_total),
        "incidents": incidents,
        "trade_episodes": episodes,
        "event_head_digest": digest([item["event_digest"] for item in documents]),
    }
    projection["projection_digest"] = digest(projection)
    return projection


def venue_telemetry_receipt(
    *,
    events: Iterable[VenueTelemetryEvent],
    dependencies: dict[str, ProducerReceipt | dict[str, Any]],
    known_at: str | datetime,
    source_commit: str,
    dataset_digest: str,
    telemetry_schema_digest: str,
    configuration: dict[str, Any],
    governance_digests: dict[str, str],
) -> ProducerReceipt:
    if set(dependencies) != set(DEPENDENCY_PRODUCERS):
        raise VenueTelemetryError(
            "EXEC-011 requires every exact quantitative dependency"
        )
    verified: dict[str, dict[str, Any]] = {}
    for milestone, value in dependencies.items():
        item = value.as_dict() if isinstance(value, ProducerReceipt) else dict(value)
        if (
            not verify_receipt(item)
            or item.get("milestone") != milestone
            or item.get("producer") != DEPENDENCY_PRODUCERS[milestone]
        ):
            raise VenueTelemetryError(f"EXEC-011 requires an exact {milestone} receipt")
        verified[milestone] = item
    if not is_sha256(telemetry_schema_digest) or any(
        not is_sha256(value) for value in governance_digests.values()
    ):
        raise VenueTelemetryError(
            "schema and governance references must be sha256 digests"
        )
    event_list = list(events)
    if len({event.normalization_version for event in event_list}) != 1:
        raise VenueTelemetryError("one receipt cannot mix normalization versions")
    replay = replay_venue_telemetry(
        event_list,
        known_at=known_at,
        freshness_seconds=int(configuration.get("freshness_seconds", 120)),
    )
    identities = {
        (event.venue, event.environment, event.account_pseudonym)
        for event in event_list
    }
    if len(identities) != 1:
        raise VenueTelemetryError(
            "one receipt must bind exactly one venue environment and account"
        )
    venue, environment, account = identities.pop()
    result = {
        "schema_version": VENUE_TELEMETRY_SCHEMA_VERSION,
        "telemetry_schema_digest": telemetry_schema_digest,
        "venue": venue,
        "environment": environment,
        "account_pseudonym": account,
        "projection": replay,
        "projection_digest": replay["projection_digest"],
        "dependency_receipt_digests": {
            key: verified[key]["receipt_digest"] for key in sorted(verified)
        },
        "governance_digests": dict(sorted(governance_digests.items())),
        "reconstructable": not replay["sequence_gaps"]
        and not replay["reconciliation_discrepancies"],
        "claim": "canonical venue telemetry and deterministic replay only; no capital, allocation, promotion, or order authority",
    }
    return build_receipt(
        milestone="EXEC-011",
        producer=VENUE_TELEMETRY_PRODUCER,
        producer_version="1.0.0",
        source_commit=source_commit,
        dataset_digest=dataset_digest,
        inputs={
            "events": [event.as_dict() for event in event_list],
            "dependencies": result["dependency_receipt_digests"],
        },
        configuration=configuration,
        artifacts={
            "event_head_digest": replay["event_head_digest"],
            "projection_digest": replay["projection_digest"],
        },
        result=result,
    )
