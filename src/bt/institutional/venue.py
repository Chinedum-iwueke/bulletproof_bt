"""Effective-dated cross-venue identity and routing-constraint evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

MAPPING_SCHEMA_VERSION = "exec003-venue-identity-v1.0.0"
RELATIONSHIP_KINDS = {"economic_equivalent", "hedge_substitute", "reference_only"}
VENUE_STATES = {"operational", "degraded", "outage"}


class VenueIdentityError(ValueError):
    """Venue identity evidence is ambiguous, stale, or internally inconsistent."""


def _time(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise VenueIdentityError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise VenueIdentityError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise VenueIdentityError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _decimal(value: Any, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise VenueIdentityError(f"{field} must be numeric") from exc
    if not number.is_finite() or number <= 0:
        raise VenueIdentityError(f"{field} must be positive and finite")
    return number


def _active_at(record: dict[str, Any], as_of: datetime, known_at: datetime) -> bool:
    available_at = _time(str(record["available_at"]), "available_at")
    effective_from = _time(str(record["effective_from"]), "effective_from")
    effective_to = (
        _time(str(record["effective_to"]), "effective_to")
        if record.get("effective_to")
        else None
    )
    if effective_to is not None and effective_to <= effective_from:
        raise VenueIdentityError("effective_to must be later than effective_from")
    return (
        available_at <= known_at
        and effective_from <= as_of
        and (effective_to is None or as_of < effective_to)
        and record.get("status", "active") == "active"
    )


def _identity(record: dict[str, Any]) -> dict[str, Any]:
    required = (
        "venue_id",
        "listing_id",
        "instrument_type",
        "base_asset",
        "quote_asset",
        "settlement_asset",
        "margin_asset",
        "contract_size",
        "price_tick",
        "quantity_step",
        "minimum_quantity",
        "minimum_notional",
    )
    missing = [field for field in required if record.get(field) in (None, "")]
    if missing:
        raise VenueIdentityError(
            f"listing {record.get('venue_id')}:{record.get('listing_id')} "
            f"is missing {', '.join(missing)}"
        )
    for field in (
        "contract_size",
        "price_tick",
        "quantity_step",
        "minimum_quantity",
        "minimum_notional",
    ):
        _decimal(record[field], field)
    if record.get("maximum_quantity") not in (None, ""):
        maximum = _decimal(record["maximum_quantity"], "maximum_quantity")
        minimum = _decimal(record["minimum_quantity"], "minimum_quantity")
        if maximum < minimum:
            raise VenueIdentityError(
                "maximum_quantity must be greater than or equal to minimum_quantity"
            )
    return {
        field: record.get(field)
        for field in (
            "venue_id",
            "listing_id",
            "canonical_instrument_id",
            "symbol",
            "instrument_type",
            "base_asset",
            "quote_asset",
            "settlement_asset",
            "margin_asset",
            "contract_size",
            "inverse",
            "expiry",
            "price_tick",
            "quantity_step",
            "minimum_quantity",
            "maximum_quantity",
            "minimum_notional",
        )
    }


def _compatibility(members: list[dict[str, Any]], kind: str) -> list[str]:
    if len(members) < 2:
        return ["relationship_requires_multiple_members"]
    fields = ["base_asset", "instrument_type"]
    if kind == "economic_equivalent":
        fields.extend(
            ["quote_asset", "settlement_asset", "margin_asset", "inverse", "expiry"]
        )
    blockers = []
    for field in fields:
        if len({str(item.get(field)) for item in members}) != 1:
            blockers.append(f"incompatible_{field}")
    return blockers


def venue_identity_map(
    *,
    data001_receipt: dict[str, Any],
    relationships: Iterable[dict[str, Any]],
    venue_observations: Iterable[dict[str, Any]],
    as_of: datetime,
    known_at: datetime,
    maximum_observation_age_seconds: int = 60,
) -> dict[str, Any]:
    """Build an explicit comparison map without inferring identity from symbols."""

    if not verify_receipt(data001_receipt) or data001_receipt["milestone"] != "DATA-001":
        raise VenueIdentityError("DATA-001 receipt is not admissible")
    as_of_utc = _utc(as_of, "as_of")
    known_at_utc = _utc(known_at, "known_at")
    if as_of_utc > known_at_utc:
        raise VenueIdentityError("as_of cannot be later than known_at")
    if maximum_observation_age_seconds <= 0:
        raise VenueIdentityError("maximum observation age must be positive")

    active: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in data001_receipt["result"].get("records", []):
        if not _active_at(raw, as_of_utc, known_at_utc):
            continue
        item = _identity(raw)
        key = (str(item["venue_id"]), str(item["listing_id"]))
        if key in active:
            raise VenueIdentityError("DATA-001 contains an ambiguous active listing")
        active[key] = item

    venue_state: dict[str, dict[str, Any]] = {}
    for raw in venue_observations:
        venue_id = str(raw.get("venue_id", ""))
        status = str(raw.get("status", ""))
        if not venue_id or status not in VENUE_STATES:
            raise VenueIdentityError("venue observation has an invalid identity or status")
        observed_at = _time(str(raw.get("observed_at")), "observed_at")
        available_at = _time(str(raw.get("available_at")), "available_at")
        if observed_at > available_at or available_at > known_at_utc:
            raise VenueIdentityError("venue observation violates point-in-time availability")
        if (known_at_utc - available_at).total_seconds() > maximum_observation_age_seconds:
            status = "outage"
            reason = "stale_venue_observation"
        else:
            reason = raw.get("reason")
        if venue_id in venue_state:
            raise VenueIdentityError("venue observation is ambiguous")
        venue_state[venue_id] = {
            "status": status,
            "observed_at": observed_at.isoformat(),
            "available_at": available_at.isoformat(),
            "reason": reason,
        }

    mappings = []
    routes = []
    member_owners: dict[tuple[str, str], str] = {}
    seen_mapping_ids: set[str] = set()
    for raw in relationships:
        mapping_id = str(raw.get("mapping_id", ""))
        kind = str(raw.get("relationship_kind", ""))
        if not mapping_id or mapping_id in seen_mapping_ids:
            raise VenueIdentityError("mapping identity is empty or duplicated")
        if kind not in RELATIONSHIP_KINDS:
            raise VenueIdentityError("relationship kind is not supported")
        seen_mapping_ids.add(mapping_id)
        relationship_available = _time(str(raw.get("available_at")), "available_at")
        relationship_from = _time(str(raw.get("effective_from")), "effective_from")
        relationship_to = (
            _time(str(raw["effective_to"]), "effective_to")
            if raw.get("effective_to")
            else None
        )
        if relationship_to is not None and relationship_to <= relationship_from:
            raise VenueIdentityError("effective_to must be later than effective_from")
        relationship_active = (
            relationship_available <= known_at_utc
            and relationship_from <= as_of_utc
            and (relationship_to is None or as_of_utc < relationship_to)
        )
        keys = [
            (str(item.get("venue_id", "")), str(item.get("listing_id", "")))
            for item in raw.get("members", [])
        ]
        if len(keys) != len(set(keys)):
            raise VenueIdentityError("relationship contains duplicate members")
        members = [active[key] for key in keys if key in active]
        blockers = []
        if not relationship_active:
            blockers.append("relationship_inactive")
        if len(members) != len(keys):
            blockers.append("member_inactive_or_unknown")
        blockers.extend(_compatibility(members, kind))
        state = "active" if not blockers else "quarantined"
        if state == "active":
            for key in keys:
                owner = member_owners.get(key)
                if owner and owner != mapping_id:
                    raise VenueIdentityError(
                        "active listing belongs to ambiguous relationships"
                    )
                member_owners[key] = mapping_id
        mappings.append(
            {
                "mapping_id": mapping_id,
                "relationship_kind": kind,
                "state": state,
                "members": [f"{venue}:{listing}" for venue, listing in keys],
                "blockers": sorted(set(blockers)),
                "effective_from": relationship_from.isoformat(),
                "effective_to": relationship_to.isoformat() if relationship_to else None,
                "available_at": relationship_available.isoformat(),
            }
        )
        if state != "active" or kind == "reference_only":
            continue
        for source in members:
            for target in members:
                if source is target:
                    continue
                source_venue = str(source["venue_id"])
                target_venue = str(target["venue_id"])
                route_blockers = []
                for venue in (source_venue, target_venue):
                    observation = venue_state.get(venue)
                    if observation is None:
                        route_blockers.append(f"{venue}:venue_state_unknown")
                    elif observation["status"] == "outage":
                        route_blockers.append(f"{venue}:venue_outage")
                conversion = _decimal(source["contract_size"], "contract_size") / _decimal(
                    target["contract_size"], "contract_size"
                )
                routes.append(
                    {
                        "mapping_id": mapping_id,
                        "source": f"{source_venue}:{source['listing_id']}",
                        "target": f"{target_venue}:{target['listing_id']}",
                        "comparison_eligible": not route_blockers,
                        "order_route_eligible": False,
                        "blockers": sorted(route_blockers),
                        "contract_conversion_ratio": str(conversion.normalize()),
                        "target_constraints": {
                            key: target[key]
                            for key in (
                                "price_tick",
                                "quantity_step",
                                "minimum_quantity",
                                "maximum_quantity",
                                "minimum_notional",
                            )
                        },
                    }
                )

    mapped_keys = set(member_owners)
    result = {
        "schema_version": MAPPING_SCHEMA_VERSION,
        "as_of": as_of_utc.isoformat(),
        "known_at": known_at_utc.isoformat(),
        "active_identities": [active[key] for key in sorted(active)],
        "mappings": sorted(mappings, key=lambda item: item["mapping_id"]),
        "routes": sorted(routes, key=lambda item: (item["mapping_id"], item["source"], item["target"])),
        "unmapped_identities": [f"{key[0]}:{key[1]}" for key in sorted(set(active) - mapped_keys)],
        "venue_observations": {key: venue_state[key] for key in sorted(venue_state)},
        "qualified": bool(mappings) and all(item["state"] == "active" for item in mappings),
        "claim": (
            "explicit economic-comparison identity only; venues remain operationally "
            "distinct and no order, routing, allocation, promotion, or capital authority exists"
        ),
    }
    result["mapping_digest"] = digest(result)
    return result


def venue_identity_receipt(
    *,
    data001_receipt: dict[str, Any],
    exec001_receipt: dict[str, Any],
    exec002_receipt: dict[str, Any],
    relationships: Iterable[dict[str, Any]],
    venue_observations: Iterable[dict[str, Any]],
    as_of: datetime,
    known_at: datetime,
    source_commit: str,
    dataset_digest: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    for receipt, milestone in (
        (data001_receipt, "DATA-001"),
        (exec001_receipt, "EXEC-001"),
        (exec002_receipt, "EXEC-002"),
    ):
        if not verify_receipt(receipt) or receipt["milestone"] != milestone:
            raise VenueIdentityError(f"{milestone} receipt is not admissible")
    relationship_list = list(relationships)
    observation_list = list(venue_observations)
    result = venue_identity_map(
        data001_receipt=data001_receipt,
        relationships=relationship_list,
        venue_observations=observation_list,
        as_of=as_of,
        known_at=known_at,
        maximum_observation_age_seconds=int(
            configuration.get("maximum_observation_age_seconds", 60)
        ),
    )
    result["mapping_schema_digest"] = digest({"schema_version": MAPPING_SCHEMA_VERSION})
    result["dependency_receipts"] = {
        "data001": data001_receipt["receipt_digest"],
        "exec001": exec001_receipt["receipt_digest"],
        "exec002": exec002_receipt["receipt_digest"],
    }
    return build_receipt(
        milestone="EXEC-003",
        producer="bt.institutional.venue.venue_identity_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "relationships": relationship_list,
            "venue_observations": observation_list,
            "as_of": as_of.isoformat(),
            "known_at": known_at.isoformat(),
            "dependencies": result["dependency_receipts"],
        },
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={"mapping_digest": result["mapping_digest"]},
        result=result,
    )
