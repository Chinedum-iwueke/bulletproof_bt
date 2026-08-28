"""Typed observed/inferred market-microstructure state production."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Iterable

from .execution import CanonicalExecutionEvent, verify_event
from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

MODEL_SCHEMA_VERSION = "exec002-microstructure-model-v1.0.0"


class MicrostructureStateError(ValueError):
    """Inputs cannot support a truthful microstructure state."""


def _decimal(value: Any, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except Exception as exc:
        raise MicrostructureStateError(f"{field} must be numeric") from exc
    if not number.is_finite():
        raise MicrostructureStateError(f"{field} must be finite")
    return number


def _field(
    value: Any,
    *,
    status: str,
    source_event_ids: list[str],
    limitations: list[str] | None = None,
    uncertainty: str = "none",
) -> dict[str, Any]:
    return {
        "value": value,
        "status": status,
        "source_event_ids": sorted(source_event_ids),
        "limitations": sorted(limitations or []),
        "uncertainty": uncertainty,
    }


def microstructure_state(
    *,
    events: Iterable[CanonicalExecutionEvent],
    as_of: datetime,
    maximum_auxiliary_age_seconds: int = 3600,
) -> dict[str, Any]:
    cutoff = as_of.astimezone(UTC)
    documents = []
    for event in events:
        if not verify_event(event):
            raise MicrostructureStateError("canonical event integrity failed")
        if datetime.fromisoformat(event.receive_time) <= cutoff:
            documents.append(event)
    documents.sort(key=lambda event: (event.event_time, event.source_sequence, event.event_digest))
    reset_index = max(
        (index for index, event in enumerate(documents) if event.kind == "venue_reset"),
        default=-1,
    )
    active = documents[reset_index + 1 :]
    latest: dict[str, CanonicalExecutionEvent] = {}
    trades: list[CanonicalExecutionEvent] = []
    for event in active:
        if event.kind == "trade":
            trades.append(event)
        else:
            latest[event.kind] = event

    limitations: list[str] = []
    fields: dict[str, dict[str, Any]] = {}
    book = latest.get("order_book")
    if book:
        bids = book.payload.get("bids") or []
        asks = book.payload.get("asks") or []
        if not bids or not asks:
            limitations.append("order_book_levels_missing")
            fields["order_book"] = _field(None, status="unavailable", source_event_ids=[book.event_id], limitations=["levels_missing"])
        else:
            best_bid = max(_decimal(level[0], "bid price") for level in bids)
            best_ask = min(_decimal(level[0], "ask price") for level in asks)
            if best_bid >= best_ask:
                raise MicrostructureStateError("crossed or locked order book")
            bid_depth = sum(_decimal(level[1], "bid size") for level in bids)
            ask_depth = sum(_decimal(level[1], "ask size") for level in asks)
            total_depth = bid_depth + ask_depth
            fields["best_bid"] = _field(str(best_bid), status="observed", source_event_ids=[book.event_id])
            fields["best_ask"] = _field(str(best_ask), status="observed", source_event_ids=[book.event_id])
            fields["spread"] = _field(str(best_ask - best_bid), status="observed", source_event_ids=[book.event_id])
            imbalance = (bid_depth - ask_depth) / total_depth if total_depth else Decimal("0")
            fields["depth_imbalance"] = _field(str(imbalance), status="observed", source_event_ids=[book.event_id])
    else:
        fields["order_book"] = _field(None, status="unavailable", source_event_ids=[], limitations=["no_post_reset_snapshot"])
        limitations.append("order_book_unavailable")

    if trades:
        buy = sum(_decimal(event.payload.get("size", 0), "trade size") for event in trades if event.payload.get("aggressor_side") == "buy")
        sell = sum(_decimal(event.payload.get("size", 0), "trade size") for event in trades if event.payload.get("aggressor_side") == "sell")
        total = buy + sell
        fields["trade_imbalance"] = _field(
            str((buy - sell) / total if total else Decimal("0")),
            status="observed",
            source_event_ids=[event.event_id for event in trades],
        )
    else:
        fields["trade_imbalance"] = _field(None, status="unavailable", source_event_ids=[], limitations=["no_trades"])

    mark = latest.get("mark_price")
    index = latest.get("index_price")
    if mark and index:
        mark_value = _decimal(mark.payload.get("price"), "mark price")
        index_value = _decimal(index.payload.get("price"), "index price")
        if index_value <= 0:
            raise MicrostructureStateError("index price must be positive")
        fields["basis"] = _field(
            str((mark_value - index_value) / index_value),
            status="observed",
            source_event_ids=[mark.event_id, index.event_id],
        )
    else:
        fields["basis"] = _field(None, status="unavailable", source_event_ids=[], limitations=["mark_or_index_missing"])

    for kind, output in (("funding", "funding_rate"), ("open_interest", "open_interest")):
        event = latest.get(kind)
        if not event:
            fields[output] = _field(None, status="unavailable", source_event_ids=[], limitations=[f"{kind}_missing"])
            continue
        age = (cutoff - datetime.fromisoformat(event.receive_time)).total_seconds()
        stale = age > maximum_auxiliary_age_seconds
        fields[output] = _field(
            event.payload.get("value"),
            status="observed" if not stale else "unavailable",
            source_event_ids=[event.event_id],
            limitations=[f"{kind}_stale"] if stale else [],
            uncertainty="stale" if stale else "none",
        )

    liquidations = [event for event in active if event.kind == "liquidation"]
    proxy = latest.get("liquidation_proxy")
    if liquidations:
        notional = sum(_decimal(event.payload.get("notional", 0), "liquidation notional") for event in liquidations)
        fields["liquidation_notional"] = _field(str(notional), status="observed", source_event_ids=[event.event_id for event in liquidations])
    elif proxy:
        fields["liquidation_notional"] = _field(
            proxy.payload.get("value"), status="inferred", source_event_ids=[proxy.event_id],
            limitations=["proxy_not_exchange_liquidation"], uncertainty="model_dependent",
        )
    else:
        fields["liquidation_notional"] = _field(None, status="unavailable", source_event_ids=[], limitations=["liquidation_history_absent_not_zero"])

    coverage = {
        "observed": sum(field["status"] == "observed" for field in fields.values()),
        "inferred": sum(field["status"] == "inferred" for field in fields.values()),
        "unavailable": sum(field["status"] == "unavailable" for field in fields.values()),
    }
    result = {
        "schema_version": MODEL_SCHEMA_VERSION,
        "as_of": cutoff.isoformat(),
        "venue_reset_observed": reset_index >= 0,
        "fields": fields,
        "coverage": coverage,
        "limitations": sorted(set(limitations)),
        "source_event_digests": [event.event_digest for event in active],
    }
    result["state_digest"] = digest(result)
    return result


def microstructure_state_receipt(
    *,
    events: Iterable[CanonicalExecutionEvent],
    exec001_receipt: dict[str, Any],
    data003_receipt: dict[str, Any],
    as_of: datetime,
    source_commit: str,
    dataset_digest: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    if not verify_receipt(exec001_receipt) or exec001_receipt["milestone"] != "EXEC-001" or not exec001_receipt["result"]["reconstructable"]:
        raise MicrostructureStateError("EXEC-001 receipt is not admissible")
    if not verify_receipt(data003_receipt) or data003_receipt["milestone"] != "DATA-003":
        raise MicrostructureStateError("DATA-003 receipt is not admissible")
    event_list = list(events)
    state = microstructure_state(
        events=event_list,
        as_of=as_of,
        maximum_auxiliary_age_seconds=int(configuration.get("maximum_auxiliary_age_seconds", 3600)),
    )
    result = {
        "schema_version": "exec002-microstructure-dossier-v1.0.0",
        "model_schema_version": MODEL_SCHEMA_VERSION,
        "model_schema_digest": digest({"schema_version": MODEL_SCHEMA_VERSION}),
        "exec001_receipt_digest": exec001_receipt["receipt_digest"],
        "data003_receipt_digest": data003_receipt["receipt_digest"],
        "state": state,
        "claim": "microstructure evidence only; observed, inferred and unavailable fields remain distinct",
    }
    return build_receipt(
        milestone="EXEC-002",
        producer="bt.institutional.microstructure.microstructure_state_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"event_digests": [event.event_digest for event in event_list], "as_of": as_of.isoformat()},
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={"state_digest": state["state_digest"], "coverage": state["coverage"]},
        result=result,
    )
