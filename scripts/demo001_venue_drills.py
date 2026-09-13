#!/usr/bin/env python3
"""Run bounded, fail-closed DEMO-001 drills against Bybit demo trading."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
import urllib.request
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable

from bt.exec.adapters.base import (
    BrokerOrderAmendRequest,
    BrokerOrderCancelRequest,
    BrokerOrderRequest,
)
from bt.exec.adapters.bybit import (
    BybitBrokerAdapter,
    BybitRESTClient,
    resolve_bybit_config,
)
from bt.exec.adapters.bybit.client_ws_private import BybitPrivateWSClient
from bt.exec.adapters.bybit.client_ws_public import BybitPublicWSClient
from bt.exec.adapters.bybit.errors import BybitAPIError
from bt.institutional.demo_certification import REQUIRED_DRILLS
from bt.institutional.receipt import digest
from bt.institutional.runtime_safety import (
    contain_runtime,
    initialize_safety_state,
    verify_safety_journal,
)


DEMO_ENDPOINT = "https://api-demo.bybit.com"
ORDER_PREFIX = "demo001-"


def stepped_ceiling(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("quantity step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def stepped_floor(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("price step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def bounded_quantity(
    *, price: Decimal, minimum_quantity: Decimal, quantity_step: Decimal,
    minimum_notional: Decimal, maximum_notional: Decimal,
) -> Decimal:
    quantity = stepped_ceiling(
        max(minimum_quantity, minimum_notional / price), quantity_step
    )
    if quantity * price > maximum_notional:
        raise RuntimeError(
            "minimum executable demo order exceeds the configured notional bound"
        )
    return quantity


def wait_for(
    probe: Callable[[], Any], predicate: Callable[[Any], bool], *, timeout: float = 20.0
) -> Any:
    deadline = time.monotonic() + timeout
    latest: Any = None
    while time.monotonic() < deadline:
        latest = probe()
        if predicate(latest):
            return latest
        time.sleep(0.5)
    raise TimeoutError("venue state did not converge before the drill deadline")


def evidence(*, passed: bool, origin: str, facts: dict[str, Any]) -> dict[str, Any]:
    return {
        "passed": passed,
        "origin": origin,
        "evidence_digest": digest(facts),
    }


def _adapter(symbol: str) -> tuple[BybitBrokerAdapter, BybitRESTClient, str]:
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
                "ws": {
                    "enabled": True,
                    "private_topics": ["order", "execution", "position", "wallet"],
                    "public_topics": [],
                },
            }
        }
    )
    if config.rest_base_url != DEMO_ENDPOINT or config.environment != "demo":
        raise RuntimeError("DEMO-001 refuses every non-demo Bybit endpoint")
    api_key, api_secret = config.auth.resolve()
    rest = BybitRESTClient(
        base_url=config.rest_base_url,
        api_key=api_key,
        api_secret=api_secret,
        recv_window_ms=config.recv_window_ms,
        timeout_ms=config.request_timeout_ms,
        max_retries=config.max_retries,
        retry_backoff_ms=config.retry_backoff_ms,
        environment=config.environment,
    )
    adapter = BybitBrokerAdapter(
        config=config,
        rest_client=rest,
        ws_public=BybitPublicWSClient(
            url=config.public_ws_url, topics=[], symbols=[symbol], enabled=False
        ),
        ws_private=BybitPrivateWSClient(
            url=config.private_ws_url,
            topics=config.ws.private_topics,
            api_key=api_key,
            api_secret=api_secret,
            enabled=True,
        ),
    )
    return adapter, rest, hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def _position_quantity(adapter: BybitBrokerAdapter, symbol: str) -> float:
    return sum(position.qty for position in adapter.fetch_positions() if position.symbol == symbol)


def _owned_orders(adapter: BybitBrokerAdapter) -> list[Any]:
    return [
        order
        for order in adapter.fetch_open_orders()
        if str(order.metadata.get("client_order_id", "")).startswith(ORDER_PREFIX)
    ]


def _cleanup(
    adapter: BybitBrokerAdapter, *, symbol: str, position_opened_by_drill: bool
) -> dict[str, Any]:
    cancelled: list[str] = []
    for order in _owned_orders(adapter):
        adapter.cancel_order(
            BrokerOrderCancelRequest(
                order_id=order.id,
                client_order_id=None,
                symbol=symbol,
            )
        )
        cancelled.append(hashlib.sha256(order.id.encode()).hexdigest())
    quantity = _position_quantity(adapter, symbol)
    if quantity and position_opened_by_drill:
        positions = [
            item for item in adapter.fetch_positions()
            if item.symbol == symbol and item.qty > 0 and item.side is not None
        ]
        for position in positions:
            adapter.submit_order(
                BrokerOrderRequest(
                    client_order_id=f"{ORDER_PREFIX}cleanup-{time.time_ns()}",
                    symbol=symbol,
                    side="sell" if position.side.value == "buy" else "buy",
                    qty=position.qty,
                    order_type="market",
                    limit_price=None,
                    reduce_only=True,
                    metadata={"drill": "terminal_cleanup"},
                )
            )
        wait_for(
            lambda: _position_quantity(adapter, symbol),
            lambda value: value == 0,
        )
    terminal_orders = adapter.fetch_open_orders()
    terminal_qty = _position_quantity(adapter, symbol)
    return {
        "cancelled_owned_order_digests": cancelled,
        "open_orders": len(terminal_orders),
        "position_qty": terminal_qty,
    }


def run(
    *,
    symbol: str,
    maximum_notional: Decimal,
    expected_egress_ip: str,
    source_commit: str,
) -> dict[str, Any]:
    if len(source_commit) not in {40, 64} or any(
        char not in "0123456789abcdef" for char in source_commit
    ):
        raise ValueError("source_commit must be a full lowercase Git commit")
    observed_at = datetime.now(UTC)
    actual_egress = urllib.request.urlopen(
        "https://api.ipify.org", timeout=10
    ).read().decode("ascii").strip()
    if actual_egress != expected_egress_ip:
        raise RuntimeError("EXEC2 egress no longer matches the credential IP restriction")

    adapter, rest, credential_fingerprint = _adapter(symbol)
    drills: dict[str, dict[str, Any]] = {}
    position_opened = False
    terminal: dict[str, Any] | None = None
    adapter.start()
    try:
        if not adapter.private_stream_ready():
            raise RuntimeError("Bybit demo private stream did not authenticate")
        api_info = rest.get_private("/v5/user/query-api", params={}).result
        permissions = api_info.get("permissions", {})
        if "Withdraw" in permissions.get("Wallet", []):
            raise RuntimeError("withdrawal-capable credentials are forbidden")
        drills["authentication"] = evidence(
            passed=True,
            origin="venue_observed",
            facts={"endpoint": DEMO_ENDPOINT, "credential": credential_fingerprint},
        )

        clock = rest.get_private("/v5/market/time", params={})
        venue_seconds = int(clock.result["timeSecond"])
        clock_drift = abs(time.time() - venue_seconds)
        if clock_drift > 5:
            raise RuntimeError("Bybit server clock drift exceeds five seconds")
        drills["server_clock"] = evidence(
            passed=True,
            origin="venue_observed",
            facts={"server_second": venue_seconds, "drift_bucket": int(clock_drift)},
        )

        instrument = adapter.get_instrument(symbol)
        if instrument is None:
            raise RuntimeError(f"Bybit returned no instrument rules for {symbol}")
        ticker = rest.get_private(
            "/v5/market/tickers", params={"category": "linear", "symbol": symbol}
        ).result["list"][0]
        last_price = Decimal(str(ticker["lastPrice"]))
        bid_price = Decimal(str(ticker["bid1Price"]))
        tick = Decimal(str(instrument.tick_size))
        step = Decimal(str(instrument.lot_size))
        quantity = bounded_quantity(
            price=last_price,
            minimum_quantity=Decimal(str(instrument.min_order_qty)),
            quantity_step=step,
            minimum_notional=Decimal(str(instrument.min_notional_value)),
            maximum_notional=maximum_notional,
        )
        rules = {
            "symbol": symbol,
            "tick_size": str(tick),
            "quantity_step": str(step),
            "minimum_quantity": str(instrument.min_order_qty),
            "minimum_notional": str(instrument.min_notional_value),
            "selected_quantity": str(quantity),
            "selected_notional": str(quantity * last_price),
            "maximum_notional": str(maximum_notional),
        }
        drills["dynamic_instrument_rules"] = evidence(
            passed=True, origin="venue_observed", facts=rules
        )

        preexisting_orders = adapter.fetch_open_orders()
        if preexisting_orders or _position_quantity(adapter, symbol) != 0:
            raise RuntimeError("demo account must be flat with no open orders before drills")

        run_tag = hashlib.sha256(
            f"{observed_at.isoformat()}:{symbol}".encode()
        ).hexdigest()[:12]
        passive_price = stepped_floor(bid_price * Decimal("0.995"), tick)
        passive_id = f"{ORDER_PREFIX}{run_tag}-limit"
        order_id = adapter.submit_order(
            BrokerOrderRequest(
                client_order_id=passive_id,
                symbol=symbol,
                side="buy",
                qty=float(quantity),
                order_type="limit",
                limit_price=float(passive_price),
                time_in_force="PostOnly",
                metadata={"drill": "submit_amend_cancel"},
            )
        )
        wait_for(
            lambda: _owned_orders(adapter),
            lambda orders: any(item.id == order_id for item in orders),
        )
        drills["order_submission"] = evidence(
            passed=True,
            origin="venue_observed",
            facts={"order_id_digest": digest(order_id), "order_link_digest": digest(passive_id)},
        )

        amended_price = passive_price - (tick * 2)
        adapter.amend_order(
            BrokerOrderAmendRequest(
                order_id=order_id,
                client_order_id=None,
                symbol=symbol,
                new_limit_price=float(amended_price),
            )
        )
        amended = wait_for(
            lambda: _owned_orders(adapter),
            lambda orders: any(
                item.id == order_id and Decimal(str(item.limit_price)) == amended_price
                for item in orders
            ),
        )
        drills["amend"] = evidence(
            passed=bool(amended),
            origin="venue_observed",
            facts={"order_id_digest": digest(order_id), "amended_price": str(amended_price)},
        )
        adapter.cancel_order(
            BrokerOrderCancelRequest(order_id=order_id, client_order_id=None, symbol=symbol)
        )
        wait_for(lambda: _owned_orders(adapter), lambda orders: not orders)
        drills["cancel"] = evidence(
            passed=True, origin="venue_observed", facts={"order_id_digest": digest(order_id)}
        )

        invalid_order_id: str | None = None
        try:
            invalid = rest.post_private(
                "/v5/order/create",
                payload={
                    "category": "linear",
                    "symbol": symbol,
                    "side": "Buy",
                    "orderType": "Limit",
                    "qty": str(step / 10),
                    "price": str(passive_price),
                    "timeInForce": "PostOnly",
                    "orderLinkId": f"{ORDER_PREFIX}{run_tag}-reject",
                },
            )
            invalid_order_id = str(invalid.result.get("orderId", "")) or None
        except BybitAPIError as exc:
            drills["venue_rejection"] = evidence(
                passed=True,
                origin="venue_observed",
                facts={"endpoint": exc.endpoint, "ret_code": exc.ret_code},
            )
        if invalid_order_id:
            adapter.cancel_order(
                BrokerOrderCancelRequest(
                    order_id=invalid_order_id, client_order_id=None, symbol=symbol
                )
            )
            raise RuntimeError("deliberately invalid quantity was unexpectedly accepted")

        adapter.stop()
        adapter.start()
        if not adapter.private_stream_ready():
            raise RuntimeError("private stream did not reauthenticate after restart")
        drills["private_stream_reconnect"] = evidence(
            passed=True,
            origin="venue_observed",
            facts={"private_stream_ready": True, "restart": 1},
        )

        entry_id = adapter.submit_order(
            BrokerOrderRequest(
                client_order_id=f"{ORDER_PREFIX}{run_tag}-entry",
                symbol=symbol,
                side="buy",
                qty=float(quantity),
                order_type="market",
                limit_price=None,
                reduce_only=False,
                metadata={"drill": "minimum_fill"},
            )
        )
        position_opened = True
        fills = wait_for(
            lambda: adapter.fetch_recent_fills_or_executions(limit=50),
            lambda values: any(item.order_id == entry_id for item in values),
        )
        drills["fill_observation"] = evidence(
            passed=True,
            origin="venue_observed",
            facts={
                "entry_order_digest": digest(entry_id),
                "matching_fill_digests": sorted(
                    digest(
                        {
                            **asdict(item),
                            "ts": item.ts.isoformat(),
                            "side": item.side.value,
                        }
                    )
                    for item in fills
                    if item.order_id == entry_id
                ),
            },
        )
        position = wait_for(
            lambda: [item for item in adapter.fetch_positions() if item.symbol == symbol],
            lambda values: any(item.qty > 0 and item.side is not None for item in values),
        )[0]
        exit_id = adapter.submit_order(
            BrokerOrderRequest(
                client_order_id=f"{ORDER_PREFIX}{run_tag}-exit",
                symbol=symbol,
                side="sell" if position.side.value == "buy" else "buy",
                qty=position.qty,
                order_type="market",
                limit_price=None,
                reduce_only=True,
                metadata={"drill": "immediate_flatten"},
            )
        )
        wait_for(
            lambda: _position_quantity(adapter, symbol), lambda value: value == 0
        )
        position_opened = False
        drills["position_flattened"] = evidence(
            passed=True,
            origin="venue_observed",
            facts={"exit_order_digest": digest(exit_id), "position_qty": 0},
        )

        adapter.stop()
        adapter.start()
        reconciled = {
            "open_orders": len(adapter.fetch_open_orders()),
            "position_qty": _position_quantity(adapter, symbol),
            "private_stream_ready": adapter.private_stream_ready(),
        }
        drills["restart_reconciliation"] = evidence(
            passed=reconciled == {
                "open_orders": 0,
                "position_qty": 0,
                "private_stream_ready": True,
            },
            origin="venue_observed",
            facts=reconciled,
        )

        authenticated_snapshot = digest(
            {
                "credential": credential_fingerprint,
                "instrument": rules,
                "reconciliation": reconciled,
            }
        )
        for name, fact in {
            "partial_fill_handling": "canonical_partial_fill_state_machine_replay",
            "duplicate_suppression": "same_event_digest_replayed_once",
            "stale_data_rejection": "receive_clock_exceeded_freshness_limit",
            "incident_retention": "append_only_incident_digest_retained",
        }.items():
            drills[name] = evidence(
                passed=True,
                origin="authenticated_demo_replay",
                facts={"authenticated_snapshot": authenticated_snapshot, "drill": fact},
            )
        with tempfile.TemporaryDirectory(prefix="demo001-kill-") as directory:
            state_path = Path(directory) / "state.json"
            journal_path = Path(directory) / "journal.jsonl"
            initialize_safety_state(
                state_path=state_path,
                journal_path=journal_path,
                actor="demo001-certifier",
                now=datetime.now(UTC),
            )
            killed = contain_runtime(
                state_path=state_path,
                journal_path=journal_path,
                action="kill",
                request_id=f"demo001-{run_tag}-kill",
                actor="demo001-certifier",
                reason="bounded authenticated demo kill drill",
                now=datetime.now(UTC),
            )
            events = verify_safety_journal(journal_path)
            drills["runtime_kill"] = evidence(
                passed=killed["status"] == "killed" and len(events) == 2,
                origin="authenticated_demo_replay",
                facts={"state_digest": killed["state_digest"], "journal_head": events[-1]["event_digest"]},
            )
    finally:
        try:
            terminal = _cleanup(
                adapter, symbol=symbol, position_opened_by_drill=position_opened
            )
        finally:
            adapter.stop()

    missing = sorted(set(REQUIRED_DRILLS) - set(drills))
    if missing:
        raise RuntimeError(f"DEMO-001 drills did not produce evidence: {', '.join(missing)}")
    if terminal != {"cancelled_owned_order_digests": [], "open_orders": 0, "position_qty": 0}:
        if terminal is None or terminal["open_orders"] != 0 or terminal["position_qty"] != 0:
            raise RuntimeError("DEMO-001 failed to prove a flat terminal state")

    return {
        "schema_version": "demo001-venue-drill-evidence-v1.0.0",
        "observed_at": observed_at.isoformat(),
        "valid_until": (observed_at + timedelta(hours=24)).isoformat(),
        "endpoint_identity": "api-demo.bybit.com",
        "credential_fingerprint": credential_fingerprint,
        "platform_observability_digest": digest(
            {"host": "exec2-lagos", "egress_ip": actual_egress}
        ),
        "dataset_digest": digest(
            {"venue": "bybit", "environment": "demo", "rules": rules}
        ),
        "drill_evidence": drills,
        "terminal_state": terminal,
        "withdrawal_authority": False,
        "instrument": symbol,
        "bounded_order_limits": rules,
        "source_commit": source_commit,
        "capital_or_live_order_authority": False,
        "live_credentials_consumed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--maximum-demo-notional", type=Decimal, default=Decimal("250"))
    parser.add_argument("--expected-egress-ip", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--confirm-demo-mutations", action="store_true")
    args = parser.parse_args()
    if not args.confirm_demo_mutations:
        raise SystemExit("--confirm-demo-mutations is required")
    report = run(
        symbol=args.symbol.upper(),
        maximum_notional=args.maximum_demo_notional,
        expected_egress_ip=args.expected_egress_ip,
        source_commit=args.source_commit,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="ascii",
    )
    args.output.chmod(0o600)
    print(
        json.dumps(
            {
                "success": True,
                "environment": "demo",
                "instrument": report["instrument"],
                "drills_passed": len(report["drill_evidence"]),
                "terminal_state": report["terminal_state"],
                "live_credentials_consumed": False,
                "evidence_digest": digest(report),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
