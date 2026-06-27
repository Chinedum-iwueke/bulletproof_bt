from __future__ import annotations

import json
import os
import sys
import hashlib
from dataclasses import asdict
from typing import Any

from bt.exec.adapters.base import BrokerOrderCancelRequest, BrokerOrderRequest
from bt.exec.adapters.binance import BinanceBrokerAdapter, BinanceRESTClient, resolve_binance_config
from bt.exec.adapters.binance.client_ws_private import BinancePrivateWSClient
from bt.exec.adapters.bybit import BybitBrokerAdapter, BybitRESTClient, resolve_bybit_config
from bt.exec.adapters.bybit.client_ws_private import BybitPrivateWSClient
from bt.exec.adapters.bybit.client_ws_public import BybitPublicWSClient


def _adapter(payload: dict[str, Any]):
    venue = str(payload["venue"]).lower()
    environment = str(payload["environment"]).lower()
    symbols = [str(item).upper() for item in payload.get("symbols", [])]
    key_env = "INVARIANCE_CONNECTOR_KEY"
    secret_env = "INVARIANCE_CONNECTOR_SECRET"
    os.environ[key_env] = str(payload["api_key"])
    os.environ[secret_env] = str(payload["api_secret"])
    probe_private_stream = payload.get("probe_private_stream") is True
    config = {"broker": {
        "venue": venue, "environment": environment, "product_type": str(payload.get("product_type", "perpetual")), "symbols": symbols,
        "auth": {"api_key_env": key_env, "api_secret_env": secret_env},
        "ws": {"enabled": probe_private_stream},
    }}
    if venue == "bybit":
        cfg = resolve_bybit_config(config)
        key, secret = cfg.auth.resolve()
        rest = BybitRESTClient(
            base_url=cfg.rest_base_url, api_key=key, api_secret=secret,
            recv_window_ms=cfg.recv_window_ms, timeout_ms=cfg.request_timeout_ms,
            max_retries=cfg.max_retries, retry_backoff_ms=cfg.retry_backoff_ms,
            environment=environment,
        )
        return BybitBrokerAdapter(
            config=cfg, rest_client=rest,
            ws_public=BybitPublicWSClient(url=cfg.public_ws_url, topics=[], symbols=symbols, enabled=False),
            ws_private=BybitPrivateWSClient(
                url=cfg.private_ws_url,
                topics=cfg.ws.private_topics,
                api_key=key,
                api_secret=secret,
                enabled=probe_private_stream,
            ),
        )
    if venue == "binance":
        cfg = resolve_binance_config(config)
        key, secret = cfg.auth.resolve()
        rest = BinanceRESTClient(
            base_url=cfg.rest_base_url, api_key=key, api_secret=secret,
            recv_window_ms=cfg.recv_window_ms, timeout_ms=cfg.request_timeout_ms,
            max_retries=cfg.max_retries, retry_backoff_ms=cfg.retry_backoff_ms,
            environment=environment,
        )
        private = BinancePrivateWSClient(
            rest=rest,
            ws_base_url=cfg.ws_base_url,
            product_type=cfg.product_type,
            enabled=probe_private_stream,
        ) if probe_private_stream else None
        return BinanceBrokerAdapter(config=cfg, rest_client=rest, ws_private=private)
    raise ValueError("connector_venue_unsupported")


def _position(value: Any) -> dict[str, Any]:
    output = asdict(value)
    output["state"] = value.state.value
    output["side"] = None if value.side is None else value.side.value
    return output


def _order(value: Any) -> dict[str, Any]:
    output = asdict(value)
    output["ts_submitted"] = value.ts_submitted.isoformat()
    output["side"] = value.side.value
    output["order_type"] = value.order_type.value
    output["state"] = value.state.value
    return output


def _fill(value: Any) -> dict[str, Any]:
    output = asdict(value)
    output["ts"] = value.ts.isoformat()
    output["side"] = value.side.value
    return output


def execute(payload: dict[str, Any]) -> dict[str, Any]:
    adapter = _adapter(payload)
    adapter.start()
    try:
        action = str(payload.get("action", "snapshot"))
        if action in {"doctor", "snapshot", "reconcile"}:
            balances = adapter.fetch_balances()
            positions = adapter.fetch_positions()
            orders = adapter.fetch_open_orders()
            fills = adapter.fetch_recent_fills_or_executions(limit=int(payload.get("fill_limit", 200)))
            instruments = [adapter.get_instrument(symbol) for symbol in payload.get("symbols", [])]
            private_stream_ready = adapter.private_stream_ready()
            private_events = adapter.iter_events()
            return {
                "ok": True,
                "action": action,
                "venue": payload["venue"],
                "environment": payload["environment"],
                "product_type": payload.get("product_type", "perpetual"),
                "health": asdict(adapter.get_health()) | {"ts": adapter.get_health().ts.isoformat()},
                "balances": balances.balances,
                "balance_metadata": balances.metadata,
                "positions": [_position(item) for item in positions],
                "open_orders": [_order(item) for item in orders],
                "fills": [_fill(item) for item in fills],
                "instruments": [asdict(item) for item in instruments if item is not None],
                "private_stream_ready": private_stream_ready,
                "private_event_count": len(private_events),
                "runtime_checks": {
                    "rest_auth": True,
                    "server_time_sync": True,
                    "instrument_metadata": all(item is not None for item in instruments),
                    "balance_snapshot": True,
                    "private_stream_auth": private_stream_ready,
                    "restart_reconciliation": action == "reconcile",
                    "live_mutation_lock": True,
                },
                "authoritative_source": "exchange_rest",
            }
        if action == "submit_order":
            if payload["environment"] == "live":
                if payload.get("live_canary_approved") is not True:
                    raise ValueError("live_canary_approval_required")
                adapter.set_live_mutations_enabled(True)
            order = payload.get("order") if isinstance(payload.get("order"), dict) else {}
            order_id = adapter.submit_order(BrokerOrderRequest(
                client_order_id=str(order["client_order_id"]), symbol=str(order["symbol"]),
                side=str(order["side"]), qty=float(order["qty"]), order_type=str(order["order_type"]),
                limit_price=None if order.get("limit_price") is None else float(order["limit_price"]),
                time_in_force=None if order.get("time_in_force") is None else str(order["time_in_force"]),
                reduce_only=bool(order.get("reduce_only", False)), metadata=dict(order.get("metadata", {})),
            ))
            return {"ok": True, "action": action, "order_id": order_id, "client_order_id": order["client_order_id"]}
        if action == "cancel_order":
            if payload["environment"] == "live":
                if payload.get("live_canary_approved") is not True:
                    raise ValueError("live_canary_approval_required")
                adapter.set_live_mutations_enabled(True)
            adapter.cancel_order(BrokerOrderCancelRequest(
                order_id=payload.get("order_id"), client_order_id=payload.get("client_order_id"), symbol=payload.get("symbol")
            ))
            return {"ok": True, "action": action}
        if action == "emergency_freeze":
            if payload["environment"] == "live":
                if payload.get("live_canary_approved") is not True:
                    raise ValueError("live_canary_approval_required")
                adapter.set_live_mutations_enabled(True)
            cancelled: list[str] = []
            for order in adapter.fetch_open_orders():
                adapter.cancel_order(BrokerOrderCancelRequest(
                    order_id=order.id,
                    client_order_id=str(order.metadata.get("client_order_id", "")) or None,
                    symbol=order.symbol,
                ))
                cancelled.append(order.id)
            closed: list[str] = []
            if payload.get("close_positions") is True and payload.get("product_type") == "perpetual":
                for position in adapter.fetch_positions():
                    if position.qty <= 0 or position.side is None:
                        continue
                    digest = hashlib.sha256(
                        f"freeze:{payload.get('deployment_id', '')}:{position.symbol}".encode()
                    ).hexdigest()[:20]
                    adapter.submit_order(BrokerOrderRequest(
                        client_order_id=f"ir-freeze-{digest}",
                        symbol=position.symbol,
                        side="sell" if position.side.value == "buy" else "buy",
                        qty=position.qty,
                        order_type="market",
                        limit_price=None,
                        reduce_only=True,
                        metadata={"reason": "emergency_freeze"},
                    ))
                    closed.append(position.symbol)
            return {
                "ok": True,
                "action": action,
                "cancelled_order_ids": cancelled,
                "reduce_only_close_symbols": closed,
                "mutations_disabled_after": True,
            }
        raise ValueError("connector_action_unsupported")
    finally:
        adapter.stop()


def main() -> None:
    try:
        payload = json.loads(sys.stdin.read())
        result = execute(payload)
        print(json.dumps(result, separators=(",", ":"), default=str))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "error_type": type(exc).__name__}, separators=(",", ":")))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
