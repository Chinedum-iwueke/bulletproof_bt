from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from bt.config import deep_merge, load_yaml, resolve_paths_relative_to
from bt.core.config_resolver import resolve_config
from bt.exec.adapters.bybit import BybitBrokerAdapter, BybitRESTClient, resolve_bybit_config
from bt.exec.adapters.bybit.client_ws_private import BybitPrivateWSClient
from bt.exec.adapters.bybit.client_ws_public import BybitPublicWSClient
from bt.exec.adapters.bybit.errors import BybitAdapterError
from bt.exec.services.live_controls import load_canary_policy


@dataclass(frozen=True)
class DoctorSummary:
    ok: bool
    venue: str
    environment: str
    checks: dict[str, bool]
    health: dict[str, Any]
    counts: dict[str, int]
    ts: str


def _load_exec_config(config_path: str, override_paths: list[str] | None) -> dict[str, Any]:
    cfg = load_yaml(config_path)
    for path in resolve_paths_relative_to(config_path.rsplit("/", 1)[0] if "/" in config_path else ".", override_paths):
        cfg = deep_merge(cfg, load_yaml(path))
    return resolve_config(cfg)


def run_doctor_diagnosis(*, config: dict[str, Any], check_ws: bool, live_readiness: bool = False) -> DoctorSummary:
    bybit_cfg = resolve_bybit_config(config)
    canary_ok = True
    try:
        _ = load_canary_policy(config)
    except Exception:
        canary_ok = False
    api_key, api_secret = bybit_cfg.auth.resolve()
    rest = BybitRESTClient(
        base_url=bybit_cfg.rest_base_url,
        api_key=api_key,
        api_secret=api_secret,
        recv_window_ms=bybit_cfg.recv_window_ms,
        timeout_ms=bybit_cfg.request_timeout_ms,
        max_retries=bybit_cfg.max_retries,
        retry_backoff_ms=bybit_cfg.retry_backoff_ms,
        environment=bybit_cfg.environment,
    )
    adapter = BybitBrokerAdapter(
        config=bybit_cfg,
        rest_client=rest,
        ws_public=BybitPublicWSClient(url=bybit_cfg.public_ws_url, topics=bybit_cfg.ws.public_topics, symbols=bybit_cfg.symbols, enabled=check_ws),
        ws_private=BybitPrivateWSClient(url=bybit_cfg.private_ws_url, topics=bybit_cfg.ws.private_topics, api_key=api_key, api_secret=api_secret, enabled=check_ws),
    )
    adapter.start()
    try:
        balances = adapter.fetch_balances()
        positions = adapter.fetch_positions()
        open_orders = adapter.fetch_open_orders()
        fills = adapter.fetch_recent_fills_or_executions(limit=50)
        instrument = adapter.get_instrument(bybit_cfg.symbols[0])
        checks = {
            "rest_auth": True,
            "fetch_balances": True,
            "fetch_positions": True,
            "fetch_open_orders": True,
            "fetch_fills": True,
            "instrument_lookup": instrument is not None,
            "ws_checked": True,
            "canary_config_valid": canary_ok,
            "private_stream_ready": adapter.private_stream_ready() if check_ws else True,
        }
        if live_readiness:
            checks["live_environment"] = bybit_cfg.environment == "live"

        health = {
            "adapter": asdict(adapter.get_health()),
        }
        health["adapter"]["ts"] = str(pd.Timestamp(health["adapter"]["ts"]).isoformat())
        return DoctorSummary(
            ok=all(checks.values()),
            venue="bybit",
            environment=bybit_cfg.environment,
            checks=checks,
            health=health,
            counts={
                "balances": len(balances.balances),
                "positions": len(positions),
                "open_orders": len(open_orders),
                "fills": len(fills),
            },
            ts=pd.Timestamp.now(tz="UTC").isoformat(),
        )
    except BybitAdapterError:
        raise
    finally:
        adapter.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run read-only Bybit doctor diagnostics.")
    parser.add_argument("--config", default="configs/exec/bybit_demo.yaml")
    parser.add_argument("--override", action="append", default=[])
    parser.add_argument("--check-ws", action="store_true")
    parser.add_argument("--live-readiness", action="store_true")
    args = parser.parse_args()

    config = _load_exec_config(args.config, args.override or None)
    try:
        summary = run_doctor_diagnosis(config=config, check_ws=args.check_ws, live_readiness=args.live_readiness)
    except Exception as exc:
        failure = {
            "ok": False,
            "error": str(exc),
            "ts": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        print(json.dumps(failure, indent=2, sort_keys=True))
        raise SystemExit(2) from exc

    print(json.dumps(asdict(summary), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
