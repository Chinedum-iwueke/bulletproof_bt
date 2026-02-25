from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from bt.logging.formatting import write_json_deterministic


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def _instrument_type_from_config(config_payload: dict[str, Any] | None) -> str:
    instrument_cfg = config_payload.get("instrument") if isinstance(config_payload, dict) else None
    raw_type = instrument_cfg.get("type") if isinstance(instrument_cfg, dict) else None
    if raw_type == "forex":
        return "fx"
    if raw_type in {"crypto", "equity", "futures"}:
        return str(raw_type)
    return "unknown"


def _execution_profile(run_dir: Path, config_payload: dict[str, Any] | None) -> str:
    run_status_payload = _read_json(run_dir / "run_status.json")
    status_profile = run_status_payload.get("execution_profile") if isinstance(run_status_payload, dict) else None
    if isinstance(status_profile, str) and status_profile:
        return status_profile

    execution_cfg = config_payload.get("execution") if isinstance(config_payload, dict) else None
    cfg_profile = execution_cfg.get("profile") if isinstance(execution_cfg, dict) else None
    if isinstance(cfg_profile, str) and cfg_profile:
        return cfg_profile

    return "unknown"


def _account_currency(config_payload: dict[str, Any] | None) -> str | None:
    if not isinstance(config_payload, dict):
        return None
    account_cfg = config_payload.get("account")
    if isinstance(account_cfg, dict) and isinstance(account_cfg.get("currency"), str):
        return account_cfg["currency"]
    if isinstance(config_payload.get("account_currency"), str):
        return config_payload["account_currency"]
    return None


def write_cost_breakdown_json(run_dir: Path, performance_payload: dict[str, Any]) -> Path:
    config_payload: dict[str, Any] | None = None
    config_path = run_dir / "config_used.yaml"
    if config_path.exists():
        try:
            loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            loaded = None
        if isinstance(loaded, dict):
            config_payload = loaded

    costs = performance_payload.get("costs") if isinstance(performance_payload.get("costs"), dict) else {}
    payload = {
        "schema_version": 1,
        "totals": {
            "fees_total": float(costs.get("fees_total", 0.0)),
            "slippage_total": float(costs.get("slippage_total", 0.0)),
            "spread_total": float(costs.get("spread_total", 0.0)),
            "commission_total": float(costs.get("commission_total", 0.0)),
        },
        "notes": {
            "execution_profile": _execution_profile(run_dir, config_payload),
            "instrument_type": _instrument_type_from_config(config_payload),
            "currency": _account_currency(config_payload),
        },
    }
    path = run_dir / "cost_breakdown.json"
    write_json_deterministic(path, payload)
    return path
