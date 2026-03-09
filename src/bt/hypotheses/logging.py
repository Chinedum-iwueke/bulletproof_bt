"""Canonical hypothesis run logging row builders."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

REQUIRED_LOG_FIELDS = (
    "run_id", "hypothesis_id", "title", "contract_version", "grid_id", "config_hash", "symbol", "timeframe",
    "start_ts", "end_ts", "tier", "execution_model_name", "params_json", "indicators_json", "gates_json",
    "num_trades", "ev_r_gross", "ev_r_net", "pnl_gross", "pnl_net", "hit_rate", "max_drawdown_r",
    "mae_mean_r", "mfe_mean_r", "avg_hold_bars", "status", "failure_reason", "created_at_utc",
)


def make_log_row(base: dict[str, Any], metrics: dict[str, Any], *, status: str = "ok", failure_reason: str = "") -> dict[str, Any]:
    row = {k: None for k in REQUIRED_LOG_FIELDS}
    row.update(base)
    row.update(metrics)
    row["status"] = status
    row["failure_reason"] = failure_reason
    row["created_at_utc"] = datetime.now(timezone.utc).isoformat()
    return row
