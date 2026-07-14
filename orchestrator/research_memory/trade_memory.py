from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TRADE_COLUMNS = [
    "id",
    "source_trade_id",
    "run_id",
    "hypothesis_id",
    "hypothesis_name",
    "strategy_id",
    "setup_class",
    "symbol",
    "dataset_type",
    "phase",
    "research_tier",
    "research_mode",
    "evidence_type",
    "portfolio_constraints_applied",
    "capital_path_valid",
    "deployability_evidence",
    "signal_episode_evidence",
    "experiment_root",
    "ts_signal",
    "ts_entry_fill",
    "ts_exit_fill",
    "r_net",
    "r_gross",
    "pnl_net",
    "pnl_gross",
    "mfe_r",
    "mae_r",
    "exit_efficiency",
    "cost_drag_r",
    "fee_drag_r",
    "slippage_drag_r",
    "spread_drag_r",
    "csi_pctile",
    "csi_source",
    "csi_components_json",
    "vol_pctile",
    "spread_pctile",
    "tr_over_atr",
    "tr_over_atr_pctile",
    "volume_pctile",
    "vol_of_vol_pctile",
    "funding_raw",
    "funding_pctile",
    "funding_z",
    "oi_level",
    "oi_accel",
    "oi_accel_pctile",
    "oi_z",
    "mark_price",
    "index_price",
    "basis_raw",
    "basis_pct",
    "basis_pctile",
    "premium_pctile",
    "crowding_proxy_pctile",
    "constraint_stress_pctile",
    "trend_state",
    "vol_regime",
    "liquidity_regime",
    "displacement_regime",
    "market_regime_class",
    "structure_class",
    "label_success_1r_before_neg_1r",
    "label_success_2r_before_neg_1r",
    "label_reached_3r",
    "label_reached_5r",
    "label_tail_trade_ge_10r",
    "label_profitable_after_costs",
    "label_high_cost_trade",
    "metrics_valid",
    "invalid_reason",
    "raw_json",
    "created_at",
]

NORMALIZE_SOURCE_COLUMNS = {
    "trade_id",
    "identity_trade_id",
    "source_trade_id",
    "run_id",
    "identity_run_id",
    "hypothesis_id",
    "hypothesis_name",
    "identity_hypothesis_name",
    "strategy_id",
    "identity_strategy_id",
    "entry_decision_setup_class",
    "label_structure_class",
    "setup_class",
    "symbol",
    "identity_symbol",
    "dataset_type",
    "identity_dataset_type",
    "phase",
    "identity_phase",
    "research_tier",
    "identity_research_tier",
    "research_mode",
    "identity_research_mode",
    "evidence_type",
    "identity_evidence_type",
    "portfolio_constraints_applied",
    "capital_path_valid",
    "deployability_evidence",
    "signal_episode_evidence",
    "identity_ts_signal",
    "ts_signal",
    "signal_ts",
    "timestamp",
    "ts",
    "ts_entry_fill",
    "entry_time",
    "entry_ts",
    "ts_exit_fill",
    "exit_time",
    "exit_ts",
    "r_net",
    "final_r_net",
    "realized_r_net",
    "pnl_r",
    "r_gross",
    "final_r_gross",
    "realized_r_gross",
    "net_pnl",
    "pnl_net",
    "realized_pnl",
    "pnl_gross",
    "gross_pnl",
    "path_mfe_r",
    "mfe_r",
    "path_mae_r",
    "mae_r",
    "counterfactual_exit_efficiency_realized_over_mfe",
    "exit_efficiency",
    "cost_drag_r",
    "counterfactual_cost_drag_r",
    "fee_drag_r",
    "counterfactual_fee_drag_r",
    "slippage_drag_r",
    "counterfactual_slippage_drag_r",
    "spread_drag_r",
    "counterfactual_spread_drag_r",
    "entry_state_csi_pctile",
    "csi_pctile",
    "entry_state_csi_source",
    "csi_source",
    "entry_state_csi_components_json",
    "csi_components_json",
    "entry_state_csi_components_available_json",
    "entry_state_vol_pctile",
    "entry_state_atr_pct_pctile",
    "vol_pctile",
    "entry_state_spread_proxy_pctile",
    "spread_pctile",
    "entry_state_tr_over_atr",
    "tr_over_atr",
    "entry_state_tr_over_atr_pctile",
    "tr_over_atr_pctile",
    "entry_state_volume_pctile",
    "volume_pctile",
    "entry_state_vol_of_vol_pctile",
    "vol_of_vol_pctile",
    "entry_state_funding_raw",
    "entry_state_funding_rate",
    "funding_raw",
    "entry_state_funding_pctile",
    "funding_pctile",
    "entry_state_funding_z",
    "funding_z",
    "entry_state_oi_level",
    "entry_state_open_interest",
    "oi_level",
    "entry_state_oi_accel",
    "oi_accel",
    "entry_state_oi_accel_pctile",
    "oi_accel_pctile",
    "entry_state_oi_z",
    "oi_z",
    "entry_state_mark_price",
    "entry_state_mark_close",
    "mark_price",
    "entry_state_index_price",
    "entry_state_index_close",
    "index_price",
    "entry_state_basis_raw",
    "entry_state_basis_close_vs_index",
    "basis_raw",
    "entry_state_basis_pct",
    "basis_pct",
    "entry_state_basis_pctile",
    "basis_pctile",
    "entry_state_premium_pctile",
    "premium_pctile",
    "entry_state_crowding_proxy_pctile",
    "crowding_proxy_pctile",
    "entry_state_constraint_stress_pctile",
    "constraint_stress_pctile",
    "trend_state",
    "entry_state_trend_state",
    "vol_regime",
    "entry_state_vol_regime",
    "liquidity_regime",
    "entry_state_liquidity_regime",
    "displacement_regime",
    "entry_state_displacement_regime",
    "market_regime_class",
    "entry_state_market_regime_class",
    "structure_class",
    "label_success_1r_before_neg_1r",
    "label_success_2r_before_neg_1r",
    "label_reached_3r",
    "label_reached_5r",
    "label_tail_trade_ge_10r",
    "entry_decision_entry_reason",
    "entry_decision_exit_reason",
}


def normalize_trade(row: dict[str, Any], *, context: dict[str, Any], row_index: int) -> dict[str, Any]:
    source_trade_id = _first(row, ["trade_id", "identity_trade_id", "source_trade_id"]) or str(row_index)
    run_id = _first(row, ["run_id", "identity_run_id"]) or context.get("run_id")
    experiment_root = str(context.get("experiment_root") or "")
    stable_id = "|".join([experiment_root, str(run_id or ""), str(source_trade_id or row_index)])
    metrics_valid = 0 if context.get("metrics_valid") is False else 1
    raw_json = _build_raw_json(row, context=context, row_index=row_index)
    rec = {
        "id": hashlib.sha1(stable_id.encode("utf-8")).hexdigest(),
        "source_trade_id": str(source_trade_id),
        "run_id": run_id,
        "hypothesis_id": _first(row, ["hypothesis_id"]) or context.get("hypothesis_id"),
        "hypothesis_name": _first(row, ["hypothesis_name", "identity_hypothesis_name"]) or context.get("hypothesis_name"),
        "strategy_id": _first(row, ["strategy_id", "identity_strategy_id"]),
        "setup_class": _first(row, ["entry_decision_setup_class", "label_structure_class", "setup_class"]),
        "symbol": _first(row, ["symbol", "identity_symbol"]),
        "dataset_type": _first(row, ["dataset_type", "identity_dataset_type"]) or context.get("dataset_type"),
        "phase": _first(row, ["phase", "identity_phase"]) or context.get("phase"),
        "research_tier": _first(row, ["research_tier", "identity_research_tier"]) or context.get("research_tier"),
        "research_mode": _first(row, ["research_mode", "identity_research_mode"]) or context.get("research_mode", "portfolio_backtest"),
        "evidence_type": _first(row, ["evidence_type", "identity_evidence_type"]) or context.get("evidence_type", "portfolio_outcome"),
        "portfolio_constraints_applied": _bool_int(
            _first(row, ["portfolio_constraints_applied"]) if _first(row, ["portfolio_constraints_applied"]) is not None else context.get("portfolio_constraints_applied", True)
        ),
        "capital_path_valid": _bool_int(
            _first(row, ["capital_path_valid"]) if _first(row, ["capital_path_valid"]) is not None else context.get("capital_path_valid", True)
        ),
        "deployability_evidence": _bool_int(
            _first(row, ["deployability_evidence"]) if _first(row, ["deployability_evidence"]) is not None else context.get("deployability_evidence", True)
        ),
        "signal_episode_evidence": _bool_int(
            _first(row, ["signal_episode_evidence"]) if _first(row, ["signal_episode_evidence"]) is not None else context.get("signal_episode_evidence", False)
        ),
        "experiment_root": experiment_root,
        "ts_signal": _first(row, ["identity_ts_signal", "ts_signal", "signal_ts", "timestamp", "ts"]),
        "ts_entry_fill": _first(row, ["ts_entry_fill", "entry_time", "entry_ts"]),
        "ts_exit_fill": _first(row, ["ts_exit_fill", "exit_time", "exit_ts"]),
        "r_net": _num(_first(row, ["r_net", "final_r_net", "realized_r_net", "pnl_r"])),
        "r_gross": _num(_first(row, ["r_gross", "final_r_gross", "realized_r_gross"])),
        "pnl_net": _num(_first(row, ["net_pnl", "pnl_net", "realized_pnl"])),
        "pnl_gross": _num(_first(row, ["pnl_gross", "gross_pnl"])),
        "mfe_r": _num(_first(row, ["path_mfe_r", "mfe_r"])),
        "mae_r": _num(_first(row, ["path_mae_r", "mae_r"])),
        "exit_efficiency": _num(_first(row, ["counterfactual_exit_efficiency_realized_over_mfe", "exit_efficiency"])),
        "cost_drag_r": _num(_first(row, ["cost_drag_r", "counterfactual_cost_drag_r"])),
        "fee_drag_r": _num(_first(row, ["fee_drag_r", "counterfactual_fee_drag_r"])),
        "slippage_drag_r": _num(_first(row, ["slippage_drag_r", "counterfactual_slippage_drag_r"])),
        "spread_drag_r": _num(_first(row, ["spread_drag_r", "counterfactual_spread_drag_r"])),
        "csi_pctile": _num(_first(row, ["entry_state_csi_pctile", "csi_pctile"])),
        "csi_source": _first(row, ["entry_state_csi_source", "csi_source"]),
        "csi_components_json": _first(row, ["entry_state_csi_components_json", "csi_components_json"]),
        "vol_pctile": _num(_first(row, ["entry_state_vol_pctile", "entry_state_atr_pct_pctile", "vol_pctile"])),
        "spread_pctile": _num(_first(row, ["entry_state_spread_proxy_pctile", "spread_pctile"])),
        "tr_over_atr": _num(_first(row, ["entry_state_tr_over_atr", "tr_over_atr"])),
        "tr_over_atr_pctile": _num(_first(row, ["entry_state_tr_over_atr_pctile", "tr_over_atr_pctile"])),
        "volume_pctile": _num(_first(row, ["entry_state_volume_pctile", "volume_pctile"])),
        "vol_of_vol_pctile": _num(_first(row, ["entry_state_vol_of_vol_pctile", "vol_of_vol_pctile"])),
        "funding_raw": _num(_first(row, ["entry_state_funding_raw", "entry_state_funding_rate", "funding_raw"])),
        "funding_pctile": _num(_first(row, ["entry_state_funding_pctile", "funding_pctile"])),
        "funding_z": _num(_first(row, ["entry_state_funding_z", "funding_z"])),
        "oi_level": _num(_first(row, ["entry_state_oi_level", "entry_state_open_interest", "oi_level"])),
        "oi_accel": _num(_first(row, ["entry_state_oi_accel", "oi_accel"])),
        "oi_accel_pctile": _num(_first(row, ["entry_state_oi_accel_pctile", "oi_accel_pctile"])),
        "oi_z": _num(_first(row, ["entry_state_oi_z", "oi_z"])),
        "mark_price": _num(_first(row, ["entry_state_mark_price", "entry_state_mark_close", "mark_price"])),
        "index_price": _num(_first(row, ["entry_state_index_price", "entry_state_index_close", "index_price"])),
        "basis_raw": _num(_first(row, ["entry_state_basis_raw", "entry_state_basis_close_vs_index", "basis_raw"])),
        "basis_pct": _num(_first(row, ["entry_state_basis_pct", "entry_state_basis_close_vs_index", "basis_pct"])),
        "basis_pctile": _num(_first(row, ["entry_state_basis_pctile", "basis_pctile"])),
        "premium_pctile": _num(_first(row, ["entry_state_premium_pctile", "premium_pctile"])),
        "crowding_proxy_pctile": _num(_first(row, ["entry_state_crowding_proxy_pctile", "crowding_proxy_pctile"])),
        "constraint_stress_pctile": _num(_first(row, ["entry_state_constraint_stress_pctile", "constraint_stress_pctile"])),
        "trend_state": _first(row, ["trend_state", "entry_state_trend_state"]),
        "vol_regime": _first(row, ["vol_regime", "entry_state_vol_regime"]),
        "liquidity_regime": _first(row, ["liquidity_regime", "entry_state_liquidity_regime"]),
        "displacement_regime": _first(row, ["displacement_regime", "entry_state_displacement_regime"]),
        "market_regime_class": _first(row, ["market_regime_class", "entry_state_market_regime_class"]),
        "structure_class": _first(row, ["structure_class", "label_structure_class"]),
        "label_success_1r_before_neg_1r": _bool_int(_first(row, ["label_success_1r_before_neg_1r"])),
        "label_success_2r_before_neg_1r": _bool_int(_first(row, ["label_success_2r_before_neg_1r"])),
        "label_reached_3r": _bool_int(_first(row, ["label_reached_3r"])),
        "label_reached_5r": _bool_int(_first(row, ["label_reached_5r"])),
        "label_tail_trade_ge_10r": _bool_int(_first(row, ["label_tail_trade_ge_10r"])),
        "label_profitable_after_costs": None,
        "label_high_cost_trade": None,
        "metrics_valid": metrics_valid,
        "invalid_reason": context.get("invalid_reason"),
        "raw_json": raw_json,
        "created_at": _now(),
    }
    if rec["r_net"] is not None:
        rec["label_profitable_after_costs"] = 1 if rec["r_net"] > 0 else 0
        rec["label_reached_3r"] = rec["label_reached_3r"] if rec["label_reached_3r"] is not None else (1 if rec["r_net"] >= 3 else 0)
        rec["label_reached_5r"] = rec["label_reached_5r"] if rec["label_reached_5r"] is not None else (1 if rec["r_net"] >= 5 else 0)
        rec["label_tail_trade_ge_10r"] = rec["label_tail_trade_ge_10r"] if rec["label_tail_trade_ge_10r"] is not None else (1 if rec["r_net"] >= 10 else 0)
    if rec["cost_drag_r"] is not None:
        rec["label_high_cost_trade"] = 1 if rec["cost_drag_r"] >= 0.5 else 0
    missing_truth = [field for field in ("source_trade_id", "run_id", "ts_signal", "pnl_net", "r_net") if rec.get(field) is None]
    if missing_truth:
        rec["metrics_valid"] = 0
        reason = f"missing required research-memory truth fields: {', '.join(missing_truth)}"
        rec["invalid_reason"] = "; ".join(part for part in (rec.get("invalid_reason"), reason) if part)
    return rec


def _build_raw_json(row: dict[str, Any], *, context: dict[str, Any], row_index: int) -> str | None:
    mode = str(context.get("raw_json_mode") or "compact").lower()
    if mode in {"0", "false", "none", "off", "disabled"}:
        return None
    if mode == "full":
        return json.dumps(_jsonable(row), sort_keys=True, default=str)
    keys = [
        "trade_id",
        "identity_trade_id",
        "run_id",
        "identity_run_id",
        "symbol",
        "identity_symbol",
        "ts_signal",
        "identity_ts_signal",
        "entry_decision_entry_reason",
        "entry_decision_exit_reason",
        "entry_state_csi_components_json",
        "entry_state_csi_components_available_json",
    ]
    compact = {
        "row_index": row_index,
        "experiment_root": context.get("experiment_root"),
        "source_dataset": "trades_dataset.parquet",
        "fields": {key: _json_value(row.get(key)) for key in keys if key in row},
    }
    return json.dumps(compact, sort_keys=True, default=str)


def insert_trades(conn, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    placeholders = ", ".join(["?"] * len(TRADE_COLUMNS))
    columns = ", ".join(TRADE_COLUMNS)
    updates = ", ".join([f"{c}=excluded.{c}" for c in TRADE_COLUMNS if c != "id"])
    sql = f"""
        INSERT INTO research_memory_trades ({columns})
        VALUES ({placeholders})
        ON CONFLICT(experiment_root, run_id, source_trade_id) DO UPDATE SET {updates}
    """
    conn.executemany(sql, ([rec.get(c) for c in TRADE_COLUMNS] for rec in records))
    return len(records)


def infer_context_from_path(path: Path, outputs_root: Path) -> dict[str, Any]:
    root = path
    name = root.name
    if root.name == "runs":
        name = root.parent.name
    phase = (
        "tier3"
        if "tier3" in root.parts
        else "tier2a"
        if "tier2a" in root.parts
        else "tier2b"
        if "tier2b" in root.parts
        else "tier2"
        if "tier2" in root.parts
        else None
    )
    if name.endswith("__tier3"):
        phase = "tier3"
    if name.endswith("__tier2"):
        phase = "tier2"
    dataset_type = "stable" if "_parallel_stable" in name else "vol" if "_parallel_vol" in name else None
    hypothesis = name.replace("_parallel_stable", "").replace("_parallel_vol", "")
    if "__" in hypothesis:
        hypothesis = root.parent.parent.name if "runs" in root.parts else hypothesis
    return {
        "experiment_root": str(root),
        "hypothesis_name": hypothesis,
        "dataset_type": dataset_type,
        "phase": phase,
        "research_tier": "tier2a" if phase == "tier2a" else "tier2b" if phase in {"tier2", "tier2b"} else phase,
        "research_mode": "signal_episode" if phase == "tier2a" else "portfolio_backtest",
        "evidence_type": "signal_outcome" if phase == "tier2a" else "portfolio_outcome",
        "portfolio_constraints_applied": phase != "tier2a",
        "capital_path_valid": phase != "tier2a",
        "deployability_evidence": phase != "tier2a",
        "signal_episode_evidence": phase == "tier2a",
        "run_id": None,
    }


def _first(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        val = row.get(key)
        if val is not None and val == val and val != "":
            return val
    return None


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        if value != value:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return 1 if value.lower() in {"1", "true", "yes", "y"} else 0 if value.lower() in {"0", "false", "no", "n"} else None
    return 1 if bool(value) else 0


def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
    return {str(k): _json_value(v) for k, v in row.items()}


def _json_value(value: Any) -> Any:
    try:
        if value != value:
            return None
    except Exception:
        pass
    return value


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
