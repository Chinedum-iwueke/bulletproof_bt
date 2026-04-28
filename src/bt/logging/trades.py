"""Trade lifecycle logging utilities."""
from __future__ import annotations

import csv
import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.data.config_utils import parse_date_range
from bt.logging.formatting import FLOAT_DECIMALS_CSV, write_json_deterministic
from bt.logging.decision_trace import flatten_decision_trace
from bt.logging.trade_enrichment import enrich_trade_row
from bt.data.dataset import load_dataset_manifest
from bt.core.types import Trade
from bt.risk.r_multiple import compute_r_multiple


def make_run_id(prefix: str = "run") -> str:
    """Return e.g. run_20260117_130501 (UTC)."""
    now = dt.datetime.now(dt.timezone.utc)
    return f"{prefix}_{now:%Y%m%d_%H%M%S}"


def prepare_run_dir(base_dir: Path, run_id: str) -> Path:
    """Create outputs/runs/<run_id>/ and return path."""
    run_dir = base_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def write_config_used(run_dir: Path, config: dict[str, Any]) -> None:
    """Write config_used.yaml."""
    path = run_dir / "config_used.yaml"
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(config, handle, sort_keys=False)




def _normalize_requested_symbols(value: Any, *, key_path: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")

    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")
        symbol = item.strip()
        if not symbol:
            continue
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)

    if not normalized:
        raise ValueError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")

    return normalized


def _resolve_requested_symbols(data_cfg: dict[str, Any]) -> tuple[list[str] | None, Any, Any]:
    requested_subset_raw = data_cfg.get("symbols_subset")
    requested_symbols_raw = data_cfg.get("symbols")

    normalized_subset = (
        None
        if requested_subset_raw is None
        else _normalize_requested_symbols(requested_subset_raw, key_path="data.symbols_subset")
    )
    normalized_symbols = (
        None
        if requested_symbols_raw is None
        else _normalize_requested_symbols(requested_symbols_raw, key_path="data.symbols")
    )

    if normalized_subset is None and normalized_symbols is None:
        return None, requested_subset_raw, requested_symbols_raw
    if normalized_subset is not None and normalized_symbols is not None and normalized_subset != normalized_symbols:
        raise ValueError(
            "Config conflict: data.symbols and data.symbols_subset both set but differ. "
            f"Use only one. data.symbols={requested_symbols_raw!r} data.symbols_subset={requested_subset_raw!r}"
        )
    return (normalized_subset if normalized_subset is not None else normalized_symbols), requested_subset_raw, requested_symbols_raw

def write_data_scope(run_dir: Path, *, config: dict, dataset_dir: str | None = None) -> None:
    """
    Write data_scope.json into run_dir if any scope-reducing knobs are active.
    This is metadata only: does not affect engine results.

    Knobs considered "scope-reducing":
      - data.symbols_subset
      - data.max_symbols
      - data.date_range
      - data.row_limit_per_symbol

    chunksize is NOT scope-reducing (perf-only) and should not trigger writing.
    """
    data_cfg = config.get("data", {}) if isinstance(config, dict) else {}
    if not isinstance(data_cfg, dict):
        data_cfg = {}

    requested_symbols, requested_subset_raw, requested_symbols_raw = _resolve_requested_symbols(data_cfg)
    requested_max_symbols = data_cfg.get("max_symbols")
    requested_date_range = data_cfg.get("date_range")
    requested_row_limit = data_cfg.get("row_limit_per_symbol")

    has_scope_knob = any(
        (
            requested_symbols is not None,
            requested_max_symbols is not None,
            requested_date_range not in (None, {}),
            requested_row_limit is not None,
        )
    )
    if not has_scope_knob:
        return

    payload: dict[str, Any] = {}
    if "mode" in data_cfg:
        payload["mode"] = data_cfg.get("mode")
    if requested_symbols is not None:
        payload["requested_symbols"] = requested_symbols
        if requested_subset_raw is not None:
            payload["requested_symbols_subset"] = requested_subset_raw
        elif requested_symbols_raw is not None:
            payload["requested_symbols_subset"] = requested_symbols_raw
    if requested_max_symbols is not None:
        payload["requested_max_symbols"] = requested_max_symbols
    if requested_row_limit is not None:
        payload["row_limit_per_symbol"] = requested_row_limit

    parsed_range = parse_date_range(config)
    if parsed_range is not None:
        payload["date_range"] = {
            "start": parsed_range[0].isoformat(),
            "end": parsed_range[1].isoformat(),
        }

    if dataset_dir is not None:
        try:
            manifest = load_dataset_manifest(dataset_dir, config)
        except Exception as exc:
            raise ValueError(
                f"Failed to compute effective symbols for dataset_dir='{dataset_dir}': {exc}"
            ) from exc
        payload["effective_symbols"] = manifest.symbols
        payload["effective_symbol_count"] = len(manifest.symbols)

    path = run_dir / "data_scope.json"
    write_json_deterministic(path, payload)


class TradesCsvWriter:
    _columns = [
        "identity_run_id",
        "identity_trade_id",
        "identity_hypothesis_id",
        "identity_strategy_id",
        "identity_parameter_set_id",
        "identity_symbol",
        "identity_dataset_id",
        "identity_tier",
        "identity_ts_signal",
        "identity_ts_entry_fill",
        "identity_ts_exit_fill",
        "identity_signal_timeframe",
        "identity_execution_timeframe",
        "run_id",
        "hypothesis_id",
        "entry_ts",
        "exit_ts",
        "symbol",
        "side",
        "qty",
        "entry_qty",
        "exit_qty",
        "entry_price",
        "exit_price",
        "pnl",
        "pnl_price",
        "fees_paid",
        "pnl_net",
        "fees",
        "slippage",
        "mae_price",
        "mfe_price",
        "initial_stop_px",
        "initial_stop_distance_px",
        "initial_stop_distance_r_denom",
        "risk_amount",
        "stop_distance",
        "entry_stop_distance",
        "r_multiple_gross",
        "r_multiple_net",
        "realized_r_gross",
        "realized_r_net",
        "mfe_r",
        "mae_r",
        "time_to_mfe_minutes",
        "holding_period_minutes",
        "holding_period_bars_signal",
        "time_to_mfe_bars_signal",
        "exit_reason",
        "whether_trail_activated",
        "trail_activation_mode",
        "bars_until_trail_activation",
        "profit_r_at_trail_activation",
        "max_unrealized_profit_r",
        "max_unrealized_loss_r",
        "reached_1r",
        "reached_2r",
        "reached_3r",
        "touched_vwap_before_exit",
        "touched_1r_before_exit",
        "touched_2r_before_exit",
        "touched_3r_before_exit",
        "entry_decision_reason_code",
        "entry_decision_setup_class",
        "entry_decision_hypothesis_branch",
        "entry_decision_parameter_combination",
        "entry_decision_conditions_json",
        "entry_decision_blockers_json",
        "entry_decision_permission_json",
        "entry_decision_score",
        "entry_decision_rank",
        "entry_decision_most_binding_gate",
        "entry_decision_gate_margins_json",
        "entry_gate_thresholds_json",
        "entry_gate_values_json",
        "execution_intended_entry_price",
        "execution_actual_entry_price",
        "execution_actual_exit_price",
        "execution_entry_order_type",
        "execution_exit_order_type",
        "execution_stop_price_initial",
        "execution_take_profit_price_initial",
        "execution_trailing_stop_initial",
        "execution_delay_bars",
        "execution_spread_paid",
        "execution_slippage_paid",
        "execution_fees_paid",
        "execution_partial_fill_flag",
        "execution_intrabar_assumption",
        "risk_amount",
        "risk_stop_distance",
        "risk_qty",
        "risk_initial_stop_r",
        "r_gross",
        "r_net",
        "path_mfe_r",
        "path_mae_r",
        "path_bars_held",
        "path_holding_time_minutes",
        "path_touched_2r",
        "path_touched_3r",
        "path_touched_5r",
        "path_touched_10r",
        "counterfactual_exit_efficiency_realized_over_mfe",
        "label_reached_3r",
        "label_reached_5r",
        "label_profitable_after_costs",
        "label_entry_quality_bucket",
        "label_exit_efficiency_bucket",
        "label_structure_class",
        "label_market_regime_class",
        "cost_drag_r",
    ]

    def __init__(self, path: Path, *, run_id: str | None = None, hypothesis_id: str | None = None):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._run_id = run_id
        self._hypothesis_id = hypothesis_id
        file_exists = path.exists()
        self._file = path.open("a", encoding="utf-8", newline="")
        self._writer = csv.writer(self._file)
        if not file_exists or path.stat().st_size == 0:
            self._writer.writerow(self._columns)
            self._file.flush()

    def _serialize_value(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, Enum):
            return value.name
        if isinstance(value, float):
            return f"{value:.{FLOAT_DECIMALS_CSV}f}"
        return str(value)

    def write_trade(self, trade: Trade) -> None:
        """Append one trade row."""
        metadata = trade.metadata if isinstance(trade.metadata, dict) else {}
        risk_amount = metadata.get("risk_amount")
        stop_distance = metadata.get("stop_distance")
        entry_qty = metadata.get("entry_qty", trade.qty)
        entry_stop_distance = metadata.get("entry_stop_distance", stop_distance)
        entry_stop_price = metadata.get("entry_stop_price")
        path_favorable_price = metadata.get("path_favorable_price", trade.mfe_price)
        path_adverse_price = metadata.get("path_adverse_price", trade.mae_price)

        pnl_price = trade.pnl
        fees_paid = trade.fees
        pnl_net = pnl_price - fees_paid
        realized_r_gross = compute_r_multiple(pnl_price, risk_amount)
        realized_r_net = compute_r_multiple(pnl_net, risk_amount)

        mfe_r = None
        mae_r = None
        try:
            stop_dist = float(entry_stop_distance) if entry_stop_distance is not None else None
        except (TypeError, ValueError):
            stop_dist = None
        if stop_dist is not None and stop_dist > 0:
            if trade.side.name == "BUY":
                if path_favorable_price is not None:
                    mfe_r = (float(path_favorable_price) - float(trade.entry_price)) / stop_dist
                if path_adverse_price is not None:
                    mae_r = (float(trade.entry_price) - float(path_adverse_price)) / stop_dist
            else:
                if path_favorable_price is not None:
                    mfe_r = (float(trade.entry_price) - float(path_favorable_price)) / stop_dist
                if path_adverse_price is not None:
                    mae_r = (float(path_adverse_price) - float(trade.entry_price)) / stop_dist

        entry_ts = trade.entry_ts
        exit_ts = trade.exit_ts
        holding_period_minutes = (exit_ts - entry_ts).total_seconds() / 60.0
        time_to_mfe_minutes = None
        mfe_ts = metadata.get("path_favorable_ts")
        if isinstance(mfe_ts, pd.Timestamp):
            time_to_mfe_minutes = (mfe_ts - entry_ts).total_seconds() / 60.0
        elif isinstance(mfe_ts, str):
            parsed_mfe_ts = pd.to_datetime(mfe_ts, utc=True, errors="coerce")
            if pd.notna(parsed_mfe_ts):
                time_to_mfe_minutes = (parsed_mfe_ts - entry_ts).total_seconds() / 60.0

        computed_values: dict[str, Any] = {
            "identity_run_id": self._run_id,
            "identity_trade_id": metadata.get("trade_id"),
            "identity_hypothesis_id": self._hypothesis_id,
            "identity_strategy_id": metadata.get("strategy_id"),
            "identity_parameter_set_id": metadata.get("parameter_set_id"),
            "identity_symbol": trade.symbol,
            "identity_dataset_id": metadata.get("dataset_id"),
            "identity_tier": metadata.get("tier"),
            "identity_ts_signal": metadata.get("signal_ts", trade.entry_ts),
            "identity_ts_entry_fill": trade.entry_ts,
            "identity_ts_exit_fill": trade.exit_ts,
            "identity_signal_timeframe": metadata.get("signal_timeframe"),
            "identity_execution_timeframe": metadata.get("execution_timeframe", metadata.get("exit_monitoring_timeframe")),
            "run_id": self._run_id,
            "hypothesis_id": self._hypothesis_id,
            "pnl_price": pnl_price,
            "fees_paid": fees_paid,
            "pnl_net": pnl_net,
            "risk_amount": risk_amount,
            "stop_distance": stop_distance,
            "initial_stop_px": entry_stop_price,
            "initial_stop_distance_px": entry_stop_distance,
            "initial_stop_distance_r_denom": entry_stop_distance,
            "entry_qty": entry_qty,
            "exit_qty": trade.qty,
            "entry_stop_distance": entry_stop_distance,
            "r_multiple_gross": realized_r_gross,
            "r_multiple_net": realized_r_net,
            "realized_r_gross": realized_r_gross,
            "realized_r_net": realized_r_net,
            "r_gross": realized_r_gross,
            "r_net": realized_r_net,
            "mfe_r": mfe_r,
            "mae_r": mae_r,
            "path_mfe_r": mfe_r,
            "path_mae_r": mae_r,
            "time_to_mfe_minutes": time_to_mfe_minutes,
            "holding_period_minutes": holding_period_minutes,
            "holding_period_bars_signal": metadata.get("holding_period_bars_signal"),
            "path_bars_held": metadata.get("holding_period_bars_signal"),
            "path_holding_time_minutes": holding_period_minutes,
            "time_to_mfe_bars_signal": metadata.get("time_to_mfe_bars_signal"),
            "exit_reason": metadata.get("exit_reason"),
            "whether_trail_activated": metadata.get("trail_activated"),
            "trail_activation_mode": metadata.get("trail_activation_mode"),
            "bars_until_trail_activation": metadata.get("bars_until_trail_activation"),
            "profit_r_at_trail_activation": metadata.get("profit_r_at_trail_activation"),
            "max_unrealized_profit_r": metadata.get("max_unrealized_profit_r", mfe_r),
            "max_unrealized_loss_r": metadata.get("max_unrealized_loss_r", mae_r),
            "reached_1r": bool(mfe_r is not None and mfe_r >= 1.0),
            "reached_2r": bool(mfe_r is not None and mfe_r >= 2.0),
            "reached_3r": bool(mfe_r is not None and mfe_r >= 3.0),
            "touched_vwap_before_exit": metadata.get("touched_vwap_before_exit"),
            "touched_1r_before_exit": bool(mfe_r is not None and mfe_r >= 1.0),
            "touched_2r_before_exit": bool(mfe_r is not None and mfe_r >= 2.0),
            "touched_3r_before_exit": bool(mfe_r is not None and mfe_r >= 3.0),
            "execution_intended_entry_price": metadata.get("intended_entry_price", trade.entry_price),
            "execution_actual_entry_price": trade.entry_price,
            "execution_actual_exit_price": trade.exit_price,
            "execution_entry_order_type": metadata.get("entry_order_type"),
            "execution_exit_order_type": metadata.get("exit_order_type"),
            "execution_stop_price_initial": metadata.get("entry_stop_price"),
            "execution_take_profit_price_initial": metadata.get("take_profit_price"),
            "execution_trailing_stop_initial": metadata.get("trailing_stop_initial"),
            "execution_delay_bars": metadata.get("delay_bars", metadata.get("delay_remaining")),
            "execution_spread_paid": metadata.get("spread_paid"),
            "execution_slippage_paid": trade.slippage,
            "execution_fees_paid": trade.fees,
            "execution_partial_fill_flag": metadata.get("partial_fill_flag"),
            "execution_intrabar_assumption": metadata.get("intrabar_assumption"),
            "risk_stop_distance": entry_stop_distance,
            "risk_qty": entry_qty,
            "risk_initial_stop_r": 1.0 if entry_stop_distance else None,
        }
        computed_values.update(flatten_decision_trace(metadata.get("decision_trace")))
        for key, value in metadata.items():
            if key.startswith(("entry_state_", "entry_gate_", "entry_decision_", "execution_", "risk_", "path_", "counterfactual_", "label_", "identity_")):
                computed_values[key] = value
                if key not in self._columns:
                    self._columns.append(key)
        computed_values = enrich_trade_row(computed_values)

        row: list[str] = []
        for column in self._columns:
            if column in computed_values:
                value = computed_values[column]
            else:
                value = getattr(trade, column, "")  # TODO: populate when Trade adds field.
            row.append(self._serialize_value(value))
        self._writer.writerow(row)
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
