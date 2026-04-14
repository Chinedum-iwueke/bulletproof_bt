"""L1-H11 family-specific post-run diagnostics."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path

import pandas as pd
import yaml

from bt.analytics.segment_rollups import load_trades_with_entry_metadata

DEPTH_BUCKETS = [0.0, 0.5, 1.0, 1.5, math.inf]
DEPTH_LABELS = ["0-0.5_atr", "0.5-1.0_atr", "1.0-1.5_atr", ">1.5_atr"]
IMPULSE_BUCKETS = [0.0, 1.0, 1.5, 2.0, math.inf]
IMPULSE_LABELS = ["0-1.0_atr", "1.0-1.5_atr", "1.5-2.0_atr", ">2.0_atr"]
ENTRY_POS_BUCKETS = [0.0, 0.33, 0.66, 1.0]
ENTRY_POS_LABELS = ["deep_zone", "mid_zone", "early_reclaim"]


@dataclass(frozen=True)
class _Thresholds:
    weak_impulse_atr: float = 1.0
    overdeep_pullback_atr: float = 1.5
    late_entry_pos: float = 0.33
    noise_stop_mae: float = 0.6


def _as_num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([float("nan")] * len(df), index=df.index)
    return pd.to_numeric(df[col], errors="coerce")


def _first(df: pd.DataFrame, *cols: str) -> pd.Series:
    for col in cols:
        if col in df.columns:
            return df[col]
    return pd.Series([pd.NA] * len(df), index=df.index)


def _mean(s: pd.Series) -> float | None:
    s = pd.to_numeric(s, errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _safe_div(n: pd.Series, d: pd.Series) -> pd.Series:
    out = n / d
    return out.where(d > 0)


def pullback_depth_bucket(depth_atr: pd.Series) -> pd.Series:
    return pd.cut(pd.to_numeric(depth_atr, errors="coerce"), bins=DEPTH_BUCKETS, labels=DEPTH_LABELS, include_lowest=True)


def impulse_bucket(impulse_atr: pd.Series) -> pd.Series:
    return pd.cut(pd.to_numeric(impulse_atr, errors="coerce"), bins=IMPULSE_BUCKETS, labels=IMPULSE_LABELS, include_lowest=True)


def entry_position_bucket(position_metric: pd.Series) -> pd.Series:
    return pd.cut(pd.to_numeric(position_metric, errors="coerce"), bins=ENTRY_POS_BUCKETS, labels=ENTRY_POS_LABELS, include_lowest=True)


def classify_failure_mode(row: pd.Series, *, thresholds: _Thresholds = _Thresholds()) -> str:
    impulse = pd.to_numeric(row.get("impulse_strength_atr"), errors="coerce")
    depth = pd.to_numeric(row.get("pullback_depth_atr"), errors="coerce")
    entry_pos = pd.to_numeric(row.get("entry_position_metric"), errors="coerce")
    gross = pd.to_numeric(row.get("realized_r_gross"), errors="coerce")
    net = pd.to_numeric(row.get("realized_r_net"), errors="coerce")
    mae = pd.to_numeric(row.get("mae_r"), errors="coerce")
    exit_reason = str(row.get("exit_reason", "")).lower()
    variant = str(row.get("variant_id", ""))

    if pd.notna(gross) and pd.notna(net) and gross > 0 and net < 0:
        return "cost_killed"
    if pd.notna(impulse) and impulse < thresholds.weak_impulse_atr:
        return "weak_impulse"
    if pd.notna(depth) and depth > thresholds.overdeep_pullback_atr:
        return "overdeep_pullback"
    if pd.notna(entry_pos) and entry_pos <= thresholds.late_entry_pos:
        return "late_entry"
    if exit_reason == "trend_failure":
        return "trend_failure"
    if exit_reason == "stop_loss" and pd.notna(mae) and mae < thresholds.noise_stop_mae:
        return "noise_stop"
    if variant == "L1-H11C" and exit_reason == "stop_loss":
        return "protection_too_tight"
    return "signal_noise"


def _aggregate(df: pd.DataFrame) -> dict[str, object]:
    gross = _as_num(df, "realized_r_gross")
    net = _as_num(df, "realized_r_net")
    mfe = _as_num(df, "mfe_r")
    mae = _as_num(df, "mae_r")
    capture = _as_num(df, "capture_ratio")
    wins = net[net > 0]
    losses = net[net < 0]
    avg_win = float(wins.mean()) if not wins.empty else None
    avg_loss = float(losses.mean()) if not losses.empty else None
    return {
        "n_trades": int(len(df)),
        "EV_r_net": _mean(net),
        "EV_r_gross": _mean(gross),
        "win_rate": float((net > 0).mean()) if len(df) else None,
        "avg_r_win": avg_win,
        "avg_r_loss": avg_loss,
        "payoff_ratio": (avg_win / abs(avg_loss)) if (avg_win is not None and avg_loss not in (None, 0.0)) else None,
        "avg_mfe_r": _mean(mfe),
        "avg_mae_r": _mean(mae),
        "avg_capture_ratio": _mean(capture),
    }


def _group(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for key, seg in df.groupby(cols, dropna=False, observed=False):
        vals = key if isinstance(key, tuple) else (key,)
        row = {cols[i]: vals[i] for i in range(len(cols))}
        row.update(_aggregate(seg))
        rows.append(row)
    return pd.DataFrame(rows)


def build_h11_trade_rows(experiment_root: str | Path, *, run_dirs: list[Path] | None = None) -> pd.DataFrame:
    root = Path(experiment_root)
    paths = run_dirs if run_dirs is not None else sorted((root / "runs").glob("*"))
    rows: list[pd.DataFrame] = []

    for run_dir in paths:
        if not run_dir.is_dir():
            continue
        cfg_path = run_dir / "config_used.yaml"
        if not cfg_path.exists():
            continue
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        strategy = cfg.get("strategy") if isinstance(cfg, dict) else {}
        if not isinstance(strategy, dict) or str(strategy.get("name", "")) != "l1_h11_quality_filtered_continuation":
            continue

        trades = load_trades_with_entry_metadata(run_dir)
        if trades.empty:
            continue
        trades = trades.copy()
        trades["run_id"] = _first(trades, "run_id").fillna(run_dir.name)
        trades["variant_id"] = _first(trades, "entry_meta__family_variant").replace("", pd.NA).fillna(str(strategy.get("family_variant", "L1-H11")))
        trades["signal_timeframe"] = _first(trades, "entry_meta__signal_timeframe", "entry_meta__timeframe").fillna(str(strategy.get("timeframe", "")))
        trades["setup_type"] = _first(trades, "entry_meta__setup_type")

        trades["impulse_strength_atr"] = _as_num(trades, "entry_meta__impulse_strength_atr").fillna(_as_num(trades, "entry_meta__swing_distance_atr"))
        trades["swing_distance_atr"] = _as_num(trades, "entry_meta__swing_distance_atr")
        trades["pullback_depth_atr"] = _as_num(trades, "entry_meta__pullback_depth_atr")
        trades["pull_entry_atr_low"] = _as_num(trades, "entry_meta__pull_entry_atr_low")
        trades["pull_entry_atr_high"] = _as_num(trades, "entry_meta__pull_entry_atr_high")
        trades["entry_position_metric"] = _as_num(trades, "entry_meta__entry_position_metric").fillna(_as_num(trades, "entry_meta__reclaim_position_metric"))
        trades["reclaim_position_metric"] = _as_num(trades, "entry_meta__reclaim_position_metric").fillna(trades["entry_position_metric"])
        trades["continuation_trigger_state"] = _first(trades, "entry_meta__continuation_trigger_state")
        trades["stop_distance"] = _as_num(trades, "entry_meta__stop_distance")
        trades["stop_price"] = _as_num(trades, "entry_meta__stop_price")
        trades["stop_padding_atr"] = _as_num(trades, "entry_meta__stop_padding_atr")
        trades["lock_r"] = _as_num(trades, "entry_meta__lock_r")
        trades["vwap_giveback_mode"] = _first(trades, "entry_meta__vwap_giveback_mode")

        trades["mfe_r"] = _as_num(trades, "mfe_r")
        trades["mae_r"] = _as_num(trades, "mae_r")
        trades["realized_r_gross"] = _as_num(trades, "r_multiple_gross")
        trades["realized_r_net"] = _as_num(trades, "r_multiple_net")
        trades["hold_bars"] = _as_num(trades, "hold_bars").fillna(_as_num(trades, "holding_period_bars_signal"))
        trades["capture_ratio"] = _safe_div(trades["realized_r_net"], trades["mfe_r"])
        trades["spread_cost"] = _as_num(trades, "spread_cost")
        trades["slippage_cost"] = _as_num(trades, "slippage")
        trades["fee_cost"] = _as_num(trades, "fees")
        trades["cost_drag_r"] = trades["realized_r_gross"] - trades["realized_r_net"]
        trades["exit_reason"] = _first(trades, "exit_reason")
        trades["failure_mode_label"] = trades.apply(classify_failure_mode, axis=1)
        rows.append(trades)

    if not rows:
        return pd.DataFrame()

    keep = [
        "run_id", "variant_id", "setup_type", "symbol", "signal_timeframe", "side", "entry_ts", "exit_ts",
        "impulse_strength_atr", "swing_distance_atr", "pullback_depth_atr", "pull_entry_atr_low", "pull_entry_atr_high",
        "entry_position_metric", "reclaim_position_metric", "continuation_trigger_state", "stop_distance", "stop_price",
        "stop_padding_atr", "lock_r", "vwap_giveback_mode", "mfe_r", "mae_r", "realized_r_gross", "realized_r_net",
        "capture_ratio", "spread_cost", "slippage_cost", "fee_cost", "cost_drag_r", "hold_bars", "exit_reason", "failure_mode_label",
    ]
    out = pd.concat(rows, ignore_index=True)
    for col in keep:
        if col not in out.columns:
            out[col] = pd.NA
    return out[keep]


def run_h11_postmortem(experiment_root: str | Path, *, run_dirs: list[Path] | None = None, output_root: str | Path | None = None) -> dict[str, str]:
    root = Path(experiment_root)
    out_root = Path(output_root) if output_root else root / "summaries" / "diagnostics" / "l1_h11"
    out_root.mkdir(parents=True, exist_ok=True)

    rows = build_h11_trade_rows(root, run_dirs=run_dirs)
    outputs: dict[str, str] = {}
    if rows.empty:
        return outputs

    rows["pullback_depth_bucket"] = pullback_depth_bucket(rows["pullback_depth_atr"])
    rows["impulse_bucket"] = impulse_bucket(rows["impulse_strength_atr"])
    rows["entry_position_bucket"] = entry_position_bucket(rows["entry_position_metric"])
    rows.to_csv(out_root / "h11_trade_diagnostics.csv", index=False)
    outputs["h11_trade_diagnostics"] = str(out_root / "h11_trade_diagnostics.csv")

    pd.DataFrame([_aggregate(rows)]).to_csv(out_root / "pullback_quality_summary.csv", index=False)
    _group(rows, ["pullback_depth_bucket"]).to_csv(out_root / "ev_by_pullback_depth_bucket.csv", index=False)
    _group(rows, ["signal_timeframe"]).to_csv(out_root / "pullback_quality_by_timeframe.csv", index=False)
    _group(rows, ["symbol"]).to_csv(out_root / "pullback_quality_by_symbol.csv", index=False)
    outputs.update({
        "pullback_quality_summary": str(out_root / "pullback_quality_summary.csv"),
        "ev_by_pullback_depth_bucket": str(out_root / "ev_by_pullback_depth_bucket.csv"),
        "pullback_quality_by_timeframe": str(out_root / "pullback_quality_by_timeframe.csv"),
        "pullback_quality_by_symbol": str(out_root / "pullback_quality_by_symbol.csv"),
    })

    impulse = _group(rows, ["impulse_bucket"])
    impulse["failure_rate"] = 1.0 - impulse["win_rate"]
    impulse.to_csv(out_root / "ev_by_impulse_bucket.csv", index=False)
    pd.DataFrame([_aggregate(rows)]).to_csv(out_root / "impulse_strength_summary.csv", index=False)
    _group(rows, ["signal_timeframe", "impulse_bucket"]).to_csv(out_root / "impulse_strength_by_timeframe.csv", index=False)
    outputs.update({
        "impulse_strength_summary": str(out_root / "impulse_strength_summary.csv"),
        "ev_by_impulse_bucket": str(out_root / "ev_by_impulse_bucket.csv"),
        "impulse_strength_by_timeframe": str(out_root / "impulse_strength_by_timeframe.csv"),
    })

    pd.DataFrame([_aggregate(rows)]).to_csv(out_root / "entry_position_summary.csv", index=False)
    _group(rows, ["entry_position_bucket"]).to_csv(out_root / "ev_by_entry_position_bucket.csv", index=False)
    outputs.update({
        "entry_position_summary": str(out_root / "entry_position_summary.csv"),
        "ev_by_entry_position_bucket": str(out_root / "ev_by_entry_position_bucket.csv"),
    })

    _group(rows, ["failure_mode_label"]).to_csv(out_root / "failure_mode_summary.csv", index=False)
    _group(rows, ["variant_id", "failure_mode_label"]).to_csv(out_root / "failure_mode_by_variant.csv", index=False)
    _group(rows, ["signal_timeframe", "failure_mode_label"]).to_csv(out_root / "failure_mode_by_timeframe.csv", index=False)
    outputs.update({
        "failure_mode_summary": str(out_root / "failure_mode_summary.csv"),
        "failure_mode_by_variant": str(out_root / "failure_mode_by_variant.csv"),
        "failure_mode_by_timeframe": str(out_root / "failure_mode_by_timeframe.csv"),
    })

    h11c = rows[rows["variant_id"] == "L1-H11C"]
    pd.DataFrame([_aggregate(h11c if not h11c.empty else rows)]).to_csv(out_root / "protection_discipline_summary.csv", index=False)
    _group(h11c if not h11c.empty else rows, ["lock_r"]).to_csv(out_root / "lock_rule_effect_summary.csv", index=False)
    _group(h11c if not h11c.empty else rows, ["vwap_giveback_mode"]).to_csv(out_root / "vwap_giveback_effect_summary.csv", index=False)
    outputs.update({
        "protection_discipline_summary": str(out_root / "protection_discipline_summary.csv"),
        "lock_rule_effect_summary": str(out_root / "lock_rule_effect_summary.csv"),
        "vwap_giveback_effect_summary": str(out_root / "vwap_giveback_effect_summary.csv"),
    })

    cost = pd.DataFrame([{
        "n_trades": int(len(rows)),
        "avg_realized_r_gross": _mean(rows["realized_r_gross"]),
        "avg_realized_r_net": _mean(rows["realized_r_net"]),
        "avg_cost_drag": _mean(rows["cost_drag_r"]),
        "share_gross_positive_net_negative": float(((rows["realized_r_gross"] > 0) & (rows["realized_r_net"] < 0)).mean()),
        "cost_drag_rate": float((rows["cost_drag_r"] > 0).mean()),
    }])
    cost.to_csv(out_root / "cost_kill_summary.csv", index=False)
    _group(rows, ["signal_timeframe"]).to_csv(out_root / "cost_kill_by_timeframe.csv", index=False)
    _group(rows, ["symbol"]).to_csv(out_root / "cost_kill_by_symbol.csv", index=False)
    outputs.update({
        "cost_kill_summary": str(out_root / "cost_kill_summary.csv"),
        "cost_kill_by_timeframe": str(out_root / "cost_kill_by_timeframe.csv"),
        "cost_kill_by_symbol": str(out_root / "cost_kill_by_symbol.csv"),
    })
    return outputs
