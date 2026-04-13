"""L1-H8 family-specific post-run diagnostics."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.analytics.segment_rollups import load_trades_with_entry_metadata


DEPTH_BUCKETS = [0.0, 0.5, 1.0, 1.5, math.inf]
DEPTH_LABELS = ["0-0.5_atr", "0.5-1.0_atr", "1.0-1.5_atr", ">1.5_atr"]


@dataclass(frozen=True)
class _Thresholds:
    pullback_too_deep_atr: float = 1.5
    pullback_too_long_bars: int = 3
    weak_adx: float = 20.0
    reclaim_failed_strength: float = 0.0
    stalled_mfe_r: float = 0.75
    runner_capture_floor: float = 0.35


def _as_num(frame: pd.DataFrame, col: str) -> pd.Series:
    if col not in frame.columns:
        return pd.Series([float("nan")] * len(frame), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[col], errors="coerce")


def _first_present(frame: pd.DataFrame, *cols: str) -> pd.Series:
    for col in cols:
        if col in frame.columns:
            return frame[col]
    return pd.Series([pd.NA] * len(frame), index=frame.index)


def _safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    out = numer / denom
    return out.where(denom > 0)


def pullback_depth_bucket(depth_atr: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(depth_atr, errors="coerce")
    return pd.cut(numeric, bins=DEPTH_BUCKETS, labels=DEPTH_LABELS, include_lowest=True, right=True)


def _mean(series: pd.Series) -> float | None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _aggregate_ev(frame: pd.DataFrame) -> dict[str, float | int | None]:
    gross = _as_num(frame, "realized_r_gross")
    net = _as_num(frame, "realized_r_net")
    mfe = _as_num(frame, "mfe_r")
    hold = _as_num(frame, "hold_bars")
    capture = _as_num(frame, "capture_ratio")

    wins = net[net > 0]
    losses = net[net < 0]
    avg_win = float(wins.mean()) if not wins.empty else None
    avg_loss = float(losses.mean()) if not losses.empty else None
    payoff = (avg_win / abs(avg_loss)) if (avg_win is not None and avg_loss not in (None, 0.0)) else None
    return {
        "n_trades": int(len(frame)),
        "EV_r_net": _mean(net),
        "EV_r_gross": _mean(gross),
        "win_rate": float((net > 0).mean()) if len(net) else None,
        "avg_r_win": avg_win,
        "avg_r_loss": avg_loss,
        "payoff_ratio": payoff,
        "avg_hold_bars": _mean(hold),
        "avg_mfe_r": _mean(mfe),
        "avg_capture_ratio": _mean(capture),
    }


def classify_failure_mode(row: pd.Series, *, thresholds: _Thresholds = _Thresholds()) -> str:
    adx = pd.to_numeric(row.get("adx_entry"), errors="coerce")
    depth = pd.to_numeric(row.get("pullback_depth_atr"), errors="coerce")
    bars = pd.to_numeric(row.get("pullback_bars_used"), errors="coerce")
    reclaim = pd.to_numeric(row.get("reclaim_strength"), errors="coerce")
    gross = pd.to_numeric(row.get("realized_r_gross"), errors="coerce")
    net = pd.to_numeric(row.get("realized_r_net"), errors="coerce")
    mfe = pd.to_numeric(row.get("mfe_r"), errors="coerce")
    capture = pd.to_numeric(row.get("capture_ratio"), errors="coerce")
    tp1_hit = bool(row.get("tp1_hit", False))

    if pd.notna(adx) and adx < thresholds.weak_adx:
        return "trend_filter_weak"
    if pd.notna(depth) and depth > thresholds.pullback_too_deep_atr:
        return "pullback_too_deep"
    if pd.notna(bars) and bars > thresholds.pullback_too_long_bars:
        return "pullback_too_long"
    if pd.notna(reclaim) and reclaim <= thresholds.reclaim_failed_strength:
        return "reclaim_failed"
    if pd.notna(gross) and pd.notna(net) and gross > 0 and net < 0:
        return "cost_killed"
    if tp1_hit and pd.notna(capture) and capture < thresholds.runner_capture_floor:
        return "runner_gave_back"
    if pd.notna(net) and net <= 0 and pd.notna(mfe) and mfe < thresholds.stalled_mfe_r:
        return "continuation_stalled"
    return "signal_noise"


def build_h8_trade_diagnostic_rows(experiment_root: str | Path, *, run_dirs: list[Path] | None = None) -> pd.DataFrame:
    root = Path(experiment_root)
    paths = run_dirs if run_dirs is not None else sorted((root / "runs").glob("*"))
    rows: list[pd.DataFrame] = []

    for run_dir in paths:
        if not run_dir.is_dir():
            continue
        config_path = run_dir / "config_used.yaml"
        if not config_path.exists():
            continue
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        strategy = cfg.get("strategy") if isinstance(cfg, dict) else {}
        if not isinstance(strategy, dict) or str(strategy.get("name", "")) != "l1_h8_trend_continuation_pullback":
            continue

        trades = load_trades_with_entry_metadata(run_dir)
        if trades.empty:
            continue
        trades = trades.copy()
        trades["run_id"] = _first_present(trades, "run_id").fillna(run_dir.name)
        trades["run_dir"] = str(run_dir)
        trades["hypothesis_id"] = _first_present(trades, "hypothesis_id").replace("", pd.NA).fillna(str(strategy.get("family_variant", "L1-H8")))
        trades["variant_id"] = _first_present(trades, "entry_meta__family_variant").replace("", pd.NA).fillna(str(strategy.get("family_variant", "L1-H8")))
        trades["signal_timeframe"] = _first_present(trades, "entry_meta__signal_timeframe", "entry_meta__timeframe").fillna(str(strategy.get("timeframe", "")))
        trades["pullback_bars_used"] = _as_num(trades, "entry_meta__pullback_bars_used").fillna(_as_num(trades, "entry_meta__pullback_bars"))
        trades["pullback_depth_atr"] = _as_num(trades, "entry_meta__pullback_depth_atr")
        trades["pullback_depth_pct_of_prior_leg"] = _as_num(trades, "entry_meta__pullback_depth_pct_of_prior_leg")
        trades["pullback_reference_mode"] = _first_present(trades, "entry_meta__pullback_reference_mode")
        trades["pullback_reference_hit"] = _first_present(trades, "entry_meta__pullback_reference_hit", "entry_meta__reference_hit")
        trades["adx_entry"] = _as_num(trades, "entry_meta__adx_entry").fillna(_as_num(trades, "entry_meta__adx"))
        trades["ema_fast_entry"] = _as_num(trades, "entry_meta__ema_fast_entry").fillna(_as_num(trades, "entry_meta__ema_fast"))
        trades["ema_slow_entry"] = _as_num(trades, "entry_meta__ema_slow_entry").fillna(_as_num(trades, "entry_meta__ema_slow"))
        trades["session_vwap_entry"] = _as_num(trades, "entry_meta__session_vwap")
        trades["reclaim_strength"] = _as_num(trades, "entry_meta__reclaim_strength")
        trades["continuation_trigger_state"] = _first_present(trades, "entry_meta__continuation_trigger_state", "entry_meta__continuation_trigger")
        trades["runner_mode"] = _first_present(trades, "entry_meta__runner_mode")
        trades["fail_fast_bars"] = _as_num(trades, "entry_meta__fail_fast_bars")
        trades["trail_atr_mult"] = _as_num(trades, "entry_meta__trail_atr_mult")
        trades["stop_distance"] = _as_num(trades, "entry_meta__stop_distance")
        trades["stop_price"] = _as_num(trades, "entry_meta__stop_price")
        trades["entry_reference_price"] = _as_num(trades, "entry_meta__entry_reference_price")
        trades["tp1_at_r"] = _as_num(trades, "entry_meta__tp1_at_r").fillna(_as_num(trades, "entry_meta__partial_at_r"))

        trades["tp1_hit"] = False
        if "max_unrealized_profit_r" in trades.columns:
            p = _as_num(trades, "max_unrealized_profit_r")
            at_r = _as_num(trades, "entry_meta__partial_at_r").fillna(1.5)
            trades["tp1_hit"] = (p >= at_r).fillna(False)
        elif "touched_1r_before_exit" in trades.columns:
            trades["tp1_hit"] = trades["touched_1r_before_exit"].astype(bool)

        trades["time_to_tp1_bars"] = _as_num(trades, "time_to_tp1_bars_signal")
        trades["time_to_tp1_bars"] = trades["time_to_tp1_bars"].fillna(_as_num(trades, "time_to_mfe_bars_signal").where(trades["tp1_hit"], pd.NA))
        trades["time_to_mfe_peak_bars"] = _as_num(trades, "time_to_mfe_bars_signal")
        trades["hold_bars"] = _as_num(trades, "hold_bars").fillna(_as_num(trades, "holding_period_bars_signal"))
        trades["mfe_r"] = _as_num(trades, "mfe_r")
        trades["mae_r"] = _as_num(trades, "mae_r")
        trades["realized_r_gross"] = _as_num(trades, "r_multiple_gross")
        trades["realized_r_net"] = _as_num(trades, "r_multiple_net")
        trades["capture_ratio"] = _safe_div(trades["realized_r_net"], trades["mfe_r"])

        trades["spread_cost"] = _as_num(trades, "spread_cost")
        trades["slippage_cost"] = _as_num(trades, "slippage")
        trades["fee_cost"] = _as_num(trades, "fees")
        trades["cost_drag_r"] = trades["realized_r_gross"] - trades["realized_r_net"]
        trades["continuation_extension_atr"] = trades["mfe_r"]
        trades["continuation_leg_vs_pullback_ratio"] = _safe_div(trades["mfe_r"], trades["pullback_depth_atr"])
        trades["failed_immediate_continuation_flag"] = _first_present(trades, "failed_immediate_continuation_flag").astype("boolean")
        trades["failed_immediate_continuation_flag"] = trades["failed_immediate_continuation_flag"].fillna((trades["mfe_r"] < 0.5).fillna(True))
        trades["failure_mode_label"] = trades.apply(classify_failure_mode, axis=1)
        rows.append(trades)

    if not rows:
        return pd.DataFrame()

    combined = pd.concat(rows, ignore_index=True)
    keep = [
        "run_id",
        "hypothesis_id",
        "variant_id",
        "symbol",
        "signal_timeframe",
        "side",
        "entry_ts",
        "exit_ts",
        "pullback_bars_used",
        "pullback_depth_atr",
        "pullback_depth_pct_of_prior_leg",
        "pullback_reference_mode",
        "pullback_reference_hit",
        "adx_entry",
        "ema_fast_entry",
        "ema_slow_entry",
        "session_vwap_entry",
        "reclaim_strength",
        "continuation_trigger_state",
        "runner_mode",
        "fail_fast_bars",
        "trail_atr_mult",
        "stop_distance",
        "stop_price",
        "entry_reference_price",
        "tp1_at_r",
        "tp1_hit",
        "time_to_tp1_bars",
        "time_to_mfe_peak_bars",
        "mfe_r",
        "mae_r",
        "realized_r_gross",
        "realized_r_net",
        "capture_ratio",
        "spread_cost",
        "slippage_cost",
        "fee_cost",
        "continuation_extension_atr",
        "continuation_leg_vs_pullback_ratio",
        "failed_immediate_continuation_flag",
        "cost_drag_r",
        "failure_mode_label",
        "hold_bars",
        "run_dir",
    ]
    for col in keep:
        if col not in combined.columns:
            combined[col] = pd.NA
    return combined[keep]


def _summary_by_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    grouped = []
    for key, segment in df.groupby(group_cols, dropna=False, observed=False):
        key_vals = key if isinstance(key, tuple) else (key,)
        payload = {group_cols[i]: key_vals[i] for i in range(len(group_cols))}
        payload.update(_aggregate_ev(segment))
        grouped.append(payload)
    return pd.DataFrame(grouped)


def run_h8_postmortem(experiment_root: str | Path, *, run_dirs: list[Path] | None = None, output_root: str | Path | None = None) -> dict[str, str]:
    root = Path(experiment_root)
    out_root = Path(output_root) if output_root else root / "summaries" / "diagnostics" / "l1_h8"
    out_root.mkdir(parents=True, exist_ok=True)

    rows = build_h8_trade_diagnostic_rows(root, run_dirs=run_dirs)
    outputs: dict[str, str] = {}
    if rows.empty:
        return outputs

    rows["pullback_depth_bucket"] = pullback_depth_bucket(rows["pullback_depth_atr"])
    rows["avg_cost_drag"] = rows["cost_drag_r"]

    path = out_root / "h8_trade_diagnostics.csv"
    rows.to_csv(path, index=False)
    outputs["h8_trade_diagnostics"] = str(path)

    pullback_summary = pd.DataFrame([_aggregate_ev(rows)])
    pullback_summary.to_csv(out_root / "pullback_quality_summary.csv", index=False)
    outputs["pullback_quality_summary"] = str(out_root / "pullback_quality_summary.csv")

    ev_depth = _summary_by_group(rows, ["pullback_depth_bucket"])
    ev_depth.to_csv(out_root / "ev_by_pullback_depth_bucket.csv", index=False)
    outputs["ev_by_pullback_depth_bucket"] = str(out_root / "ev_by_pullback_depth_bucket.csv")

    ev_bars = _summary_by_group(rows, ["pullback_bars_used"])
    ev_bars.to_csv(out_root / "ev_by_pullback_bars.csv", index=False)
    outputs["ev_by_pullback_bars"] = str(out_root / "ev_by_pullback_bars.csv")

    ev_ref = _summary_by_group(rows, ["pullback_reference_mode"])
    ev_ref.to_csv(out_root / "ev_by_reference_mode.csv", index=False)
    outputs["ev_by_reference_mode"] = str(out_root / "ev_by_reference_mode.csv")

    cont_summary = pd.DataFrame(
        [
            {
                "n_trades": int(len(rows)),
                "tp1_hit_rate": float(rows["tp1_hit"].mean()) if len(rows) else None,
                "avg_mfe_r": _mean(rows["mfe_r"]),
                "avg_realized_r_net": _mean(rows["realized_r_net"]),
                "avg_capture_ratio": _mean(rows["capture_ratio"]),
                "avg_time_to_tp1_bars": _mean(rows["time_to_tp1_bars"]),
                "avg_time_to_mfe_peak_bars": _mean(rows["time_to_mfe_peak_bars"]),
                "failed_immediate_continuation_rate": float(rows["failed_immediate_continuation_flag"].mean()),
                "avg_continuation_extension_atr": _mean(rows["continuation_extension_atr"]),
                "avg_continuation_leg_vs_pullback_ratio": _mean(rows["continuation_leg_vs_pullback_ratio"]),
            }
        ]
    )
    cont_summary.to_csv(out_root / "continuation_strength_summary.csv", index=False)
    outputs["continuation_strength_summary"] = str(out_root / "continuation_strength_summary.csv")

    by_tf = _summary_by_group(rows, ["signal_timeframe"])
    by_tf["tp1_hit_rate"] = rows.groupby("signal_timeframe", dropna=False, observed=False)["tp1_hit"].mean().values
    by_tf.to_csv(out_root / "continuation_strength_by_timeframe.csv", index=False)
    outputs["continuation_strength_by_timeframe"] = str(out_root / "continuation_strength_by_timeframe.csv")

    by_symbol = _summary_by_group(rows, ["symbol"])
    by_symbol["tp1_hit_rate"] = rows.groupby("symbol", dropna=False, observed=False)["tp1_hit"].mean().values
    by_symbol.to_csv(out_root / "continuation_strength_by_symbol.csv", index=False)
    outputs["continuation_strength_by_symbol"] = str(out_root / "continuation_strength_by_symbol.csv")

    fail = _summary_by_group(rows, ["failure_mode_label"])
    fail["avg_cost_drag"] = rows.groupby("failure_mode_label", dropna=False, observed=False)["cost_drag_r"].mean().values
    fail.to_csv(out_root / "failure_mode_summary.csv", index=False)
    outputs["failure_mode_summary"] = str(out_root / "failure_mode_summary.csv")

    fail_var = _summary_by_group(rows, ["variant_id", "failure_mode_label"])
    fail_var["avg_cost_drag"] = rows.groupby(["variant_id", "failure_mode_label"], dropna=False, observed=False)["cost_drag_r"].mean().values
    fail_var.to_csv(out_root / "failure_mode_by_variant.csv", index=False)
    outputs["failure_mode_by_variant"] = str(out_root / "failure_mode_by_variant.csv")

    fail_tf = _summary_by_group(rows, ["signal_timeframe", "failure_mode_label"])
    fail_tf["avg_cost_drag"] = rows.groupby(["signal_timeframe", "failure_mode_label"], dropna=False, observed=False)["cost_drag_r"].mean().values
    fail_tf.to_csv(out_root / "failure_mode_by_timeframe.csv", index=False)
    outputs["failure_mode_by_timeframe"] = str(out_root / "failure_mode_by_timeframe.csv")

    top_n = max(1, int(math.ceil(len(rows) * 0.1)))
    top_share = _as_num(rows.sort_values("realized_r_net", ascending=False).head(top_n), "realized_r_net").sum()
    total_share = _as_num(rows, "realized_r_net").sum()
    share_top = (float(top_share / total_share) if total_share else None)

    runner = pd.DataFrame(
        [
            {
                "n_trades": int(len(rows)),
                "tp1_hit_rate": float(rows["tp1_hit"].mean()) if len(rows) else None,
                "runner_survival_rate": float((rows["tp1_hit"] & (rows["realized_r_net"] > 0)).mean()),
                "avg_runner_r": _mean(rows.loc[rows["tp1_hit"], "realized_r_net"]),
                "runner_giveback_r": _mean(rows.loc[rows["tp1_hit"], "mfe_r"] - rows.loc[rows["tp1_hit"], "realized_r_net"]),
                "capture_after_tp1": _mean(rows.loc[rows["tp1_hit"], "capture_ratio"]),
                "share_of_total_pnl_from_top_10pct_trades": share_top,
            }
        ]
    )
    runner.to_csv(out_root / "runner_capture_summary.csv", index=False)
    outputs["runner_capture_summary"] = str(out_root / "runner_capture_summary.csv")

    runner_rows: list[dict[str, Any]] = []
    for variant, group in rows.groupby("variant_id", dropna=False, observed=False):
        runner_rows.append(
            {
                "variant_id": variant,
                "n_trades": int(len(group)),
                "tp1_hit_rate": float(group["tp1_hit"].mean()) if len(group) else None,
                "runner_survival_rate": float((group["tp1_hit"] & (group["realized_r_net"] > 0)).mean()),
                "avg_runner_r": _mean(group.loc[group["tp1_hit"], "realized_r_net"]),
                "runner_giveback_r": _mean(group.loc[group["tp1_hit"], "mfe_r"] - group.loc[group["tp1_hit"], "realized_r_net"]),
                "capture_after_tp1": _mean(group.loc[group["tp1_hit"], "capture_ratio"]),
            }
        )
    runner_var = pd.DataFrame(runner_rows)
    runner_var.to_csv(out_root / "runner_capture_by_variant.csv", index=False)
    outputs["runner_capture_by_variant"] = str(out_root / "runner_capture_by_variant.csv")

    cost_summary = pd.DataFrame(
        [
            {
                "n_trades": int(len(rows)),
                "avg_realized_r_gross": _mean(rows["realized_r_gross"]),
                "avg_realized_r_net": _mean(rows["realized_r_net"]),
                "avg_cost_drag": _mean(rows["cost_drag_r"]),
                "share_of_trades_gross_positive_but_net_negative": float(((rows["realized_r_gross"] > 0) & (rows["realized_r_net"] < 0)).mean()),
                "cost_drag_rate": float((rows["cost_drag_r"] > 0).mean()),
            }
        ]
    )
    cost_summary.to_csv(out_root / "cost_kill_summary.csv", index=False)
    outputs["cost_kill_summary"] = str(out_root / "cost_kill_summary.csv")

    cost_tf_rows: list[dict[str, Any]] = []
    for timeframe, group in rows.groupby("signal_timeframe", dropna=False, observed=False):
        cost_tf_rows.append(
            {
                "signal_timeframe": timeframe,
                "n_trades": int(len(group)),
                "avg_realized_r_gross": _mean(group["realized_r_gross"]),
                "avg_realized_r_net": _mean(group["realized_r_net"]),
                "avg_cost_drag": _mean(group["cost_drag_r"]),
                "share_of_trades_gross_positive_but_net_negative": float(((group["realized_r_gross"] > 0) & (group["realized_r_net"] < 0)).mean()),
                "cost_drag_rate": float((group["cost_drag_r"] > 0).mean()),
            }
        )
    cost_tf = pd.DataFrame(cost_tf_rows)
    cost_tf.to_csv(out_root / "cost_kill_by_timeframe.csv", index=False)
    outputs["cost_kill_by_timeframe"] = str(out_root / "cost_kill_by_timeframe.csv")

    cost_symbol_rows: list[dict[str, Any]] = []
    for symbol, group in rows.groupby("symbol", dropna=False, observed=False):
        cost_symbol_rows.append(
            {
                "symbol": symbol,
                "n_trades": int(len(group)),
                "avg_realized_r_gross": _mean(group["realized_r_gross"]),
                "avg_realized_r_net": _mean(group["realized_r_net"]),
                "avg_cost_drag": _mean(group["cost_drag_r"]),
                "share_of_trades_gross_positive_but_net_negative": float(((group["realized_r_gross"] > 0) & (group["realized_r_net"] < 0)).mean()),
                "cost_drag_rate": float((group["cost_drag_r"] > 0).mean()),
            }
        )
    cost_symbol = pd.DataFrame(cost_symbol_rows)
    cost_symbol.to_csv(out_root / "cost_kill_by_symbol.csv", index=False)
    outputs["cost_kill_by_symbol"] = str(out_root / "cost_kill_by_symbol.csv")

    return outputs
