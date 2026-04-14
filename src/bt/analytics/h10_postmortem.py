"""L1-H10 family diagnostics for high-win-rate/tight-TP systems."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from bt.analytics.segment_rollups import load_trades_with_entry_metadata

TAIL_EXTENSION_BINS = [0.0, 0.5, 1.0, 1.5, 2.0, float("inf")]
TAIL_EXTENSION_LABELS = ["0-0.5R", "0.5-1.0R", "1.0-1.5R", "1.5-2.0R", ">2.0R"]
TAIL_FLAG_THRESHOLD_R = 2.0


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


def _safe_payoff(avg_win: float | None, avg_loss: float | None) -> float | None:
    if avg_win is None or avg_loss is None or avg_loss == 0:
        return None
    return float(avg_win / abs(avg_loss))


def classify_failure_mode(row: pd.Series) -> str:
    setup = str(row.get("setup_type", "")).lower()
    gross = pd.to_numeric(row.get("realized_r_gross"), errors="coerce")
    net = pd.to_numeric(row.get("realized_r_net"), errors="coerce")
    mfe = pd.to_numeric(row.get("mfe_r"), errors="coerce")
    mae = pd.to_numeric(row.get("mae_r"), errors="coerce")
    tp_hit = bool(row.get("tp_hit_flag", False))
    time_to_tp = pd.to_numeric(row.get("time_to_tp_bars"), errors="coerce")

    if pd.notna(gross) and pd.notna(net) and gross > 0 and net < 0:
        return "cost_killed"

    if "mean_reversion" in setup:
        z = pd.to_numeric(row.get("z_vwap_t"), errors="coerce")
        if pd.notna(mfe) and mfe < 0.5:
            return "no_reversion"
        if not tp_hit and pd.notna(mfe) and mfe >= 0.5 and mfe < 1.0:
            return "reversion_too_small"
        if not tp_hit and pd.notna(mfe) and mfe >= 1.0 and pd.notna(mae) and mae >= 1.0:
            return "stop_out_after_extension"
        if pd.notna(z) and abs(z) < 1.0:
            return "noise_entry"
        return "no_reversion"

    adx = pd.to_numeric(row.get("adx_entry"), errors="coerce")
    breakout_distance = pd.to_numeric(row.get("breakout_distance_atr"), errors="coerce")
    if pd.notna(adx) and adx < 20:
        return "noise_break"
    if pd.notna(mfe) and mfe < 0.5:
        return "false_breakout"
    if not tp_hit and pd.notna(mfe) and mfe >= 0.5 and mfe < 1.0:
        return "insufficient_followthrough"
    if pd.notna(time_to_tp) and time_to_tp > 6 and pd.notna(breakout_distance) and breakout_distance >= 1.0:
        return "exhaustion_entry"
    return "insufficient_followthrough"


def _slice_key(row: pd.Series) -> str:
    if str(row.get("variant_id", "")).upper() == "L1-H10A":
        return f"tf={row.get('signal_timeframe')}|z0={row.get('z0')}|tp_r={row.get('tp_r')}|k_atr_stop={row.get('k_atr_stop')}"
    return f"tf={row.get('signal_timeframe')}|breakout_atr={row.get('breakout_atr')}|tp_r={row.get('tp_r')}|k_atr_stop={row.get('k_atr_stop')}"


def _aggregate(df: pd.DataFrame) -> dict[str, object]:
    gross = _as_num(df, "realized_r_gross")
    net = _as_num(df, "realized_r_net")
    mfe = _as_num(df, "mfe_r")
    tp_hit = df["tp_hit_flag"].astype(bool)
    wins = net[net > 0]
    losses = net[net < 0]
    avg_win = float(wins.mean()) if not wins.empty else None
    avg_loss = float(losses.mean()) if not losses.empty else None
    return {
        "n_trades": int(len(df)),
        "tp_hit_rate": float(tp_hit.mean()) if len(df) else None,
        "avg_mfe_r": _mean(mfe),
        "avg_realized_r_net": _mean(net),
        "avg_tail_potential_excess_r": _mean(df["tail_potential_excess_r"]),
        "share_of_tp_hits_with_tail_potential": float((tp_hit & df["tail_potential_flag"]).mean()) if len(df) else None,
        "share_of_all_trades_with_mfe_r_gte_2_0": float((mfe >= 2.0).mean()) if len(df) else None,
        "share_of_all_trades_with_mfe_r_gte_2_5": float((mfe >= 2.5).mean()) if len(df) else None,
        "avg_realized_r_gross": _mean(gross),
        "avg_cost_drag": _mean(gross - net),
        "share_gross_positive_net_negative": float(((gross > 0) & (net < 0)).mean()) if len(df) else None,
        "cost_drag_rate": float(((gross - net) > 0).mean()) if len(df) else None,
        "win_rate": float((net > 0).mean()) if len(df) else None,
        "avg_r_win": avg_win,
        "avg_r_loss": avg_loss,
        "payoff_ratio": _safe_payoff(avg_win, avg_loss),
        "EV_r_net": _mean(net),
        "EV_r_gross": _mean(gross),
        "min_trade_count_reliable": bool(len(df) >= 30),
    }


def _group(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for key, seg in df.groupby(keys, dropna=False, observed=False):
        vals = key if isinstance(key, tuple) else (key,)
        row = {keys[i]: vals[i] for i in range(len(keys))}
        row.update(_aggregate(seg))
        rows.append(row)
    return pd.DataFrame(rows)


def build_h10_trade_rows(experiment_root: str | Path, *, run_dirs: list[Path] | None = None) -> pd.DataFrame:
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
        if not isinstance(strategy, dict):
            continue
        strategy_name = str(strategy.get("name", ""))
        if strategy_name not in {"l1_h10a_mean_reversion_small_tp", "l1_h10b_breakout_scalping"}:
            continue

        trades = load_trades_with_entry_metadata(run_dir)
        if trades.empty:
            continue
        trades = trades.copy()
        trades["run_id"] = _first(trades, "run_id").fillna(run_dir.name)
        trades["variant_id"] = _first(trades, "entry_meta__family_variant").replace("", pd.NA)
        fallback_variant = "L1-H10A" if strategy_name.endswith("h10a_mean_reversion_small_tp") else "L1-H10B"
        trades["variant_id"] = trades["variant_id"].fillna(str(strategy.get("family_variant", fallback_variant)))
        trades["setup_type"] = _first(trades, "entry_meta__setup_type")
        trades["signal_timeframe"] = _first(trades, "entry_meta__signal_timeframe", "entry_meta__timeframe").fillna(str(strategy.get("timeframe", "")))
        trades["entry_ts"] = _first(trades, "entry_ts")
        trades["side"] = _first(trades, "side")

        trades["z_vwap_t"] = _as_num(trades, "entry_meta__z_vwap_t")
        trades["z0"] = _as_num(trades, "entry_meta__z0")
        trades["breakout_atr"] = _as_num(trades, "entry_meta__breakout_atr")
        trades["breakout_distance_atr"] = _as_num(trades, "entry_meta__breakout_distance_atr")
        trades["breakout_reference_price"] = _as_num(trades, "entry_meta__breakout_reference_price")
        trades["adx_entry"] = _as_num(trades, "entry_meta__adx_entry")

        trades["atr_entry"] = _as_num(trades, "entry_meta__atr_entry")
        trades["stop_distance"] = _as_num(trades, "entry_meta__stop_distance")
        trades["tp_distance"] = _as_num(trades, "entry_meta__tp_distance")
        trades["tp_r"] = _as_num(trades, "entry_meta__tp_r")
        trades["rr_ratio"] = _as_num(trades, "entry_meta__rr_ratio")
        trades["k_atr_stop"] = trades["stop_distance"] / trades["atr_entry"]

        trades["mfe_r"] = _as_num(trades, "mfe_r")
        trades["mae_r"] = _as_num(trades, "mae_r")
        trades["realized_r_gross"] = _as_num(trades, "r_multiple_gross")
        trades["realized_r_net"] = _as_num(trades, "r_multiple_net")
        trades["spread_cost"] = _as_num(trades, "spread_cost")
        trades["slippage_cost"] = _as_num(trades, "slippage")
        trades["fee_cost"] = _as_num(trades, "fees")
        trades["hold_bars"] = _as_num(trades, "holding_period_bars_signal")

        trades["tp_hit_flag"] = (_first(trades, "exit_reason").astype(str) == "take_profit")
        trades["time_to_tp_bars"] = _as_num(trades, "time_to_tp_bars_signal")
        trades["time_to_tp_bars"] = trades["time_to_tp_bars"].fillna(_as_num(trades, "time_to_mfe_bars_signal").where(trades["tp_hit_flag"], pd.NA))

        trades["tail_potential_excess_r"] = (trades["mfe_r"] - trades["realized_r_net"]).clip(lower=0)
        trades["tail_potential_flag"] = (trades["mfe_r"] >= TAIL_FLAG_THRESHOLD_R).fillna(False)
        trades["tail_extension_bucket"] = pd.cut(trades["tail_potential_excess_r"], bins=TAIL_EXTENSION_BINS, labels=TAIL_EXTENSION_LABELS, include_lowest=True)
        trades["parameter_slice"] = trades.apply(_slice_key, axis=1)
        trades["failure_mode_label"] = trades.apply(classify_failure_mode, axis=1)
        rows.append(trades)

    if not rows:
        return pd.DataFrame()

    keep = [
        "run_id", "variant_id", "setup_type", "symbol", "signal_timeframe", "entry_ts", "side",
        "atr_entry", "stop_distance", "tp_distance", "tp_r", "rr_ratio", "k_atr_stop",
        "z_vwap_t", "z0", "breakout_atr", "breakout_distance_atr", "breakout_reference_price", "adx_entry",
        "spread_cost", "slippage_cost", "fee_cost", "mfe_r", "mae_r", "realized_r_gross", "realized_r_net",
        "tp_hit_flag", "time_to_tp_bars", "hold_bars", "tail_potential_excess_r", "tail_potential_flag",
        "tail_extension_bucket", "parameter_slice", "failure_mode_label",
    ]
    out = pd.concat(rows, ignore_index=True)
    for col in keep:
        if col not in out.columns:
            out[col] = pd.NA
    return out[keep]


def run_h10_postmortem(experiment_root: str | Path, *, run_dirs: list[Path] | None = None, output_root: str | Path | None = None) -> dict[str, str]:
    root = Path(experiment_root)
    out_root = Path(output_root) if output_root else root / "summaries" / "diagnostics" / "l1_h10"
    out_root.mkdir(parents=True, exist_ok=True)

    rows = build_h10_trade_rows(root, run_dirs=run_dirs)
    outputs: dict[str, str] = {}
    if rows.empty:
        return outputs

    rows.to_csv(out_root / "h10_trade_diagnostics.csv", index=False)
    outputs["h10_trade_diagnostics"] = str(out_root / "h10_trade_diagnostics.csv")

    pd.DataFrame([_aggregate(rows)]).to_csv(out_root / "tail_potential_summary.csv", index=False)
    _group(rows, ["variant_id"]).to_csv(out_root / "tail_potential_by_variant.csv", index=False)
    _group(rows, ["signal_timeframe"]).to_csv(out_root / "tail_potential_by_timeframe.csv", index=False)
    _group(rows, ["symbol"]).to_csv(out_root / "tail_potential_by_symbol.csv", index=False)
    outputs.update({
        "tail_potential_summary": str(out_root / "tail_potential_summary.csv"),
        "tail_potential_by_variant": str(out_root / "tail_potential_by_variant.csv"),
        "tail_potential_by_timeframe": str(out_root / "tail_potential_by_timeframe.csv"),
        "tail_potential_by_symbol": str(out_root / "tail_potential_by_symbol.csv"),
    })

    _group(rows, ["variant_id"]).to_csv(out_root / "cost_kill_summary.csv", index=False)
    _group(rows, ["signal_timeframe"]).to_csv(out_root / "cost_kill_by_timeframe.csv", index=False)
    _group(rows, ["symbol"]).to_csv(out_root / "cost_kill_by_symbol.csv", index=False)
    _group(rows, ["parameter_slice"]).to_csv(out_root / "cost_kill_by_parameter_slice.csv", index=False)
    outputs.update({
        "cost_kill_summary": str(out_root / "cost_kill_summary.csv"),
        "cost_kill_by_timeframe": str(out_root / "cost_kill_by_timeframe.csv"),
        "cost_kill_by_symbol": str(out_root / "cost_kill_by_symbol.csv"),
        "cost_kill_by_parameter_slice": str(out_root / "cost_kill_by_parameter_slice.csv"),
    })

    pd.DataFrame([_aggregate(rows)]).to_csv(out_root / "win_rate_stability_summary.csv", index=False)
    _group(rows, ["signal_timeframe"]).to_csv(out_root / "win_rate_by_timeframe.csv", index=False)
    _group(rows, ["symbol"]).to_csv(out_root / "win_rate_by_symbol.csv", index=False)
    _group(rows, ["parameter_slice"]).to_csv(out_root / "win_rate_by_parameter_slice.csv", index=False)
    outputs.update({
        "win_rate_stability_summary": str(out_root / "win_rate_stability_summary.csv"),
        "win_rate_by_timeframe": str(out_root / "win_rate_by_timeframe.csv"),
        "win_rate_by_symbol": str(out_root / "win_rate_by_symbol.csv"),
        "win_rate_by_parameter_slice": str(out_root / "win_rate_by_parameter_slice.csv"),
    })

    _group(rows, ["failure_mode_label"]).to_csv(out_root / "failure_mode_summary.csv", index=False)
    _group(rows, ["variant_id", "failure_mode_label"]).to_csv(out_root / "failure_mode_by_variant.csv", index=False)
    _group(rows, ["signal_timeframe", "failure_mode_label"]).to_csv(out_root / "failure_mode_by_timeframe.csv", index=False)
    outputs.update({
        "failure_mode_summary": str(out_root / "failure_mode_summary.csv"),
        "failure_mode_by_variant": str(out_root / "failure_mode_by_variant.csv"),
        "failure_mode_by_timeframe": str(out_root / "failure_mode_by_timeframe.csv"),
    })
    return outputs
