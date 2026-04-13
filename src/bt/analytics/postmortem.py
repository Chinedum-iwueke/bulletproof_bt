"""Reusable post-run diagnostics for hypothesis experiments."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from bt.analytics.h8_postmortem import run_h8_postmortem
from bt.analytics.segment_rollups import load_trades_with_entry_metadata

VOL_METADATA_KEYS = ["vol_pct_t", "rvhat_pct_t", "vol_percentile", "rv_hat_pct"]
TIMEFRAME_KEYS = ["signal_timeframe", "timeframe"]
HYPOTHESIS_GROUPINGS: dict[str, list[str]] = {
    "L1-H1": ["gate_pass", "vol_pct_t"],
    "L1-H2": ["comp_gate_t", "q_comp", "z_vwap_t"],
    "L1-H3": ["rvhat_pct_t", "fit_window_days"],
    "L1-H3B": ["rvhat_pct_t", "fit_window_days", "z_vwap_t"],
    "L1-H3C": ["branch_selected", "regime_label", "rvhat_pct_t", "fit_window_days"],
}


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _bucket_percentile(series: pd.Series, *, bins: int = 5) -> pd.Series:
    numeric = _safe_numeric(series)
    if numeric.dropna().empty:
        return pd.Series([pd.NA] * len(series), index=series.index)
    ranked = numeric.rank(method="first", pct=True)
    return pd.cut(ranked, bins=[i / bins for i in range(bins + 1)], include_lowest=True)


def _group_ev(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if group_col not in df.columns:
        return pd.DataFrame()
    r_net = _safe_numeric(df["r_multiple_net"]) if "r_multiple_net" in df.columns else pd.Series(dtype=float)
    working = df.assign(_r_net=r_net).dropna(subset=["_r_net"])
    if working.empty:
        return pd.DataFrame()
    grouped = working.groupby(group_col, dropna=False, observed=False)
    out = grouped["_r_net"].agg([("n_trades", "count"), ("ev_r_net", "mean"), ("median_r", "median")]).reset_index()
    return out


def _load_run_trades(run_dir: Path) -> pd.DataFrame:
    try:
        df = load_trades_with_entry_metadata(run_dir)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    df = df.copy()
    df["run_dir"] = str(run_dir)
    return df


def _infer_hypothesis_id(run_dir: Path) -> str:
    config_path = run_dir / "config_used.yaml"
    if not config_path.exists():
        return ""
    import yaml

    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        return ""
    strategy = cfg.get("strategy") if isinstance(cfg.get("strategy"), dict) else {}
    strategy_name = strategy.get("name")
    mapping = {
        "l1_h1_vol_floor_trend": "L1-H1",
        "l1_h2_compression_mean_reversion": "L1-H2",
        "l1_h3_har_rv_gate_trend": "L1-H3",
        "l1_h3b_har_rv_gate_mean_reversion": "L1-H3B",
        "l1_h3c_har_regime_switch": "L1-H3C",
        "l1_h8_trend_continuation_pullback": "L1-H8",
    }
    return mapping.get(str(strategy_name), str(strategy_name) if strategy_name else "")


def run_postmortem_for_experiment(
    experiment_root: str | Path,
    *,
    run_dirs: list[Path] | None = None,
    output_root: str | Path | None = None,
) -> dict[str, str]:
    root = Path(experiment_root)
    run_paths = run_dirs if run_dirs is not None else sorted((root / "runs").glob("*"))
    run_paths = [p for p in run_paths if p.is_dir()]

    diagnostics_root = Path(output_root) if output_root else root / "summaries" / "diagnostics"
    common_root = diagnostics_root / "common"
    common_root.mkdir(parents=True, exist_ok=True)

    frames: list[pd.DataFrame] = []
    for run_dir in run_paths:
        frame = _load_run_trades(run_dir)
        if frame.empty:
            continue
        frame["hypothesis_id"] = _infer_hypothesis_id(run_dir)
        frames.append(frame)

    outputs: dict[str, str] = {}
    if not frames:
        return outputs

    all_trades = pd.concat(frames, ignore_index=True)

    vol_col = next((f"entry_meta__{key}" for key in VOL_METADATA_KEYS if f"entry_meta__{key}" in all_trades.columns), None)
    if vol_col is not None:
        bucket_col = "vol_bucket"
        all_trades[bucket_col] = _bucket_percentile(all_trades[vol_col])
        vol_ev = _group_ev(all_trades, bucket_col)
        path = common_root / "conditional_ev_by_vol_bucket.csv"
        vol_ev.to_csv(path, index=False)
        outputs["conditional_ev_by_vol_bucket"] = str(path)

    tf_col = next((f"entry_meta__{k}" for k in TIMEFRAME_KEYS if f"entry_meta__{k}" in all_trades.columns), None)
    if tf_col is None:
        tf_col = "_timeframe_fallback"
        all_trades[tf_col] = "unknown"
    tf_ev = _group_ev(all_trades, tf_col)
    tf_path = common_root / "conditional_ev_by_timeframe.csv"
    tf_ev.to_csv(tf_path, index=False)
    outputs["conditional_ev_by_timeframe"] = str(tf_path)

    if "symbol" in all_trades.columns:
        symbol_ev = _group_ev(all_trades, "symbol")
        sym_path = common_root / "conditional_ev_by_symbol.csv"
        symbol_ev.to_csv(sym_path, index=False)
        outputs["conditional_ev_by_symbol"] = str(sym_path)

    if {"mfe_price", "risk_amount", "r_multiple_net"}.issubset(all_trades.columns):
        mfe_r = _safe_numeric(all_trades["mfe_price"]) / _safe_numeric(all_trades["risk_amount"])
        realized = _safe_numeric(all_trades["r_multiple_net"])
        capture = realized / mfe_r.where(mfe_r > 0)
        mfe_df = pd.DataFrame(
            {
                "avg_mfe_r": [float(mfe_r.dropna().mean()) if not mfe_r.dropna().empty else None],
                "avg_realized_r": [float(realized.dropna().mean()) if not realized.dropna().empty else None],
                "avg_capture_ratio": [float(capture.dropna().mean()) if not capture.dropna().empty else None],
                "high_mfe_low_capture_share": [
                    float(((mfe_r > 1.0) & (capture < 0.5)).mean()) if len(all_trades) else None
                ],
            }
        )
        mfe_path = common_root / "mfe_capture_summary.csv"
        mfe_df.to_csv(mfe_path, index=False)
        outputs["mfe_capture_summary"] = str(mfe_path)

        if "symbol" in all_trades.columns:
            by_symbol = pd.DataFrame({"symbol": all_trades["symbol"], "mfe_r": mfe_r, "capture_ratio": capture})
            by_symbol = by_symbol.groupby("symbol", dropna=False).agg(avg_mfe_r=("mfe_r", "mean"), avg_capture_ratio=("capture_ratio", "mean"), n_trades=("mfe_r", "count")).reset_index()
            by_symbol_path = common_root / "mfe_capture_by_symbol.csv"
            by_symbol.to_csv(by_symbol_path, index=False)
            outputs["mfe_capture_by_symbol"] = str(by_symbol_path)

    # gross-vs-net degradation per run
    cost_rows: list[dict[str, Any]] = []
    for run_dir in run_paths:
        perf_path = run_dir / "performance.json"
        if not perf_path.exists():
            continue
        payload = json.loads(perf_path.read_text(encoding="utf-8"))
        ev_g = payload.get("ev_r_gross")
        ev_n = payload.get("ev_r_net")
        if ev_g is None or ev_n is None:
            continue
        cost_rows.append({"run_dir": str(run_dir), "ev_r_gross": ev_g, "ev_r_net": ev_n, "cost_drag_r": ev_g - ev_n})
    if cost_rows:
        cost_path = common_root / "cost_drag_summary.csv"
        pd.DataFrame(cost_rows).to_csv(cost_path, index=False)
        outputs["cost_drag_summary"] = str(cost_path)

    h8_outputs = run_h8_postmortem(root, run_dirs=run_paths, output_root=diagnostics_root / "l1_h8")
    outputs.update({f"L1-H8:{k}": v for k, v in h8_outputs.items()})

    # lightweight hypothesis-specific diagnostics registry
    for hypothesis_id, segment_keys in HYPOTHESIS_GROUPINGS.items():
        subset = all_trades[all_trades["hypothesis_id"] == hypothesis_id]
        if subset.empty:
            continue
        hypo_root = diagnostics_root / hypothesis_id.lower().replace("-", "_")
        hypo_root.mkdir(parents=True, exist_ok=True)
        for key in segment_keys:
            column = f"entry_meta__{key}"
            if column not in subset.columns:
                continue
            grouped = _group_ev(subset, column)
            if grouped.empty:
                continue
            out_path = hypo_root / f"conditional_ev_by_{key}.csv"
            grouped.to_csv(out_path, index=False)
            outputs[f"{hypothesis_id}:{key}"] = str(out_path)

    manifest_path = diagnostics_root / "manifest.json"
    manifest_path.write_text(json.dumps(outputs, indent=2, sort_keys=True), encoding="utf-8")
    outputs["manifest"] = str(manifest_path)
    return outputs
