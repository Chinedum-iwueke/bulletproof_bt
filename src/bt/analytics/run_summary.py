"""Reusable run-summary extraction for hypothesis experiments."""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.experiments.status import detect_run_artifact_status

CORE_COLUMNS = [
    "run_id",
    "hypothesis_id",
    "hypothesis_title",
    "variant_id",
    "tier",
    "symbol",
    "signal_timeframe",
    "start_ts",
    "end_ts",
    "status",
    "ev_r_net",
    "ev_r_gross",
    "win_rate",
    "avg_r_win",
    "avg_r_loss",
    "payoff_ratio",
    "max_consecutive_losses",
    "num_trades",
    "max_drawdown_r",
    "drawdown_duration",
    "turnover",
    "tail_loss_p95",
    "tail_loss_p99",
    "mfe_mean_r",
    "mae_mean_r",
    "mfe_median_r",
    "mae_median_r",
    "mfe_p95_r",
    "mae_p95_r",
    "frac_trades_hit_1r",
    "frac_trades_hit_2r",
    "frac_trades_hit_3r",
    "avg_time_to_mfe_signal_bars",
    "median_time_to_mfe_signal_bars",
    "avg_holding_period_signal_bars",
    "median_holding_period_signal_bars",
    "avg_holding_period_winners_signal_bars",
    "avg_holding_period_losers_signal_bars",
    "long_trade_count",
    "short_trade_count",
    "long_ev_r_net",
    "short_ev_r_net",
    "vwap_touch_rate",
    "avg_extension_to_entry_delay_signal_bars",
    "avg_vwap_distance_at_entry_atr_units",
    "armed_setup_count",
    "confirmed_reentry_count",
    "entry_conversion_rate",
    "vwap_touch_before_time_stop_rate",
    "stopout_before_any_meaningful_reversion_rate",
    "mean_max_extension_z_before_entry",
    "capture_ratio_mean",
    "output_dir",
]

HYPERPARAM_KEYS = [
    "theta_vol",
    "k_atr",
    "k_atr_entry_stop",
    "T_hold",
    "chandelier_lookback",
    "chandelier_atr_mult",
    "trail_activation_mode",
    "trail_activate_after_bars",
    "trail_activate_after_profit_r",
    "q_comp",
    "z0",
    "z_ext",
    "z_reentry",
    "require_reversal_close",
    "fit_window_days",
    "gate_quantile",
    "k",
]

_VOL_FIELD_CANDIDATES = [
    "entry_meta__vol_pct_t",
    "entry_meta__rvhat_pct_t",
    "entry_meta__vol_percentile",
    "entry_meta__rv_hat_pct",
]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _discover_run_dirs(experiment_root: Path, runs_glob: str = "runs/*") -> list[Path]:
    if (experiment_root / "performance.json").exists():
        return [experiment_root]
    run_dirs = [path for path in experiment_root.glob(runs_glob) if path.is_dir()]
    return sorted(run_dirs)


def _strategy_to_hypothesis_map() -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    for path in sorted(Path("research/hypotheses").glob("*.yaml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else {}
        strategy_name = entry.get("strategy")
        if not isinstance(strategy_name, str):
            continue
        mapping[strategy_name] = {
            "hypothesis_id": str(payload.get("hypothesis_id", strategy_name)),
            "hypothesis_title": str(payload.get("title", "")),
        }
    return mapping


def _load_manifest_hypothesis_map(experiment_root: Path) -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    manifests_dir = experiment_root / "manifests"
    if not manifests_dir.exists():
        return mapping
    for manifest_path in sorted(manifests_dir.glob("*.csv")):
        try:
            with manifest_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    run_slug = str(row.get("run_slug", "")).strip()
                    if not run_slug:
                        continue
                    hypothesis_id = str(row.get("hypothesis_id", "")).strip()
                    hypothesis_path = str(row.get("hypothesis_path", "")).strip()
                    hypothesis_title = ""
                    if hypothesis_path:
                        candidate_path = Path(hypothesis_path)
                        if candidate_path.exists():
                            payload = yaml.safe_load(candidate_path.read_text(encoding="utf-8"))
                            if isinstance(payload, dict):
                                hypothesis_title = str(payload.get("title", ""))
                    mapping[run_slug] = {
                        "hypothesis_id": hypothesis_id,
                        "hypothesis_title": hypothesis_title,
                    }
        except OSError:
            continue
    return mapping


def _load_default_hypothesis(experiment_root: Path) -> dict[str, str] | None:
    snapshot_dir = experiment_root / "contract_snapshot"
    if not snapshot_dir.exists():
        return None
    candidates = sorted(snapshot_dir.glob("*.yaml")) + sorted(snapshot_dir.glob("*.yml"))
    if len(candidates) != 1:
        return None
    payload = yaml.safe_load(candidates[0].read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    hypothesis_id = str(payload.get("hypothesis_id", "")).strip()
    hypothesis_title = str(payload.get("title", "")).strip()
    if not hypothesis_id and not hypothesis_title:
        return None
    return {"hypothesis_id": hypothesis_id, "hypothesis_title": hypothesis_title}


def _infer_variant_id(run_dir: Path) -> str:
    m = re.search(r"(g\d{4,})", run_dir.name)
    if m:
        return m.group(1)
    return ""


def _infer_tier(config: dict[str, Any], run_dir: Path) -> str:
    execution = config.get("execution") if isinstance(config.get("execution"), dict) else {}
    profile = execution.get("profile")
    if isinstance(profile, str) and profile.startswith("tier"):
        return profile.capitalize()
    m = re.search(r"__(tier\d)", run_dir.name)
    if m:
        return m.group(1).capitalize()
    return ""


def _capture_ratio_mean_from_trades(trades_df: pd.DataFrame) -> float | None:
    if trades_df.empty or "r_multiple_net" not in trades_df.columns:
        return None
    if "mfe_price" not in trades_df.columns or "risk_amount" not in trades_df.columns:
        return None

    mfe_r = pd.to_numeric(trades_df["mfe_price"], errors="coerce") / pd.to_numeric(trades_df["risk_amount"], errors="coerce")
    realized_r = pd.to_numeric(trades_df["r_multiple_net"], errors="coerce")
    denom = mfe_r.where(mfe_r > 0)
    capture = realized_r / denom
    capture = capture.replace([pd.NA, float("inf"), float("-inf")], pd.NA).dropna()
    if capture.empty:
        return None
    return float(capture.mean())


def _turnover_from_trades(trades_df: pd.DataFrame) -> float | None:
    if trades_df.empty:
        return None
    if {"entry_price", "exit_price", "qty"}.issubset(trades_df.columns):
        qty = pd.to_numeric(trades_df["qty"], errors="coerce").abs()
        entry_notional = qty * pd.to_numeric(trades_df["entry_price"], errors="coerce")
        exit_notional = qty * pd.to_numeric(trades_df["exit_price"], errors="coerce")
        return float((entry_notional + exit_notional).sum())
    return None


def _compute_trade_diagnostics(trades_df: pd.DataFrame, *, status: str, run_dir: Path) -> dict[str, Any]:
    if trades_df.empty:
        return {
            "num_trades": 0,
            "diag_status": "zero_closed_trades",
            "mfe_mean_r": None,
            "mae_mean_r": None,
            "vwap_touch_rate": None,
        }

    if "mfe_r" not in trades_df.columns or "mae_r" not in trades_df.columns:
        if status == "SUCCESS":
            raise ValueError(
                f"completed run missing mandatory path diagnostics columns mfe_r/mae_r: run_dir={run_dir}"
            )
        return {"diag_status": "missing_path_metrics"}

    mfe_r = pd.to_numeric(trades_df["mfe_r"], errors="coerce")
    mae_r = pd.to_numeric(trades_df["mae_r"], errors="coerce")
    if status == "SUCCESS" and ((mfe_r.notna().sum() == 0) or (mae_r.notna().sum() == 0)):
        raise ValueError(f"completed run has closed trades but null mfe_r/mae_r diagnostics: run_dir={run_dir}")

    realized_r = pd.to_numeric(
        trades_df.get("realized_r_net", trades_df.get("r_multiple_net", pd.Series(index=trades_df.index))),
        errors="coerce",
    )
    hold_signal = pd.to_numeric(trades_df.get("holding_period_bars_signal"), errors="coerce")
    ttm_signal = pd.to_numeric(trades_df.get("time_to_mfe_bars_signal"), errors="coerce")
    side = trades_df.get("side", pd.Series(index=trades_df.index)).astype(str).str.upper()
    exit_reason = trades_df.get("exit_reason", pd.Series(index=trades_df.index)).astype(str)
    def _series(name: str, default: float | bool | None = None) -> pd.Series:
        if name in trades_df.columns:
            return trades_df[name]
        return pd.Series(default, index=trades_df.index)

    delay_signal = pd.to_numeric(_series("extension_to_entry_delay_signal_bars"), errors="coerce")
    vwap_dist = pd.to_numeric(_series("vwap_distance_at_entry_atr_units"), errors="coerce")
    max_ext = pd.to_numeric(_series("max_extension_z_before_entry"), errors="coerce")
    touched_vwap = _series("touched_vwap_before_exit", False).astype(str).str.lower().isin({"true", "1"})
    touched_1r = _series("touched_1r_before_exit", False).astype(str).str.lower().isin({"true", "1"})

    return {
        "diag_status": "ok",
        "mfe_mean_r": float(mfe_r.mean()) if mfe_r.notna().any() else None,
        "mae_mean_r": float(mae_r.mean()) if mae_r.notna().any() else None,
        "mfe_median_r": float(mfe_r.median()) if mfe_r.notna().any() else None,
        "mae_median_r": float(mae_r.median()) if mae_r.notna().any() else None,
        "mfe_p95_r": float(mfe_r.quantile(0.95)) if mfe_r.notna().any() else None,
        "mae_p95_r": float(mae_r.quantile(0.95)) if mae_r.notna().any() else None,
        "frac_trades_hit_1r": float((mfe_r >= 1.0).mean()) if mfe_r.notna().any() else None,
        "frac_trades_hit_2r": float((mfe_r >= 2.0).mean()) if mfe_r.notna().any() else None,
        "frac_trades_hit_3r": float((mfe_r >= 3.0).mean()) if mfe_r.notna().any() else None,
        "avg_time_to_mfe_signal_bars": float(ttm_signal.mean()) if ttm_signal.notna().any() else None,
        "median_time_to_mfe_signal_bars": float(ttm_signal.median()) if ttm_signal.notna().any() else None,
        "avg_holding_period_signal_bars": float(hold_signal.mean()) if hold_signal.notna().any() else None,
        "median_holding_period_signal_bars": float(hold_signal.median()) if hold_signal.notna().any() else None,
        "avg_holding_period_winners_signal_bars": float(hold_signal[realized_r > 0].mean()) if (hold_signal[realized_r > 0].notna().any()) else None,
        "avg_holding_period_losers_signal_bars": float(hold_signal[realized_r <= 0].mean()) if (hold_signal[realized_r <= 0].notna().any()) else None,
        "long_trade_count": int((side == "BUY").sum()),
        "short_trade_count": int((side == "SELL").sum()),
        "long_ev_r_net": float(realized_r[side == "BUY"].mean()) if (side == "BUY").any() else None,
        "short_ev_r_net": float(realized_r[side == "SELL"].mean()) if (side == "SELL").any() else None,
        "vwap_touch_rate": float((exit_reason == "vwap_touch").mean()) if not exit_reason.empty else None,
        "avg_extension_to_entry_delay_signal_bars": float(delay_signal.mean()) if delay_signal.notna().any() else None,
        "avg_vwap_distance_at_entry_atr_units": float(vwap_dist.mean()) if vwap_dist.notna().any() else None,
        "vwap_touch_before_time_stop_rate": float(((exit_reason == "time_stop") & touched_vwap).mean()) if not exit_reason.empty else None,
        "stopout_before_any_meaningful_reversion_rate": float(((exit_reason == "stop_initial") & (~touched_1r)).mean()) if not exit_reason.empty else None,
        "mean_max_extension_z_before_entry": float(max_ext.mean()) if max_ext.notna().any() else None,
    }


def build_run_summary_row(
    run_dir: Path,
    *,
    completed_only: bool = True,
    hypothesis_catalog: dict[str, dict[str, str]] | None = None,
    run_hypothesis_catalog: dict[str, dict[str, str]] | None = None,
    default_hypothesis: dict[str, str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    status = detect_run_artifact_status(run_dir)
    if completed_only and status.state != "SUCCESS":
        return {}, []

    performance = _read_json(run_dir / "performance.json")
    config = _read_config(run_dir / "config_used.yaml")

    if not performance:
        warnings.append("missing_or_invalid_performance_json")

    strategy_cfg = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
    strategy_name = str(strategy_cfg.get("name", ""))

    hypo_id = strategy_name
    hypo_title = ""
    if run_hypothesis_catalog and run_dir.name in run_hypothesis_catalog:
        hypo_id = run_hypothesis_catalog[run_dir.name].get("hypothesis_id", hypo_id)
        hypo_title = run_hypothesis_catalog[run_dir.name].get("hypothesis_title", hypo_title)
    elif default_hypothesis:
        hypo_id = default_hypothesis.get("hypothesis_id", hypo_id)
        hypo_title = default_hypothesis.get("hypothesis_title", hypo_title)
    elif hypothesis_catalog and strategy_name in hypothesis_catalog:
        hypo_id = hypothesis_catalog[strategy_name]["hypothesis_id"]
        hypo_title = hypothesis_catalog[strategy_name]["hypothesis_title"]

    data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
    symbols = data_cfg.get("symbols_subset") if isinstance(data_cfg.get("symbols_subset"), list) else []
    date_range = data_cfg.get("date_range") if isinstance(data_cfg.get("date_range"), dict) else {}

    trades_path = run_dir / "trades.csv"
    try:
        trades_df = pd.read_csv(trades_path) if trades_path.exists() else pd.DataFrame()
    except pd.errors.EmptyDataError:
        trades_df = pd.DataFrame()

    row = {
        "run_id": run_dir.name,
        "hypothesis_id": hypo_id,
        "hypothesis_title": hypo_title,
        "variant_id": _infer_variant_id(run_dir),
        "tier": _infer_tier(config, run_dir),
        "symbol": "|".join(str(s) for s in symbols),
        "signal_timeframe": strategy_cfg.get("timeframe", strategy_cfg.get("signal_timeframe", "")),
        "start_ts": date_range.get("start", ""),
        "end_ts": date_range.get("end", ""),
        "status": status.state,
        "ev_r_net": performance.get("ev_r_net"),
        "ev_r_gross": performance.get("ev_r_gross"),
        "win_rate": performance.get("win_rate_r", performance.get("win_rate")),
        "avg_r_win": performance.get("avg_r_win"),
        "avg_r_loss": performance.get("avg_r_loss"),
        "payoff_ratio": performance.get("payoff_ratio_r"),
        "max_consecutive_losses": performance.get("max_consecutive_losses"),
        "num_trades": performance.get("total_trades", performance.get("trades")),
        "n_symbols": int(trades_df["symbol"].nunique()) if ("symbol" in trades_df.columns and not trades_df.empty) else 0,
        "max_drawdown_r": performance.get("max_drawdown_pct", performance.get("max_drawdown")),
        "drawdown_duration": performance.get("max_drawdown_duration", performance.get("max_drawdown_duration_bars")),
        "turnover": performance.get("turnover", _turnover_from_trades(trades_df)),
        "tail_loss_p95": performance.get("tail_loss_p95"),
        "tail_loss_p99": performance.get("tail_loss_p99"),
        "mfe_mean_r": performance.get("mfe_mean_r"),
        "mae_mean_r": performance.get("mae_mean_r"),
        "capture_ratio_mean": _capture_ratio_mean_from_trades(trades_df),
        "output_dir": str(run_dir),
    }
    row.update(_compute_trade_diagnostics(trades_df, status=status.state, run_dir=run_dir))
    mechanism = _read_json(run_dir / "l1_h2b_mechanism.json")
    armed_setup_count = mechanism.get("armed_setup_count")
    confirmed_reentry_count = mechanism.get("confirmed_reentry_count")
    row["armed_setup_count"] = armed_setup_count
    row["confirmed_reentry_count"] = confirmed_reentry_count
    if armed_setup_count in (None, 0):
        row["entry_conversion_rate"] = None
    else:
        try:
            row["entry_conversion_rate"] = float(row.get("num_trades") or 0) / float(armed_setup_count)
        except (TypeError, ValueError, ZeroDivisionError):
            row["entry_conversion_rate"] = None

    for key in HYPERPARAM_KEYS:
        row[key] = strategy_cfg.get(key)

    # evaluation-metric vocabulary availability snapshot
    row["eval_metric_tail_loss_max"] = performance.get("worst_streak_loss")
    row["eval_metric_avg_hold_bars"] = performance.get("avg_hold_bars")

    if not trades_path.exists():
        warnings.append("missing_trades_csv")

    return row, warnings


def summarize_experiment_runs(
    experiment_root: str | Path,
    *,
    output_csv: str | Path | None = None,
    completed_only: bool = True,
    runs_glob: str = "runs/*",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    root = Path(experiment_root)
    run_dirs = _discover_run_dirs(root, runs_glob=runs_glob)
    catalog = _strategy_to_hypothesis_map()
    run_hypothesis_catalog = _load_manifest_hypothesis_map(root)
    default_hypothesis = _load_default_hypothesis(root)

    rows: list[dict[str, Any]] = []
    warning_rows: list[dict[str, Any]] = []
    symbol_rows: list[dict[str, Any]] = []
    exit_rows: list[dict[str, Any]] = []

    for run_dir in run_dirs:
        row, run_warnings = build_run_summary_row(
            run_dir,
            completed_only=completed_only,
            hypothesis_catalog=catalog,
            run_hypothesis_catalog=run_hypothesis_catalog,
            default_hypothesis=default_hypothesis,
        )
        if row:
            rows.append(row)
            trades_path = run_dir / "trades.csv"
            try:
                trades_df = pd.read_csv(trades_path) if trades_path.exists() else pd.DataFrame()
            except pd.errors.EmptyDataError:
                trades_df = pd.DataFrame()
            if not trades_df.empty and "symbol" in trades_df.columns:
                for symbol, sdf in trades_df.groupby("symbol", dropna=False):
                    realized = pd.to_numeric(sdf.get("realized_r_net", sdf.get("r_multiple_net")), errors="coerce")
                    mfe_r = pd.to_numeric(sdf.get("mfe_r"), errors="coerce")
                    mae_r = pd.to_numeric(sdf.get("mae_r"), errors="coerce")
                    exit_reason = sdf.get("exit_reason", pd.Series(index=sdf.index)).fillna("other").astype(str)
                    symbol_side = sdf.get("side", pd.Series(index=sdf.index)).astype(str).str.upper()
                    is_long = symbol_side == "BUY"
                    is_short = symbol_side == "SELL"
                    symbol_rows.append(
                        {
                            "run_id": run_dir.name,
                            "hypothesis_id": row.get("hypothesis_id"),
                            "symbol": symbol,
                            "n_trades": int(sdf.shape[0]),
                            "win_rate": float((realized > 0).mean()) if realized.notna().any() else None,
                            "ev_r_net": float(realized.mean()) if realized.notna().any() else None,
                            "ev_r_gross": float(pd.to_numeric(sdf.get("realized_r_gross", sdf.get("r_multiple_gross")), errors="coerce").mean()),
                            "mfe_mean_r": float(mfe_r.mean()) if mfe_r.notna().any() else None,
                            "mae_mean_r": float(mae_r.mean()) if mae_r.notna().any() else None,
                            "frac_trades_hit_1r": float((mfe_r >= 1.0).mean()) if mfe_r.notna().any() else None,
                            "frac_trades_hit_2r": float((mfe_r >= 2.0).mean()) if mfe_r.notna().any() else None,
                            "frac_trades_hit_3r": float((mfe_r >= 3.0).mean()) if mfe_r.notna().any() else None,
                            "vwap_touch_rate": float((exit_reason == "vwap_touch").mean()) if not exit_reason.empty else None,
                            "avg_time_to_mfe_signal_bars": float(pd.to_numeric(sdf.get("time_to_mfe_bars_signal"), errors="coerce").mean()) if "time_to_mfe_bars_signal" in sdf.columns else None,
                            "avg_holding_period_signal_bars": float(pd.to_numeric(sdf.get("holding_period_bars_signal"), errors="coerce").mean()) if "holding_period_bars_signal" in sdf.columns else None,
                            "primary_exit_reason": exit_reason.value_counts().index[0] if not exit_reason.empty else "other",
                            "long_trade_count": int(is_long.sum()),
                            "short_trade_count": int(is_short.sum()),
                            "long_ev_r_net": float(realized[is_long].mean()) if is_long.any() else None,
                            "short_ev_r_net": float(realized[is_short].mean()) if is_short.any() else None,
                        }
                    )
                if "exit_reason" in trades_df.columns:
                    reasons = trades_df["exit_reason"].fillna("other").astype(str).value_counts()
                    for reason, count in reasons.items():
                        exit_rows.append({"run_id": run_dir.name, "exit_reason": reason, "n_trades": int(count)})
        for warning in run_warnings:
            warning_rows.append({"run_dir": str(run_dir), "warning": warning})

    summary_df = pd.DataFrame(rows)
    warning_df = pd.DataFrame(warning_rows)

    summaries_dir = root / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(output_csv) if output_csv else summaries_dir / "run_summary.csv"
    if not summary_df.empty:
        ordered = [col for col in CORE_COLUMNS + HYPERPARAM_KEYS if col in summary_df.columns]
        ordered += sorted(c for c in summary_df.columns if c not in ordered)
        summary_df = summary_df[ordered]
    summary_df.to_csv(out_path, index=False)

    warnings_path = summaries_dir / "diagnostics_warnings.csv"
    warning_df.to_csv(warnings_path, index=False)
    pd.DataFrame(symbol_rows).to_csv(summaries_dir / "symbol_summary.csv", index=False)
    pd.DataFrame(exit_rows).to_csv(summaries_dir / "exit_reason_summary.csv", index=False)

    return summary_df, warning_df


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["key", "value"])
        for key in sorted(payload):
            writer.writerow([key, payload[key]])
