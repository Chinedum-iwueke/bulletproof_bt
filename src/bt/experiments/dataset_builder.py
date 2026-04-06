"""Canonical experiment dataset extraction pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from time import perf_counter
from typing import Any

import pandas as pd
import yaml

SCRIPT_VERSION = "1.0.0"
DATASET_SCHEMA_VERSION = "v1"

REQUIRED_OUTPUT_FILES = {
    "trades_dataset": "trades_dataset.parquet",
    "runs_dataset": "runs_dataset.parquet",
    "dataset_manifest": "dataset_manifest.json",
    "feature_dictionary": "feature_dictionary.json",
    "experiment_summary": "experiment_summary.json",
    "extraction_log": "extraction_log.json",
    "dropped_runs": "dropped_runs.csv",
}

RUN_REQUIRED_ARTIFACTS = ("performance.json", "config_used.yaml")

TRADE_OPTIONAL_CONTEXT_COLUMNS = [
    "entry_signal_tf",
    "regime_label",
    "rv_hat",
    "rv_percentile",
    "realized_vol",
    "adx",
    "atr",
    "ema_fast",
    "ema_slow",
    "spread_proxy",
    "liq_quantile",
    "vol_bucket",
    "liq_bucket",
]

RUN_DATASET_COLUMNS_V1 = [
    "experiment_id",
    "experiment_root",
    "hypothesis_id",
    "dataset_tag",
    "run_id",
    "manifest_row_index",
    "variant_id",
    "parameter_set_id",
    "params_json",
    "net_pnl",
    "gross_pnl",
    "return_pct",
    "sharpe",
    "sortino",
    "calmar",
    "max_drawdown",
    "trade_count",
    "win_rate",
    "profit_factor",
    "expectancy",
    "avg_trade",
    "avg_win",
    "avg_loss",
    "median_pnl_r",
    "mean_pnl_r",
    "avg_duration_bars",
    "median_duration_bars",
    "long_trade_count",
    "short_trade_count",
    "avg_mae_r",
    "avg_mfe_r",
    "fees_total",
    "slippage_total",
    "cost_drag_pct",
    "exit_reason_distribution_json",
    "run_complete_flag",
    "required_artifacts_present",
    "parse_success_flag",
    "dropped_reason",
    "run_rank_by_net_pnl",
    "run_rank_by_sharpe",
    "run_is_top_decile",
    "run_is_bottom_decile",
]

TRADES_DATASET_COLUMNS_V1 = [
    "experiment_id",
    "hypothesis_id",
    "dataset_tag",
    "run_id",
    "parameter_set_id",
    "manifest_row_index",
    "trade_id",
    "symbol",
    "side",
    "entry_time",
    "exit_time",
    "entry_price",
    "exit_price",
    "pnl",
    "pnl_pct",
    "pnl_r",
    "gross_pnl",
    "net_pnl",
    "fees_paid",
    "slippage_cost",
    "win_flag",
    "mfe",
    "mae",
    "mfe_r",
    "mae_r",
    "duration_bars",
    "duration_minutes",
    "exit_reason",
    "run_net_pnl",
    "run_sharpe",
    "run_max_drawdown",
    "run_trade_count",
    "run_rank_by_net_pnl",
    "run_is_top_decile",
    "run_passes_min_trade_count",
]


@dataclass
class ExtractionLog:
    missing_artifacts: list[dict[str, Any]] = field(default_factory=list)
    columns_skipped: list[str] = field(default_factory=list)
    source_conflicts: list[str] = field(default_factory=list)
    fallback_derivations: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)


@dataclass
class RunParseResult:
    include: bool
    run_record: dict[str, Any] | None
    trades_df: pd.DataFrame
    dropped_reason: str | None = None
    missing_artifact: str = ""
    notes: str = ""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _discover_run_dirs(experiment_root: Path, runs_glob: str) -> list[Path]:
    run_dirs = [path for path in experiment_root.glob(runs_glob) if path.is_dir()]
    return sorted(run_dirs, key=lambda p: p.name)


def _extract_dataset_tag(experiment_root_name: str) -> str | None:
    lowered = experiment_root_name.lower()
    for tag in ("stable", "vol"):
        if re.search(rf"(^|[_\-]){tag}($|[_\-])", lowered):
            return tag
    return None


def _load_summary_map(experiment_root: Path) -> dict[str, dict[str, Any]]:
    path = experiment_root / "summaries" / "run_summary.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if "run_id" not in df.columns:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        run_id = str(row.get("run_id", "")).strip()
        if not run_id:
            continue
        result[run_id] = {k: _to_native(v) for k, v in row.to_dict().items()}
    return result


def _select_manifest(experiment_root: Path) -> Path | None:
    manifest_dir = experiment_root / "manifests"
    if not manifest_dir.exists():
        return None
    csvs = sorted(manifest_dir.glob("*.csv"))
    if not csvs:
        return None
    grid_like = [path for path in csvs if path.name.endswith("_grid.csv")]
    selected = grid_like[0] if grid_like else csvs[0]
    return selected


def _manifest_map(manifest_path: Path | None) -> tuple[dict[str, dict[str, Any]], str | None]:
    if manifest_path is None:
        return {}, None
    df = pd.read_csv(manifest_path)
    rows: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        payload = {k: _to_native(v) for k, v in row.to_dict().items()}
        output_dir = str(payload.get("output_dir", "")).strip()
        run_slug = str(payload.get("run_slug", "")).strip()
        row_id = str(payload.get("row_id", "")).strip()
        key = Path(output_dir).name if output_dir else run_slug
        if key:
            rows[key] = payload
        if row_id and key and row_id != key:
            rows[row_id] = payload
    return rows, str(manifest_path)


def _load_contract_snapshot_hypothesis(experiment_root: Path) -> str | None:
    snapshot_dir = experiment_root / "contract_snapshot"
    if not snapshot_dir.exists():
        return None
    candidates = sorted(snapshot_dir.glob("*.yaml")) + sorted(snapshot_dir.glob("*.yml"))
    for path in candidates:
        payload = _read_yaml(path)
        hypothesis = payload.get("hypothesis_id")
        if isinstance(hypothesis, str) and hypothesis.strip():
            return hypothesis.strip()
    return None


def _infer_hypothesis_id(
    *,
    run_id: str,
    summary_row: dict[str, Any] | None,
    manifest_row: dict[str, Any] | None,
    config: dict[str, Any],
    default_hypothesis: str | None,
) -> str | None:
    if default_hypothesis:
        return default_hypothesis
    if summary_row:
        value = summary_row.get("hypothesis_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    if manifest_row:
        value = manifest_row.get("hypothesis_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    strategy = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
    name = strategy.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _to_native(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else value


def _clean_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _clean_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean_for_json(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _pick(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        return value
    return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    return int(parsed) if parsed is not None else None


def _manifest_row_index(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    match = re.search(r"(\d+)$", text)
    if match:
        return int(match.group(1))
    return None


def _parse_params(manifest_row: dict[str, Any] | None, config: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    if manifest_row:
        raw = manifest_row.get("params_json")
        if isinstance(raw, str) and raw.strip():
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, dict):
                return raw, payload
    strategy = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
    excluded = {"name", "timeframe", "signal_timeframe"}
    payload = {k: v for k, v in strategy.items() if k not in excluded}
    if payload:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return serialized, payload
    return None, {}


def _flatten_params(params: dict[str, Any]) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in sorted(params.items()):
        normalized_key = re.sub(r"[^0-9a-zA-Z_]+", "_", str(key)).strip("_").lower()
        if not normalized_key:
            continue
        name = f"param_{normalized_key}"
        if isinstance(value, (dict, list)):
            flat[name] = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        else:
            flat[name] = value
    return flat




def _series_from_candidates(df: pd.DataFrame, *candidates: str, default: float | str | None = None) -> pd.Series:
    for name in candidates:
        if name in df.columns:
            return df[name]
    return pd.Series(default, index=df.index)


def _numeric_series_from_candidates(df: pd.DataFrame, *candidates: str, default: float | None = None) -> pd.Series:
    return pd.to_numeric(_series_from_candidates(df, *candidates, default=default), errors="coerce")

def _parse_timestamp_utc(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed


def _normalize_trade_side(side_series: pd.Series) -> pd.Series:
    normalized = side_series.astype(str).str.strip().str.lower()
    mapped = normalized.map({"buy": "long", "sell": "short", "long": "long", "short": "short"})
    return mapped.where(mapped.notna(), normalized)


def _compute_run_metrics_from_trades(trades_df: pd.DataFrame, log: ExtractionLog, run_id: str) -> dict[str, Any]:
    if trades_df.empty:
        return {}
    pnl_net_col = "pnl_net" if "pnl_net" in trades_df.columns else ("pnl" if "pnl" in trades_df.columns else None)
    if pnl_net_col is None:
        return {}

    pnl_net = pd.to_numeric(trades_df[pnl_net_col], errors="coerce")
    pnl_gross = _numeric_series_from_candidates(trades_df, "pnl_price", "pnl", "pnl_net")
    pnl_r = _numeric_series_from_candidates(trades_df, "r_multiple_net", "realized_r_net")
    fees = _numeric_series_from_candidates(trades_df, "fees_paid", "fees", default=0.0).fillna(0.0)
    slippage = _numeric_series_from_candidates(trades_df, "slippage", "slippage_total", default=0.0).fillna(0.0)

    winners = pnl_net[pnl_net > 0]
    losers = pnl_net[pnl_net < 0]
    gross_wins = winners.sum() if not winners.empty else 0.0
    gross_losses = abs(losers.sum()) if not losers.empty else 0.0

    durations = _numeric_series_from_candidates(trades_df, "holding_period_bars_signal", "duration_bars")

    side = trades_df.get("side", pd.Series(index=trades_df.index, dtype=object)).astype(str).str.upper()
    exit_reason_counts = {}
    if "exit_reason" in trades_df.columns:
        exit_reason_counts = {str(k): int(v) for k, v in trades_df["exit_reason"].fillna("unknown").astype(str).value_counts().items()}

    metrics = {
        "trade_count": int(len(trades_df)),
        "net_pnl": float(pnl_net.sum()) if pnl_net.notna().any() else None,
        "gross_pnl": float(pnl_gross.sum()) if pnl_gross.notna().any() else None,
        "win_rate": float((pnl_net > 0).mean()) if pnl_net.notna().any() else None,
        "profit_factor": float(gross_wins / gross_losses) if gross_losses > 0 else None,
        "expectancy": float(pnl_net.mean()) if pnl_net.notna().any() else None,
        "avg_trade": float(pnl_net.mean()) if pnl_net.notna().any() else None,
        "avg_win": float(winners.mean()) if not winners.empty else None,
        "avg_loss": float(losers.mean()) if not losers.empty else None,
        "median_pnl_r": float(pnl_r.median()) if pnl_r.notna().any() else None,
        "mean_pnl_r": float(pnl_r.mean()) if pnl_r.notna().any() else None,
        "long_trade_count": int((side == "BUY").sum()),
        "short_trade_count": int((side == "SELL").sum()),
        "avg_duration_bars": float(durations.mean()) if durations.notna().any() else None,
        "median_duration_bars": float(durations.median()) if durations.notna().any() else None,
        "avg_mae_r": float(_numeric_series_from_candidates(trades_df, "mae_r").mean()) if "mae_r" in trades_df.columns else None,
        "avg_mfe_r": float(_numeric_series_from_candidates(trades_df, "mfe_r").mean()) if "mfe_r" in trades_df.columns else None,
        "fees_total": float(fees.sum()),
        "slippage_total": float(slippage.sum()),
        "exit_reason_distribution_json": json.dumps(exit_reason_counts, sort_keys=True),
    }
    if metrics["gross_pnl"] not in (None, 0):
        metrics["cost_drag_pct"] = float((metrics["fees_total"] + metrics["slippage_total"]) / abs(metrics["gross_pnl"]) * 100.0)
    else:
        metrics["cost_drag_pct"] = None

    if "holding_period_bars_signal" not in trades_df.columns and "duration_bars" not in trades_df.columns:
        log.fallback_derivations.append(f"run={run_id}: missing duration bars columns")

    return metrics


def _build_trade_rows(
    trades_df: pd.DataFrame,
    *,
    run_provenance: dict[str, Any],
    min_trades_per_run: int,
) -> pd.DataFrame:
    if trades_df.empty or len(trades_df) < min_trades_per_run:
        return pd.DataFrame()

    out = pd.DataFrame(index=trades_df.index)
    out["trade_id"] = [f"{run_provenance['run_id']}_t{idx + 1:05d}" for idx in range(len(trades_df))]
    out["symbol"] = trades_df.get("symbol")
    out["side"] = _normalize_trade_side(trades_df.get("side", pd.Series(index=trades_df.index)))
    out["entry_time"] = _parse_timestamp_utc(trades_df.get("entry_ts", pd.Series(index=trades_df.index)))
    out["exit_time"] = _parse_timestamp_utc(trades_df.get("exit_ts", pd.Series(index=trades_df.index)))

    out["entry_price"] = pd.to_numeric(trades_df.get("entry_price"), errors="coerce")
    out["exit_price"] = pd.to_numeric(trades_df.get("exit_price"), errors="coerce")

    out["pnl"] = pd.to_numeric(trades_df.get("pnl", trades_df.get("pnl_price")), errors="coerce")
    out["gross_pnl"] = pd.to_numeric(trades_df.get("pnl_price", trades_df.get("pnl")), errors="coerce")
    out["net_pnl"] = pd.to_numeric(trades_df.get("pnl_net", trades_df.get("pnl")), errors="coerce")
    out["fees_paid"] = pd.to_numeric(trades_df.get("fees_paid", trades_df.get("fees", 0.0)), errors="coerce")
    out["slippage_cost"] = pd.to_numeric(trades_df.get("slippage", trades_df.get("slippage_total", 0.0)), errors="coerce")

    entry_notional = pd.to_numeric(trades_df.get("entry_price"), errors="coerce") * pd.to_numeric(trades_df.get("qty"), errors="coerce").abs()
    out["pnl_pct"] = out["net_pnl"] / entry_notional.replace(0.0, pd.NA)
    out["pnl_r"] = pd.to_numeric(trades_df.get("r_multiple_net", trades_df.get("realized_r_net")), errors="coerce")

    out["mfe"] = pd.to_numeric(trades_df.get("mfe_price"), errors="coerce")
    out["mae"] = pd.to_numeric(trades_df.get("mae_price"), errors="coerce")
    out["mfe_r"] = pd.to_numeric(trades_df.get("mfe_r"), errors="coerce")
    out["mae_r"] = pd.to_numeric(trades_df.get("mae_r"), errors="coerce")

    out["duration_bars"] = pd.to_numeric(
        trades_df.get("holding_period_bars_signal", trades_df.get("duration_bars")),
        errors="coerce",
    )
    out["duration_minutes"] = (out["exit_time"] - out["entry_time"]).dt.total_seconds() / 60.0
    out["exit_reason"] = trades_df.get("exit_reason")
    out["win_flag"] = (out["net_pnl"] > 0).astype("int64")

    for column in TRADE_OPTIONAL_CONTEXT_COLUMNS:
        if column in trades_df.columns:
            out[column] = trades_df[column]

    for key, value in run_provenance.items():
        out[key] = value

    return out


def _parse_run(
    run_dir: Path,
    *,
    experiment_id: str,
    experiment_root: Path,
    dataset_tag: str | None,
    summary_row: dict[str, Any] | None,
    manifest_row: dict[str, Any] | None,
    default_hypothesis_id: str | None,
    min_trades_per_run: int,
    log: ExtractionLog,
) -> RunParseResult:
    run_id = run_dir.name

    artifacts_present = {name: (run_dir / name).exists() for name in RUN_REQUIRED_ARTIFACTS}
    missing_required = [name for name, present in artifacts_present.items() if not present]

    config = _read_yaml(run_dir / "config_used.yaml")
    performance = _read_json(run_dir / "performance.json")
    run_status = _read_json(run_dir / "run_status.json")
    cost_breakdown = _read_json(run_dir / "cost_breakdown.json")

    try:
        trades_df = pd.read_csv(run_dir / "trades.csv") if (run_dir / "trades.csv").exists() else pd.DataFrame()
    except Exception as exc:
        return RunParseResult(
            include=False,
            run_record=None,
            trades_df=pd.DataFrame(),
            dropped_reason="trades_parse_error",
            missing_artifact="trades.csv",
            notes=str(exc),
        )

    if summary_row is None and not performance and trades_df.empty:
        return RunParseResult(
            include=False,
            run_record=None,
            trades_df=trades_df,
            dropped_reason="missing_run_level_sources",
            notes="No summary row, no performance.json, and empty/missing trades.csv",
        )

    hypothesis_id = _infer_hypothesis_id(
        run_id=run_id,
        summary_row=summary_row,
        manifest_row=manifest_row,
        config=config,
        default_hypothesis=default_hypothesis_id,
    )
    params_json, params = _parse_params(manifest_row, config)

    manifest_row_index = None
    variant_id = None
    parameter_set_id = None
    if manifest_row:
        manifest_row_index = _manifest_row_index(_pick(manifest_row.get("row_id"), manifest_row.get("row_index")))
        variant_id = manifest_row.get("variant_id")
        parameter_set_id = _pick(manifest_row.get("config_hash"), manifest_row.get("parameter_set_id"))

    trade_metrics = _compute_run_metrics_from_trades(trades_df, log, run_id)

    run_complete_flag = str(run_status.get("status", "")).upper() in {"PASS", "SUCCESS"}
    required_artifacts_present = not missing_required

    summary_row = summary_row or {}

    fees_total = _pick(
        _safe_float(cost_breakdown.get("totals", {}).get("fees_total") if isinstance(cost_breakdown.get("totals"), dict) else None),
        trade_metrics.get("fees_total"),
    )
    slippage_total = _pick(
        _safe_float(cost_breakdown.get("totals", {}).get("slippage_total") if isinstance(cost_breakdown.get("totals"), dict) else None),
        trade_metrics.get("slippage_total"),
    )

    run_record: dict[str, Any] = {
        "experiment_id": experiment_id,
        "experiment_root": str(experiment_root.resolve()),
        "hypothesis_id": hypothesis_id,
        "dataset_tag": dataset_tag,
        "run_id": run_id,
        "manifest_row_index": manifest_row_index,
        "variant_id": variant_id,
        "parameter_set_id": parameter_set_id,
        "params_json": params_json,
        "config_path": str(run_dir / "config_used.yaml"),
        "contract_snapshot_path": None,
        "net_pnl": _pick(_safe_float(summary_row.get("pnl_net")), _safe_float(performance.get("net_pnl")), trade_metrics.get("net_pnl")),
        "gross_pnl": _pick(_safe_float(performance.get("gross_pnl")), trade_metrics.get("gross_pnl")),
        "return_pct": _pick(_safe_float(summary_row.get("return_pct")), _safe_float(performance.get("total_return"))),
        "sharpe": _pick(_safe_float(summary_row.get("sharpe")), _safe_float(performance.get("sharpe_annualized"))),
        "sortino": _pick(_safe_float(summary_row.get("sortino")), _safe_float(performance.get("sortino_annualized"))),
        "calmar": _pick(_safe_float(summary_row.get("calmar")), _safe_float(performance.get("mar_ratio")), _safe_float(performance.get("calmar"))),
        "max_drawdown": _pick(_safe_float(summary_row.get("max_drawdown")), _safe_float(performance.get("max_drawdown_pct")), _safe_float(performance.get("max_drawdown"))),
        "trade_count": _pick(_safe_int(summary_row.get("num_trades")), _safe_int(performance.get("total_trades")), trade_metrics.get("trade_count")),
        "win_rate": _pick(_safe_float(summary_row.get("win_rate")), _safe_float(performance.get("win_rate")), _safe_float(performance.get("win_rate_r")), trade_metrics.get("win_rate")),
        "profit_factor": _pick(_safe_float(summary_row.get("profit_factor")), _safe_float(performance.get("profit_factor_r")), trade_metrics.get("profit_factor")),
        "expectancy": _pick(_safe_float(summary_row.get("expectancy")), trade_metrics.get("expectancy")),
        "avg_trade": _pick(_safe_float(summary_row.get("avg_trade")), trade_metrics.get("avg_trade")),
        "avg_win": _pick(_safe_float(summary_row.get("avg_win")), trade_metrics.get("avg_win")),
        "avg_loss": _pick(_safe_float(summary_row.get("avg_loss")), trade_metrics.get("avg_loss")),
        "median_pnl_r": _pick(_safe_float(summary_row.get("median_pnl_r")), trade_metrics.get("median_pnl_r")),
        "mean_pnl_r": _pick(_safe_float(summary_row.get("mean_pnl_r")), trade_metrics.get("mean_pnl_r")),
        "long_trade_count": trade_metrics.get("long_trade_count"),
        "short_trade_count": trade_metrics.get("short_trade_count"),
        "avg_duration_bars": trade_metrics.get("avg_duration_bars"),
        "median_duration_bars": trade_metrics.get("median_duration_bars"),
        "avg_mae_r": trade_metrics.get("avg_mae_r"),
        "avg_mfe_r": trade_metrics.get("avg_mfe_r"),
        "fees_total": fees_total,
        "slippage_total": slippage_total,
        "cost_drag_pct": _pick(_safe_float(cost_breakdown.get("cost_drag_pct")), trade_metrics.get("cost_drag_pct")),
        "exit_reason_distribution_json": trade_metrics.get("exit_reason_distribution_json"),
        "run_complete_flag": run_complete_flag,
        "required_artifacts_present": required_artifacts_present,
        "parse_success_flag": True,
        "dropped_reason": None,
    }

    run_record.update(_flatten_params(params))

    if missing_required:
        log.missing_artifacts.append({"run_id": run_id, "artifacts": missing_required})

    if trades_df.empty:
        log.info.append(f"run={run_id}: trades.csv empty or missing; run kept with run-level metrics only")
    elif len(trades_df) < min_trades_per_run:
        log.info.append(f"run={run_id}: trades below --min-trades-per-run ({len(trades_df)} < {min_trades_per_run})")

    provenance = {
        "experiment_id": experiment_id,
        "hypothesis_id": hypothesis_id,
        "dataset_tag": dataset_tag,
        "run_id": run_id,
        "manifest_row_index": manifest_row_index,
        "variant_id": variant_id,
        "parameter_set_id": parameter_set_id,
        "params_json": params_json,
    }
    provenance.update(_flatten_params(params))
    trade_rows = _build_trade_rows(trades_df, run_provenance=provenance, min_trades_per_run=min_trades_per_run)

    return RunParseResult(include=True, run_record=run_record, trades_df=trade_rows)


def _enrich_trade_labels(
    trades_df: pd.DataFrame,
    runs_df: pd.DataFrame,
    *,
    min_trades_per_run: int,
) -> pd.DataFrame:
    if trades_df.empty:
        return trades_df

    enriched = trades_df.merge(
        runs_df[
            [
                "run_id",
                "net_pnl",
                "sharpe",
                "max_drawdown",
                "trade_count",
                "run_rank_by_net_pnl",
                "run_is_top_decile",
            ]
        ],
        on="run_id",
        how="left",
        suffixes=("", "_run"),
    )
    enriched = enriched.rename(
        columns={
            "net_pnl": "run_net_pnl",
            "sharpe": "run_sharpe",
            "max_drawdown": "run_max_drawdown",
            "trade_count": "run_trade_count",
        }
    )
    enriched["run_passes_min_trade_count"] = enriched["run_trade_count"].fillna(0) >= min_trades_per_run
    return enriched


def _build_feature_dictionary(trades_df: pd.DataFrame, runs_df: pd.DataFrame) -> list[dict[str, Any]]:
    descriptions = {
        "experiment_id": "Stable experiment identifier (defaults to experiment root name).",
        "experiment_root": "Absolute experiment root path.",
        "hypothesis_id": "Hypothesis identifier inferred from contract snapshot/summary/manifest/config.",
        "dataset_tag": "Dataset family tag inferred from experiment name (e.g., stable, vol).",
        "run_id": "Run folder identifier.",
        "params_json": "Original JSON parameter blob when available.",
        "run_rank_by_net_pnl": "Rank of run by net_pnl descending across extracted runs.",
        "run_is_top_decile": "True when run ranks in top decile by net_pnl.",
    }

    source_defaults = {
        "trades_dataset": "trades.csv",
        "runs_dataset": "performance.json + summaries/run_summary.csv + trades.csv",
    }

    rows: list[dict[str, Any]] = []
    for dataset_name, df in (("trades_dataset", trades_df), ("runs_dataset", runs_df)):
        for column in df.columns:
            derivation = "direct" if not column.startswith("run_") else "enriched from runs dataset"
            if column.startswith("param_"):
                derivation = "flattened from params_json"
            rows.append(
                {
                    "dataset": dataset_name,
                    "column": column,
                    "dtype": str(df[column].dtype),
                    "description": descriptions.get(column, f"{column} extracted for research dataset."),
                    "source_artifact": source_defaults[dataset_name],
                    "derivation_rule": derivation,
                }
            )
    return rows


def _add_run_rankings(runs_df: pd.DataFrame) -> pd.DataFrame:
    ranked = runs_df.copy()
    ranked["run_rank_by_net_pnl"] = pd.NA
    ranked["run_rank_by_sharpe"] = pd.NA
    ranked["run_is_top_decile"] = False
    ranked["run_is_bottom_decile"] = False

    valid_net = ranked["net_pnl"].notna() if "net_pnl" in ranked.columns else pd.Series(False, index=ranked.index)
    if valid_net.any():
        ranked.loc[valid_net, "run_rank_by_net_pnl"] = (
            ranked.loc[valid_net, "net_pnl"].rank(method="dense", ascending=False).astype("Int64")
        )

    valid_sharpe = ranked["sharpe"].notna() if "sharpe" in ranked.columns else pd.Series(False, index=ranked.index)
    if valid_sharpe.any():
        ranked.loc[valid_sharpe, "run_rank_by_sharpe"] = (
            ranked.loc[valid_sharpe, "sharpe"].rank(method="dense", ascending=False).astype("Int64")
        )

    total = len(ranked)
    if total > 0 and "run_rank_by_net_pnl" in ranked.columns:
        decile_n = max(1, int(round(total * 0.1)))
        rank_series = pd.to_numeric(ranked["run_rank_by_net_pnl"], errors="coerce")
        ranked["run_is_top_decile"] = (rank_series <= decile_n).fillna(False)
        ranked["run_is_bottom_decile"] = (rank_series >= (total - decile_n + 1)).fillna(False)
    return ranked


def _enforce_contract_column_order(
    runs_df: pd.DataFrame,
    trades_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    run_cols = RUN_DATASET_COLUMNS_V1 + sorted(
        col for col in runs_df.columns if col.startswith("param_") and col not in RUN_DATASET_COLUMNS_V1
    )
    run_cols += [col for col in runs_df.columns if col not in run_cols]
    for col in run_cols:
        if col not in runs_df.columns:
            runs_df[col] = None
    runs_df = runs_df[run_cols]

    trade_cols = TRADES_DATASET_COLUMNS_V1 + TRADE_OPTIONAL_CONTEXT_COLUMNS + sorted(
        col for col in trades_df.columns if col.startswith("param_") and col not in TRADES_DATASET_COLUMNS_V1
    )
    trade_cols += [col for col in trades_df.columns if col not in trade_cols]
    for col in trade_cols:
        if col not in trades_df.columns:
            trades_df[col] = None
    trades_df = trades_df[trade_cols]
    return runs_df, trades_df


def _experiment_summary(experiment_root: Path, runs_df: pd.DataFrame, trades_df: pd.DataFrame) -> dict[str, Any]:
    best_run_id = None
    worst_run_id = None
    if not runs_df.empty and "net_pnl" in runs_df.columns:
        ordered = runs_df.dropna(subset=["net_pnl"]).sort_values("net_pnl", ascending=False)
        if not ordered.empty:
            best_run_id = ordered.iloc[0]["run_id"]
            worst_run_id = ordered.iloc[-1]["run_id"]

    profitable = runs_df["net_pnl"] > 0 if "net_pnl" in runs_df.columns else pd.Series(dtype=bool)

    return {
        "experiment_root": str(experiment_root),
        "total_runs_scanned": int(runs_df.attrs.get("total_runs_scanned", len(runs_df))),
        "total_runs_parsed": int(len(runs_df)),
        "total_trade_rows": int(len(trades_df)),
        "profitable_run_count": int(profitable.sum()) if not profitable.empty else 0,
        "profitable_run_pct": float(profitable.mean()) if not profitable.empty else 0.0,
        "best_run_id": best_run_id,
        "worst_run_id": worst_run_id,
        "median_run_net_pnl": float(runs_df["net_pnl"].median()) if "net_pnl" in runs_df.columns and runs_df["net_pnl"].notna().any() else None,
        "mean_run_net_pnl": float(runs_df["net_pnl"].mean()) if "net_pnl" in runs_df.columns and runs_df["net_pnl"].notna().any() else None,
        "mean_trade_pnl_r": float(trades_df["pnl_r"].mean()) if "pnl_r" in trades_df.columns and trades_df["pnl_r"].notna().any() else None,
        "median_trade_pnl_r": float(trades_df["pnl_r"].median()) if "pnl_r" in trades_df.columns and trades_df["pnl_r"].notna().any() else None,
    }


def extract_experiment_dataset(
    *,
    experiment_root: Path,
    runs_glob: str = "runs/*",
    out_dir: Path | None = None,
    skip_existing: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
    min_trades_per_run: int = 1,
    top_run_count_for_labels: int = 5,
) -> dict[str, Any]:
    del top_run_count_for_labels  # reserved for future labeling modes

    start_time = perf_counter()
    if not experiment_root.exists():
        raise ValueError(f"experiment root does not exist: {experiment_root}")

    output_dir = out_dir if out_dir is not None else experiment_root / "research_data"
    output_dir = output_dir.resolve()

    output_paths = {name: output_dir / filename for name, filename in REQUIRED_OUTPUT_FILES.items()}

    if skip_existing and all(path.exists() for path in output_paths.values()):
        return {
            "status": "skipped",
            "reason": "all outputs already exist and --skip-existing provided",
            "out_dir": str(output_dir),
        }

    if output_dir.exists() and not overwrite and not skip_existing and any(output_dir.iterdir()):
        raise ValueError(
            f"Output directory exists and is not empty: {output_dir}. Use --overwrite or --skip-existing."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    log = ExtractionLog()
    summary_map = _load_summary_map(experiment_root)
    manifest_rows, manifest_path = _manifest_map(_select_manifest(experiment_root))
    default_hypothesis_id = _load_contract_snapshot_hypothesis(experiment_root)

    run_dirs = _discover_run_dirs(experiment_root, runs_glob)
    if not run_dirs:
        raise ValueError(f"No run directories discovered under {experiment_root} using glob={runs_glob!r}")

    experiment_root_name = experiment_root.name
    experiment_id = experiment_root_name
    dataset_tag = _extract_dataset_tag(experiment_root_name)

    contract_snapshot_dir = experiment_root / "contract_snapshot"
    snapshot_path = None
    if contract_snapshot_dir.exists():
        snapshot_candidates = sorted(contract_snapshot_dir.glob("*.yaml")) + sorted(contract_snapshot_dir.glob("*.yml"))
        if snapshot_candidates:
            snapshot_path = str(snapshot_candidates[0])

    run_rows: list[dict[str, Any]] = []
    trade_dfs: list[pd.DataFrame] = []
    dropped_rows: list[dict[str, Any]] = []

    for run_dir in run_dirs:
        run_id = run_dir.name
        summary_row = summary_map.get(run_id)
        manifest_row = manifest_rows.get(run_id)
        result = _parse_run(
            run_dir,
            experiment_id=experiment_id,
            experiment_root=experiment_root,
            dataset_tag=dataset_tag,
            summary_row=summary_row,
            manifest_row=manifest_row,
            default_hypothesis_id=default_hypothesis_id,
            min_trades_per_run=min_trades_per_run,
            log=log,
        )
        if not result.include:
            dropped_rows.append(
                {
                    "run_id": run_id,
                    "reason": result.dropped_reason or "excluded",
                    "missing_artifact": result.missing_artifact,
                    "notes": result.notes,
                }
            )
            continue

        if result.run_record is None:
            dropped_rows.append(
                {
                    "run_id": run_id,
                    "reason": "internal_missing_run_record",
                    "missing_artifact": "",
                    "notes": "run parser returned include=True but no run record",
                }
            )
            continue

        result.run_record["contract_snapshot_path"] = snapshot_path
        run_rows.append(result.run_record)
        if not result.trades_df.empty:
            trade_dfs.append(result.trades_df)

    runs_df = pd.DataFrame(run_rows)
    if runs_df.empty:
        raise ValueError("No valid run-level rows were produced; see dropped_runs.csv for details.")

    if "run_id" in runs_df.columns:
        runs_df = runs_df.sort_values("run_id").reset_index(drop=True)
    runs_df = _add_run_rankings(runs_df)
    runs_df.attrs["total_runs_scanned"] = len(run_dirs)

    trades_df = pd.concat(trade_dfs, ignore_index=True) if trade_dfs else pd.DataFrame()
    if trades_df.empty:
        raise ValueError(
            "No trade rows were produced (all runs missing/empty trades.csv or below --min-trades-per-run)."
        )

    trades_df = _enrich_trade_labels(trades_df, runs_df, min_trades_per_run=min_trades_per_run)
    trades_df = trades_df.sort_values(["run_id", "entry_time", "trade_id"], na_position="last").reset_index(drop=True)

    if "manifest_row_index" in runs_df.columns:
        runs_df["manifest_row_index"] = pd.to_numeric(runs_df["manifest_row_index"], errors="coerce").astype("Int64")
    if "manifest_row_index" in trades_df.columns:
        trades_df["manifest_row_index"] = pd.to_numeric(trades_df["manifest_row_index"], errors="coerce").astype("Int64")
    if "duration_bars" in trades_df.columns:
        trades_df["duration_bars"] = pd.to_numeric(trades_df["duration_bars"], errors="coerce").round().astype("Int64")

    runs_df, trades_df = _enforce_contract_column_order(runs_df, trades_df)

    for frame in (runs_df, trades_df):
        for col in frame.columns:
            if frame[col].dtype == "object":
                frame[col] = frame[col].where(frame[col].notna(), None)

    runs_df.to_parquet(output_paths["runs_dataset"], index=False)
    trades_df.to_parquet(output_paths["trades_dataset"], index=False)

    pd.DataFrame(dropped_rows, columns=["run_id", "reason", "missing_artifact", "notes"]).to_csv(
        output_paths["dropped_runs"],
        index=False,
    )

    feature_dictionary = _build_feature_dictionary(trades_df, runs_df)
    experiment_summary = _experiment_summary(experiment_root, runs_df, trades_df)

    elapsed_seconds = perf_counter() - start_time

    extraction_log_payload = {
        "missing_artifacts": log.missing_artifacts,
        "columns_skipped": sorted(set(log.columns_skipped)),
        "source_conflicts": sorted(set(log.source_conflicts)),
        "fallback_derivations_used": sorted(set(log.fallback_derivations)),
        "info": log.info,
        "timing": {"elapsed_seconds": round(elapsed_seconds, 4)},
        "row_counts": {
            "runs_scanned": len(run_dirs),
            "runs_parsed": len(runs_df),
            "runs_dropped": len(dropped_rows),
            "trade_rows": len(trades_df),
        },
    }

    dataset_manifest = {
        "experiment_root": str(experiment_root),
        "created_at_utc": _utc_now_iso(),
        "script_version": SCRIPT_VERSION,
        "schema_version": DATASET_SCHEMA_VERSION,
        "hypothesis_id": default_hypothesis_id,
        "runs_scanned": len(run_dirs),
        "runs_parsed": len(runs_df),
        "runs_dropped": len(dropped_rows),
        "trade_rows": len(trades_df),
        "run_rows": len(runs_df),
        "output_files": {name: str(path) for name, path in output_paths.items()},
        "manifest_source": manifest_path,
    }

    output_paths["dataset_manifest"].write_text(
        json.dumps(_clean_for_json(dataset_manifest), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    output_paths["feature_dictionary"].write_text(
        json.dumps(_clean_for_json(feature_dictionary), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    output_paths["experiment_summary"].write_text(
        json.dumps(_clean_for_json(experiment_summary), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    output_paths["extraction_log"].write_text(
        json.dumps(_clean_for_json(extraction_log_payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if verbose:
        print(
            "Extracted datasets",
            json.dumps(
                {
                    "runs_scanned": len(run_dirs),
                    "runs_parsed": len(runs_df),
                    "runs_dropped": len(dropped_rows),
                    "trade_rows": len(trades_df),
                    "out_dir": str(output_dir),
                },
                sort_keys=True,
            ),
        )

    return {
        "status": "ok",
        "out_dir": str(output_dir),
        "runs_scanned": len(run_dirs),
        "runs_parsed": len(runs_df),
        "runs_dropped": len(dropped_rows),
        "trade_rows": len(trades_df),
        "run_rows": len(runs_df),
    }
