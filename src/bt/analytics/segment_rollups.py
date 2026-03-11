"""Reusable metadata-segment rollups from run artifacts."""
from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from bt.metrics.r_metrics import summarize_r

SEGMENT_ROLLUPS_SCHEMA_VERSION = 1
_MISSING = "__MISSING__"


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _normalize_segment_value(value: Any) -> Any:
    if value is None:
        return _MISSING
    if isinstance(value, float):
        if not math.isfinite(value):
            return _MISSING
        return float(value)
    if isinstance(value, (bool, int, str)):
        return value
    return str(value)


def _key_tuple(*, symbol: str, ts: str, side: str) -> tuple[str, str, str]:
    return symbol.strip(), ts.strip(), side.strip()


def _parse_entry_metadata_map(fills_path: Path) -> dict[tuple[str, str, str], dict[str, Any]]:
    if not fills_path.exists():
        raise ValueError(f"run_dir is missing fills.jsonl: {fills_path}")

    entry_meta: dict[tuple[str, str, str], dict[str, Any]] = {}
    with fills_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                continue
            metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            if bool(metadata.get("close_only") or metadata.get("reduce_only")):
                continue
            key = _key_tuple(
                symbol=str(payload.get("symbol", "")),
                ts=str(payload.get("ts", "")),
                side=str(payload.get("side", "")),
            )
            if key in entry_meta:
                continue
            entry_meta[key] = dict(metadata)
    return entry_meta


def load_trades_with_entry_metadata(run_dir: str | Path, *, required_segment_keys: list[str] | None = None) -> pd.DataFrame:
    """Load trades enriched with `entry_meta__*` columns sourced from fills entry metadata."""
    run_path = Path(run_dir)
    trades_path = run_path / "trades.csv"
    fills_path = run_path / "fills.jsonl"
    if not trades_path.exists():
        raise ValueError(f"run_dir is missing trades.csv: {trades_path}")

    try:
        trades_df = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades_df = pd.DataFrame()

    if trades_df.empty:
        return trades_df

    for col in ("symbol", "entry_ts", "side"):
        if col not in trades_df.columns:
            raise ValueError(f"trades.csv missing required column '{col}' for metadata join")

    entry_map = _parse_entry_metadata_map(fills_path)
    enriched = trades_df.copy()

    metadata_rows: list[dict[str, Any]] = []
    for row in enriched.itertuples(index=False):
        key = _key_tuple(symbol=str(getattr(row, "symbol", "")), ts=str(getattr(row, "entry_ts", "")), side=str(getattr(row, "side", "")))
        metadata_rows.append(entry_map.get(key, {}))

    all_keys = sorted({k for md in metadata_rows for k in md.keys()})
    for key in all_keys:
        enriched[f"entry_meta__{key}"] = [_normalize_segment_value(md.get(key)) for md in metadata_rows]

    if required_segment_keys:
        missing = [key for key in required_segment_keys if f"entry_meta__{key}" not in enriched.columns]
        if missing:
            raise ValueError(f"Missing requested segment keys in entry metadata: {missing}")

    return enriched


def _segment_series(df: pd.DataFrame, key: str) -> pd.Series:
    column = f"entry_meta__{key}"
    if column not in df.columns:
        raise ValueError(f"Segment key '{key}' not present in enriched trades")
    return df[column].apply(_normalize_segment_value)


def _compute_segment_metrics(segment: pd.DataFrame) -> dict[str, Any]:
    n_trades = int(segment.shape[0])
    if "r_multiple_net" not in segment.columns:
        raise ValueError("Segment rollups require trades.csv column 'r_multiple_net'")
    r_net = _coerce_numeric(segment["r_multiple_net"])
    r_summary = summarize_r(r_net.tolist())

    hold = _coerce_numeric(segment["hold_bars"]) if "hold_bars" in segment.columns else pd.Series(dtype=float)
    pnl_net = _coerce_numeric(segment["pnl_net"]) if "pnl_net" in segment.columns else pd.Series(dtype=float)

    return {
        "n_trades": n_trades,
        "ev_r_net": float(r_summary.ev_r) if r_summary.ev_r is not None else None,
        "win_rate": float(r_summary.win_rate) if r_summary.win_rate is not None else None,
        "avg_win_r": float(r_summary.avg_r_win) if r_summary.avg_r_win is not None else None,
        "avg_loss_r": float(r_summary.avg_r_loss) if r_summary.avg_r_loss is not None else None,
        "payoff_ratio": float(r_summary.payoff_ratio_r) if r_summary.payoff_ratio_r is not None else None,
        "avg_hold_bars": float(hold.mean()) if not hold.dropna().empty else None,
        "pnl_net": float(pnl_net.sum()) if not pnl_net.dropna().empty else None,
        "max_loss_r": float(r_net.min()) if not r_net.dropna().empty else None,
    }


def compute_segment_rollups(
    trades_with_metadata: pd.DataFrame,
    *,
    segment_keys: list[str],
    source_run_dir: str | None = None,
    hypothesis_id: str | None = None,
) -> list[dict[str, Any]]:
    if not segment_keys:
        raise ValueError("segment_keys cannot be empty")
    if trades_with_metadata.empty:
        return []

    df = trades_with_metadata.copy()
    group_columns: list[str] = []
    for key in segment_keys:
        group_col = f"_segment__{key}"
        df[group_col] = _segment_series(df, key)
        group_columns.append(group_col)

    grouped = df.groupby(group_columns, dropna=False, sort=True)
    rows: list[dict[str, Any]] = []
    for group_values, frame in grouped:
        if not isinstance(group_values, tuple):
            group_values = (group_values,)
        metrics = _compute_segment_metrics(frame)
        segment_dict = {segment_keys[idx]: _normalize_segment_value(group_values[idx]) for idx in range(len(segment_keys))}
        rows.append(
            {
                "schema_version": SEGMENT_ROLLUPS_SCHEMA_VERSION,
                "grouping_keys": json.dumps(segment_keys, sort_keys=True),
                "segment_value_json": json.dumps(segment_dict, sort_keys=True),
                "source_run_dir": source_run_dir,
                "hypothesis_id": hypothesis_id,
                **metrics,
            }
        )

    rows.sort(key=lambda item: (str(item["grouping_keys"]), str(item["segment_value_json"])))
    return rows


def write_segment_rollup_artifacts(run_dir: str | Path, rows: list[dict[str, Any]]) -> tuple[Path, Path]:
    run_path = Path(run_dir)
    csv_path = run_path / "segment_rollups.csv"
    jsonl_path = run_path / "segment_rollups.jsonl"

    fields = [
        "schema_version",
        "grouping_keys",
        "segment_value_json",
        "source_run_dir",
        "hypothesis_id",
        "n_trades",
        "ev_r_net",
        "win_rate",
        "avg_win_r",
        "avg_loss_r",
        "payoff_ratio",
        "avg_hold_bars",
        "pnl_net",
        "max_loss_r",
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fields})

    with jsonl_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    return csv_path, jsonl_path


def default_segment_keys_for_run(run_dir: str | Path) -> list[str]:
    run_path = Path(run_dir)
    config_path = run_path / "config_used.yaml"
    if not config_path.exists():
        return ["entry_reason"]
    import yaml

    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    strategy_name = ""
    if isinstance(config, dict) and isinstance(config.get("strategy"), dict):
        strategy_name = str(config["strategy"].get("name", ""))

    if strategy_name == "l1_h2_compression_mean_reversion":
        return ["entry_reason", "q_comp"]
    if strategy_name == "l1_h1_vol_floor_trend":
        return ["gate_pass"]
    if strategy_name == "l1_h3_har_rv_gate_trend":
        return ["rvhat_pct_t"]
    if strategy_name == "l1_h3b_har_rv_gate_mean_reversion":
        return ["rvhat_pct_t", "fit_window_days"]
    return ["entry_reason"]


def build_run_segment_rollups(run_dir: str | Path, *, segment_keys: list[str] | None = None, hypothesis_id: str | None = None) -> list[dict[str, Any]]:
    keys = list(segment_keys) if segment_keys else default_segment_keys_for_run(run_dir)
    enriched = load_trades_with_entry_metadata(run_dir, required_segment_keys=keys)
    rows = compute_segment_rollups(enriched, segment_keys=keys, source_run_dir=str(Path(run_dir)), hypothesis_id=hypothesis_id)
    write_segment_rollup_artifacts(run_dir, rows)
    return rows
