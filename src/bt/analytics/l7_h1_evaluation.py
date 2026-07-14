"""L7-H1 signal-feature extraction and bucket evaluation artifacts."""
from __future__ import annotations

import json
import math
import csv
from pathlib import Path
from typing import Any

import pandas as pd


def _num(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _metadata_from_decision(row: dict[str, Any]) -> dict[str, Any]:
    signal = row.get("signal")
    if not isinstance(signal, dict):
        return {}
    metadata = signal.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def write_signal_feature_artifact(run_dir: str | Path) -> Path | None:
    run_path = Path(run_dir)
    decisions_path = run_path / "decisions.jsonl"
    if not decisions_path.exists():
        return None

    feature_keys = {
        "CSI",
        "CSI_raw",
        "D_t",
        "ATR_14",
        "S_t",
        "oi_z",
        "volume_z",
        "funding_pct",
        "basis_pct",
        "recent_return_1",
        "recent_return_5",
        "volatility_20",
        "state_vector",
    }
    def iter_rows():
        with decisions_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                metadata = _metadata_from_decision(payload)
                if not feature_keys.intersection(metadata):
                    continue
                signal = payload.get("signal") if isinstance(payload.get("signal"), dict) else {}
                row = {
                    "ts": payload.get("ts"),
                    "symbol": payload.get("symbol") or signal.get("symbol"),
                    "signal_type": signal.get("signal_type"),
                    "approved": payload.get("approved"),
                    "reason": payload.get("reason"),
                }
                for key, value in metadata.items():
                    if key in feature_keys or key.startswith(("entry_state_", "csi_component_")):
                        row[key] = (
                            value
                            if isinstance(value, (str, int, float, bool)) or value is None
                            else json.dumps(value, sort_keys=True, default=str)
                        )
                yield row

    base_columns = ["ts", "symbol", "signal_type", "approved", "reason"]
    extra_columns: set[str] = set()
    row_count = 0
    for row in iter_rows():
        row_count += 1
        extra_columns.update(key for key in row if key not in base_columns)

    if row_count == 0:
        return None

    out = run_path / "signal_features.csv"
    fieldnames = base_columns + sorted(extra_columns)
    with out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in iter_rows():
            writer.writerow(row)
    return out


def _ccdf_slope(r_values: pd.Series) -> float | None:
    r = pd.to_numeric(r_values, errors="coerce").dropna()
    r = r[r > 0].sort_values()
    if len(r) < 3:
        return None
    thresholds = r.quantile([0.5, 0.75, 0.9]).drop_duplicates()
    xs: list[float] = []
    ys: list[float] = []
    for threshold in thresholds:
        if threshold <= 0:
            continue
        prob = float((r >= threshold).mean())
        if prob <= 0:
            continue
        xs.append(math.log(float(threshold)))
        ys.append(math.log(prob))
    if len(xs) < 2:
        return None
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom <= 0:
        return None
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denom


def _drawdown_duration_from_r(r_values: pd.Series) -> int:
    r = pd.to_numeric(r_values, errors="coerce").fillna(0.0)
    if r.empty:
        return 0
    equity = r.cumsum()
    peak = equity.cummax()
    dd = equity - peak
    longest = 0
    current = 0
    for value in dd:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return int(longest)


def _bucketize_csi(series: pd.Series) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    return pd.cut(
        vals,
        bins=[-0.001, 0.5, 0.7, 0.85, 1.001],
        labels=["csi_low", "csi_mid", "csi_high", "csi_extreme"],
    ).astype("string")


def write_l7_h1_evaluation_artifacts(run_dir: str | Path, *, tier: str | None = None) -> dict[str, str]:
    run_path = Path(run_dir)
    trades_path = run_path / "trades.csv"
    if not trades_path.exists():
        return {}
    try:
        trades = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame()
    if trades.empty:
        return {}

    r_col = "r_net" if "r_net" in trades.columns else ("realized_r_net" if "realized_r_net" in trades.columns else "r_multiple_net")
    if r_col not in trades.columns:
        return {}
    r = pd.to_numeric(trades[r_col], errors="coerce")
    csi_col = "entry_state_csi_pctile" if "entry_state_csi_pctile" in trades.columns else "CSI" if "CSI" in trades.columns else None
    if csi_col is not None:
        trades["_l7_csi_bucket"] = _bucketize_csi(trades[csi_col])
    else:
        trades["_l7_csi_bucket"] = "all"

    rows: list[dict[str, Any]] = []
    for bucket, part in trades.groupby("_l7_csi_bucket", dropna=False):
        part_r = pd.to_numeric(part[r_col], errors="coerce")
        rows.append(
            {
                "tier": tier,
                "bucket": str(bucket),
                "n_trades": int(len(part)),
                "EV_r_net": float(part_r.mean()) if part_r.notna().any() else None,
                "count_ge_10R": int((part_r >= 10.0).sum()),
                "max_R": float(part_r.max()) if part_r.notna().any() else None,
                "CCDF_slope": _ccdf_slope(part_r),
                "drawdown_duration_trades": _drawdown_duration_from_r(part_r),
            }
        )

    bucket_path = run_path / "l7_h1_bucket_report.csv"
    pd.DataFrame(rows).to_csv(bucket_path, index=False)

    summary = {
        "tier": tier,
        "metrics": {
            "count_over_10R": int((r >= 10.0).sum()),
            "max_R": float(r.max()) if r.notna().any() else None,
            "right_tail_CCDF_slope": _ccdf_slope(r),
            "ev_r_tier2": float(r.mean()) if str(tier).lower() == "tier2" and r.notna().any() else None,
            "ev_r_tier3": float(r.mean()) if str(tier).lower() == "tier3" and r.notna().any() else None,
            "drawdown_duration": _drawdown_duration_from_r(r),
            "capacity_proxy": _num(trades.get("notional_est", pd.Series(dtype="float64")).median()),
        },
    }
    summary_path = run_path / "l7_h1_evaluation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return {"l7_h1_bucket_report": str(bucket_path), "l7_h1_evaluation_summary": str(summary_path)}
