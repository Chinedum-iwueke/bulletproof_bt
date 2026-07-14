from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


STATE_COLUMNS = [
    "id", "state_key", "bucket", "joint_key", "setup_class", "hypothesis_name", "dataset_type", "phase",
    "n_trades", "ev_r_net", "ev_r_gross", "median_r", "p95_r", "p99_r", "max_r", "min_r",
    "tail_3r_rate", "tail_5r_rate", "tail_10r_rate", "avg_mfe_r", "avg_mae_r", "avg_exit_efficiency",
    "avg_cost_drag_r", "finding_type", "confidence_score", "evidence_json", "source_path", "created_at",
]


def normalize_state_record(row: dict[str, Any], source_path: Path) -> dict[str, Any]:
    state_key = _first(row, ["state_key", "state_variable", "variable", "feature"]) or "unknown"
    bucket = _first(row, ["bucket", "state_bucket", "value"]) or "unknown"
    joint_key = _first(row, ["joint_key", "interaction_key"])
    raw = json.dumps(row, sort_keys=True, default=str)
    rid = hashlib.sha1(f"{source_path}|{state_key}|{bucket}|{joint_key}|{raw}".encode("utf-8")).hexdigest()
    return {
        "id": rid,
        "state_key": str(state_key),
        "bucket": str(bucket),
        "joint_key": joint_key,
        "setup_class": _first(row, ["setup_class", "entry_decision_setup_class"]),
        "hypothesis_name": _first(row, ["hypothesis_name", "name"]),
        "dataset_type": _first(row, ["dataset_type"]),
        "phase": _first(row, ["phase"]),
        "n_trades": _int(_first(row, ["n_trades", "count"])),
        "ev_r_net": _num(_first(row, ["ev_r_net", "mean_r_net", "r_net_mean"])),
        "ev_r_gross": _num(_first(row, ["ev_r_gross", "mean_r_gross"])),
        "median_r": _num(_first(row, ["median_r"])),
        "p95_r": _num(_first(row, ["p95_r"])),
        "p99_r": _num(_first(row, ["p99_r"])),
        "max_r": _num(_first(row, ["max_r"])),
        "min_r": _num(_first(row, ["min_r"])),
        "tail_3r_rate": _num(_first(row, ["tail_3r_rate"])),
        "tail_5r_rate": _num(_first(row, ["tail_5r_rate"])),
        "tail_10r_rate": _num(_first(row, ["tail_10r_rate"])),
        "avg_mfe_r": _num(_first(row, ["avg_mfe_r", "mfe_r_mean"])),
        "avg_mae_r": _num(_first(row, ["avg_mae_r", "mae_r_mean"])),
        "avg_exit_efficiency": _num(_first(row, ["avg_exit_efficiency", "exit_efficiency_mean"])),
        "avg_cost_drag_r": _num(_first(row, ["avg_cost_drag_r", "cost_drag_r_mean"])),
        "finding_type": _first(row, ["finding_type", "type"]),
        "confidence_score": _num(_first(row, ["confidence_score", "confidence"])),
        "evidence_json": raw,
        "source_path": str(source_path),
        "created_at": _now(),
    }


def aggregate_buckets_from_trades(conn, min_trades: int = 1) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    bucket_specs: list[tuple[str, str]] = [
        ("csi_pctile", _pct_bucket_sql("csi_pctile")),
        ("vol_pctile", _pct_bucket_sql("vol_pctile")),
        ("spread_pctile", _pct_bucket_sql("spread_pctile")),
        ("tr_over_atr", _tr_bucket_sql("tr_over_atr")),
        ("setup_class", "CASE WHEN setup_class IS NULL OR setup_class = '' THEN NULL ELSE setup_class END"),
    ]
    for state_key, bucket_expr in bucket_specs:
        sql = f"""
            WITH bucketed AS (
                SELECT
                    {bucket_expr} AS bucket,
                    setup_class,
                    hypothesis_name,
                    dataset_type,
                    phase,
                    run_id,
                    r_net,
                    r_gross,
                    mfe_r,
                    mae_r,
                    exit_efficiency,
                    cost_drag_r
                FROM research_memory_trades
                WHERE metrics_valid = 1 AND r_net IS NOT NULL
            )
            SELECT
                bucket,
                setup_class,
                hypothesis_name,
                dataset_type,
                phase,
                COUNT(*) AS n_trades,
                AVG(r_net) AS ev_r_net,
                AVG(r_gross) AS ev_r_gross,
                MAX(r_net) AS max_r,
                MIN(r_net) AS min_r,
                AVG(CASE WHEN r_net >= 3 THEN 1.0 ELSE 0.0 END) AS tail_3r_rate,
                AVG(CASE WHEN r_net >= 5 THEN 1.0 ELSE 0.0 END) AS tail_5r_rate,
                AVG(CASE WHEN r_net >= 10 THEN 1.0 ELSE 0.0 END) AS tail_10r_rate,
                AVG(mfe_r) AS avg_mfe_r,
                AVG(mae_r) AS avg_mae_r,
                AVG(exit_efficiency) AS avg_exit_efficiency,
                AVG(cost_drag_r) AS avg_cost_drag_r,
                COUNT(DISTINCT run_id) AS run_count
            FROM bucketed
            WHERE bucket IS NOT NULL
            GROUP BY bucket, setup_class, hypothesis_name, dataset_type, phase
            HAVING COUNT(*) >= ?
        """
        for db_row in conn.execute(sql, (min_trades,)).fetchall():
            row = {
                "n_trades": int(db_row["n_trades"]),
                "ev_r_net": _num(db_row["ev_r_net"]),
                "ev_r_gross": _num(db_row["ev_r_gross"]),
                "median_r": None,
                "p95_r": None,
                "p99_r": None,
                "max_r": _num(db_row["max_r"]),
                "min_r": _num(db_row["min_r"]),
                "tail_3r_rate": _num(db_row["tail_3r_rate"]),
                "tail_5r_rate": _num(db_row["tail_5r_rate"]),
                "tail_10r_rate": _num(db_row["tail_10r_rate"]),
                "avg_mfe_r": _num(db_row["avg_mfe_r"]),
                "avg_mae_r": _num(db_row["avg_mae_r"]),
                "avg_exit_efficiency": _num(db_row["avg_exit_efficiency"]),
                "avg_cost_drag_r": _num(db_row["avg_cost_drag_r"]),
            }
            row.update({
                "state_key": state_key,
                "bucket": str(db_row["bucket"]),
                "setup_class": db_row["setup_class"],
                "hypothesis_name": db_row["hypothesis_name"],
                "dataset_type": db_row["dataset_type"],
                "phase": db_row["phase"],
                "joint_key": None,
                "finding_type": _finding_type(row),
                "confidence_score": min(1.0, int(db_row["n_trades"]) / 30.0),
                "source_path": "trade_aggregate",
                "created_at": _now(),
            })
            rid = hashlib.sha1(json.dumps(row, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            row["id"] = rid
            row["evidence_json"] = json.dumps({"source": "trade_aggregate", "run_count": int(db_row["run_count"]), **row}, sort_keys=True, default=str)
            records.append(row)
    return records


def insert_state_buckets(conn, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    placeholders = ", ".join(["?"] * len(STATE_COLUMNS))
    columns = ", ".join(STATE_COLUMNS)
    updates = ", ".join([f"{c}=excluded.{c}" for c in STATE_COLUMNS if c != "id"])
    sql = f"INSERT INTO research_memory_state_buckets ({columns}) VALUES ({placeholders}) ON CONFLICT(id) DO UPDATE SET {updates}"
    conn.executemany(sql, [[rec.get(c) for c in STATE_COLUMNS] for rec in records])
    return len(records)


def load_state_file(path: Path) -> list[dict[str, Any]]:
    try:
        if path.suffix == ".csv":
            rows = pd.read_csv(path).to_dict("records")
        elif path.suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            rows = data if isinstance(data, list) else data.get("findings", data.get("state_findings", []))
            if isinstance(rows, dict):
                rows = [rows]
        else:
            return []
        return [normalize_state_record(dict(row), path) for row in rows]
    except Exception:
        return []


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    r = group["r_net"].astype(float)
    return {
        "n_trades": int(len(group)),
        "ev_r_net": float(r.mean()),
        "ev_r_gross": _mean(group, "r_gross"),
        "median_r": float(r.median()),
        "p95_r": float(r.quantile(0.95)),
        "p99_r": float(r.quantile(0.99)),
        "max_r": float(r.max()),
        "min_r": float(r.min()),
        "tail_3r_rate": float((r >= 3).mean()),
        "tail_5r_rate": float((r >= 5).mean()),
        "tail_10r_rate": float((r >= 10).mean()),
        "avg_mfe_r": _mean(group, "mfe_r"),
        "avg_mae_r": _mean(group, "mae_r"),
        "avg_exit_efficiency": _mean(group, "exit_efficiency"),
        "avg_cost_drag_r": _mean(group, "cost_drag_r"),
    }


def _finding_type(row: dict[str, Any]) -> str:
    if (row.get("ev_r_net") or 0) > 0.15:
        return "POSITIVE_EDGE_STATE"
    if (row.get("ev_r_net") or 0) < -0.10:
        return "NEGATIVE_EDGE_STATE"
    return "NEUTRAL_STATE"


def _pct_bucket(v: Any) -> str | None:
    x = _num(v)
    if x is None:
        return None
    if x >= 0.85:
        return "very_high"
    if x >= 0.70:
        return "high"
    if x >= 0.30:
        return "mid"
    if x >= 0.15:
        return "low"
    return "very_low"


def _tr_bucket(v: Any) -> str | None:
    x = _num(v)
    if x is None:
        return None
    if x >= 2.0:
        return "very_high"
    if x >= 1.5:
        return "high"
    if x >= 0.75:
        return "mid"
    return "low"


def _pct_bucket_sql(column: str) -> str:
    return f"""
        CASE
            WHEN {column} IS NULL THEN NULL
            WHEN {column} >= 0.85 THEN 'very_high'
            WHEN {column} >= 0.70 THEN 'high'
            WHEN {column} >= 0.30 THEN 'mid'
            WHEN {column} >= 0.15 THEN 'low'
            ELSE 'very_low'
        END
    """


def _tr_bucket_sql(column: str) -> str:
    return f"""
        CASE
            WHEN {column} IS NULL THEN NULL
            WHEN {column} >= 2.0 THEN 'very_high'
            WHEN {column} >= 1.5 THEN 'high'
            WHEN {column} >= 0.75 THEN 'mid'
            ELSE 'low'
        END
    """


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if col not in df.columns:
        return None
    val = pd.to_numeric(df[col], errors="coerce").mean()
    return None if pd.isna(val) else float(val)


def _first(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        val = row.get(key)
        if val is not None and val == val and val != "":
            return val
    return None


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "" or value != value:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    x = _num(value)
    return None if x is None else int(x)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
