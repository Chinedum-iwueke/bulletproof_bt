from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

import pandas as pd


RECOMMENDATION_TYPES = {
    "ADD_GATE", "REMOVE_GATE", "TIGHTEN_GATE", "LOOSEN_GATE", "CHANGE_SIZING", "REFINE_EXIT",
    "REFINE_ENTRY", "PROMOTE_TIER3", "AVOID_STATE", "WATCHLIST", "SCRAP_PATTERN", "TEST_NEXT",
    "ADD_FUNDING_GATE", "ADD_OI_ACCEL_GATE", "ADD_BASIS_GATE", "ADD_CONSTRAINT_STRESS_GATE",
    "AVOID_FUNDING_EXTREME", "SIZE_UP_CONSTRAINT_STRESS", "SIZE_DOWN_DERIVATIVE_FRAGILITY",
}


def generate_recommendations(conn: sqlite3.Connection, *, min_trades: int = 3) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    buckets = pd.read_sql_query("SELECT * FROM research_memory_state_buckets WHERE n_trades >= ?", conn, params=(min_trades,))
    if not buckets.empty:
        recs.extend(_bucket_recommendations(buckets))
    recs.extend(_trade_recommendations_from_db(conn, min_trades=min_trades))
    candidates = pd.read_sql_query("SELECT * FROM research_memory_candidates", conn)
    if not candidates.empty:
        recs.extend(_candidate_recommendations(candidates))
    recs = _dedupe(recs)
    write_recommendations(conn, recs)
    return recs


def write_recommendations(conn: sqlite3.Connection, recs: list[dict[str, Any]]) -> None:
    if not recs:
        return
    cols = [
        "id", "recommendation_type", "target_type", "target_id", "hypothesis_name", "setup_class",
        "state_condition_json", "recommendation", "evidence_score", "confidence", "status",
        "human_approved", "evidence_json", "created_at", "updated_at",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    updates = ", ".join([f"{c}=excluded.{c}" for c in cols if c not in {"id", "created_at"}])
    sql = f"INSERT INTO research_memory_recommendations ({', '.join(cols)}) VALUES ({placeholders}) ON CONFLICT(id) DO UPDATE SET {updates}"
    conn.executemany(sql, [[rec.get(c) for c in cols] for rec in recs])
    conn.commit()


def _bucket_recommendations(df: pd.DataFrame) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    for row in df.to_dict("records"):
        ev = _num(row.get("ev_r_net")) or 0
        n = int(row.get("n_trades") or 0)
        cost = _num(row.get("avg_cost_drag_r")) or 0
        state = {"state_key": row.get("state_key"), "bucket": row.get("bucket"), "joint_key": row.get("joint_key")}
        repeated = _repeat_count(df, row)
        if ev >= 0.15 and n >= 3 and repeated >= 1:
            state_key = str(row.get("state_key") or "")
            rec_kind = "ADD_GATE"
            if "funding" in state_key:
                rec_kind = "ADD_FUNDING_GATE"
            elif "oi" in state_key:
                rec_kind = "ADD_OI_ACCEL_GATE"
            elif "basis" in state_key:
                rec_kind = "ADD_BASIS_GATE"
            elif "constraint" in state_key:
                rec_kind = "ADD_CONSTRAINT_STRESS_GATE"
            text = f"Add gate favoring {row.get('state_key')}={row.get('bucket')}"
            if row.get("setup_class"):
                text += f" for {row.get('setup_class')} setups"
            recs.append(_rec(rec_kind, "state_bucket", row.get("id"), text + ".", row, state, min(1, ev), min(1, n / 30)))
        if ev <= -0.10 and n >= 3:
            rec_kind = "AVOID_FUNDING_EXTREME" if "funding" in str(row.get("state_key") or "") else "AVOID_STATE"
            recs.append(_rec(rec_kind, "state_bucket", row.get("id"), f"Block or avoid trades when {row.get('state_key')}={row.get('bucket')}.", row, state, abs(ev), min(1, n / 30)))
        if ev > 0 and cost >= 0.35:
            rec_kind = "SIZE_DOWN_DERIVATIVE_FRAGILITY" if any(token in str(row.get("state_key") or "") for token in ("funding", "oi", "basis", "constraint")) else "CHANGE_SIZING"
            recs.append(_rec(rec_kind, "state_bucket", row.get("id"), f"Reduce size in {row.get('state_key')}={row.get('bucket')} because edge is cost-fragile.", row, state, cost, min(1, n / 30)))
        if "constraint" in str(row.get("state_key") or "") and ev >= 0.20 and cost < 0.25:
            recs.append(_rec("SIZE_UP_CONSTRAINT_STRESS", "state_bucket", row.get("id"), f"Review controlled size-up in {row.get('state_key')}={row.get('bucket')}; evidence is positive after costs.", row, state, ev, min(1, n / 30)))
        if (row.get("avg_mfe_r") or 0) >= 1.5 and (row.get("avg_exit_efficiency") or 1) <= 0.35 and ev < 0.25:
            recs.append(_rec("REFINE_EXIT", "state_bucket", row.get("id"), f"Test trailing or chandelier exits for {row.get('state_key')}={row.get('bucket')}.", row, state, row.get("avg_mfe_r") or 0, min(1, n / 30)))
    return recs


def _trade_recommendations(df: pd.DataFrame, *, min_trades: int) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    group_cols = ["setup_class", "hypothesis_name"]
    for keys, group in df.groupby(group_cols, dropna=False):
        if len(group) < min_trades:
            continue
        setup_class, hypothesis_name = keys
        r = pd.to_numeric(group["r_net"], errors="coerce")
        mfe = pd.to_numeric(group["mfe_r"], errors="coerce")
        exit_eff = pd.to_numeric(group["exit_efficiency"], errors="coerce")
        evidence = {
            "setup_class": None if pd.isna(setup_class) else setup_class,
            "hypothesis_name": None if pd.isna(hypothesis_name) else hypothesis_name,
            "n_trades": int(len(group)),
            "ev_r_net": float(r.mean()),
            "avg_mfe_r": None if pd.isna(mfe.mean()) else float(mfe.mean()),
            "avg_exit_efficiency": None if pd.isna(exit_eff.mean()) else float(exit_eff.mean()),
            "tail_5r_rate": float((r >= 5).mean()),
        }
        if evidence["avg_mfe_r"] and evidence["avg_mfe_r"] >= 1.5 and (evidence["avg_exit_efficiency"] or 1) <= 0.35 and evidence["ev_r_net"] < 0.25:
            recs.append(_rec("REFINE_EXIT", "setup_class", str(setup_class), "Good entry excursion but poor realized exits. Test trailing, time-stop, or partial exit variants.", evidence, {"setup_class": evidence["setup_class"]}, evidence["avg_mfe_r"], min(1, len(group) / 30)))
        if evidence["ev_r_net"] < -0.15 and evidence["avg_mfe_r"] is not None and evidence["avg_mfe_r"] < 0.75 and evidence["tail_5r_rate"] == 0:
            recs.append(_rec("SCRAP_PATTERN", "setup_class", str(setup_class), "Repeated negative results with weak MFE and no tail generation. Consider scrapping this setup family.", evidence, {"setup_class": evidence["setup_class"]}, abs(evidence["ev_r_net"]), min(1, len(group) / 30)))
    return recs


def _trade_recommendations_from_db(conn: sqlite3.Connection, *, min_trades: int) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    rows = conn.execute(
        """
        SELECT
            setup_class,
            hypothesis_name,
            COUNT(*) AS n_trades,
            AVG(r_net) AS ev_r_net,
            AVG(mfe_r) AS avg_mfe_r,
            AVG(exit_efficiency) AS avg_exit_efficiency,
            AVG(CASE WHEN r_net >= 5 THEN 1.0 ELSE 0.0 END) AS tail_5r_rate
        FROM research_memory_trades
        WHERE metrics_valid = 1 AND r_net IS NOT NULL
        GROUP BY setup_class, hypothesis_name
        HAVING COUNT(*) >= ?
        """,
        (min_trades,),
    ).fetchall()
    for row in rows:
        evidence = {
            "setup_class": row["setup_class"],
            "hypothesis_name": row["hypothesis_name"],
            "n_trades": int(row["n_trades"]),
            "ev_r_net": _num(row["ev_r_net"]) or 0.0,
            "avg_mfe_r": _num(row["avg_mfe_r"]),
            "avg_exit_efficiency": _num(row["avg_exit_efficiency"]),
            "tail_5r_rate": _num(row["tail_5r_rate"]) or 0.0,
        }
        if evidence["avg_mfe_r"] and evidence["avg_mfe_r"] >= 1.5 and (evidence["avg_exit_efficiency"] or 1) <= 0.35 and evidence["ev_r_net"] < 0.25:
            recs.append(_rec("REFINE_EXIT", "setup_class", str(evidence["setup_class"]), "Good entry excursion but poor realized exits. Test trailing, time-stop, or partial exit variants.", evidence, {"setup_class": evidence["setup_class"]}, evidence["avg_mfe_r"], min(1, evidence["n_trades"] / 30)))
        if evidence["ev_r_net"] < -0.15 and evidence["avg_mfe_r"] is not None and evidence["avg_mfe_r"] < 0.75 and evidence["tail_5r_rate"] == 0:
            recs.append(_rec("SCRAP_PATTERN", "setup_class", str(evidence["setup_class"]), "Repeated negative results with weak MFE and no tail generation. Consider scrapping this setup family.", evidence, {"setup_class": evidence["setup_class"]}, abs(evidence["ev_r_net"]), min(1, evidence["n_trades"] / 30)))
    return recs


def _candidate_recommendations(candidates: pd.DataFrame) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    for row in candidates.to_dict("records"):
        ev = _num(row.get("ev_r_net"))
        n = int(row.get("n_trades") or 0)
        score = _num(row.get("promotion_score")) or _num(row.get("rank_score")) or 0
        if (ev is not None and ev > 0.15 and n >= 3 and score >= 0.5) or row.get("recommended_action") == "PROMOTE_TIER3":
            recs.append(_rec("PROMOTE_TIER3", "candidate", row.get("candidate_id") or row.get("run_id"), "Review this alpha candidate for Tier3 promotion. Human approval required.", row, {"candidate_id": row.get("candidate_id"), "run_id": row.get("run_id")}, score or ev or 0, min(1, n / 30) if n else score))
    return recs


def _rec(kind: str, target_type: str, target_id: Any, text: str, evidence: dict[str, Any], state: dict[str, Any], score: float, confidence: float) -> dict[str, Any]:
    assert kind in RECOMMENDATION_TYPES
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    payload = {
        "recommendation_type": kind,
        "target_type": target_type,
        "target_id": None if target_id is None else str(target_id),
        "hypothesis_name": evidence.get("hypothesis_name"),
        "setup_class": evidence.get("setup_class"),
        "state_condition_json": json.dumps(state, sort_keys=True, default=str),
        "recommendation": text,
        "evidence_score": float(score or 0),
        "confidence": float(confidence or 0),
        "status": "PROPOSED",
        "human_approved": 0,
        "evidence_json": json.dumps(evidence, sort_keys=True, default=str),
        "created_at": now,
        "updated_at": now,
    }
    payload["id"] = hashlib.sha1(json.dumps({k: payload[k] for k in ["recommendation_type", "target_type", "target_id", "state_condition_json", "recommendation"]}, sort_keys=True).encode("utf-8")).hexdigest()
    return payload


def _dedupe(recs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list({rec["id"]: rec for rec in recs}.values())


def _repeat_count(df: pd.DataFrame, row: dict[str, Any]) -> int:
    mask = (df["state_key"] == row.get("state_key")) & (df["bucket"] == row.get("bucket"))
    return int(df.loc[mask, "hypothesis_name"].nunique())


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "" or value != value:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
