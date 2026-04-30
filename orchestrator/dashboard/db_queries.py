from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def rows(conn: sqlite3.Connection, q: str, args: tuple = ()) -> list[dict]:
    return [dict(r) for r in conn.execute(q, args).fetchall()]


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    r = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return r is not None


def get_summary(conn: sqlite3.Connection) -> dict:
    def c(q: str): return conn.execute(q).fetchone()[0]
    return {
        "pending_approved_backtests": c("SELECT COUNT(*) FROM queues WHERE queue_name='approved_backtests' AND status='PENDING'"),
        "locked_running_jobs": c("SELECT COUNT(*) FROM queues WHERE status IN ('LOCKED','RUNNING')"),
        "failed_jobs": c("SELECT COUNT(*) FROM queues WHERE status='FAILED'"),
        "completed_pipeline_runs": c("SELECT COUNT(*) FROM pipeline_runs WHERE status IN ('COMPLETED','DONE')"),
        "latest_verdicts": c("SELECT COUNT(*) FROM verdicts WHERE datetime(created_at) >= datetime('now','-7 day')"),
        "tier3_candidates": c("SELECT COUNT(*) FROM verdicts WHERE recommended_next_action='PROMOTE_TIER3'"),
        "alpha_zoo_candidates": c("SELECT COUNT(*) FROM verdicts WHERE recommended_next_action='ADD_TO_ALPHA_ZOO'"),
        "state_findings_count": c("SELECT COUNT(*) FROM state_findings"),
    }


def get_queue(conn, queue_name=None, status=None):
    q = "SELECT * FROM queues WHERE 1=1"
    args = []
    if queue_name:
        q += " AND queue_name=?"; args.append(queue_name)
    if status:
        q += " AND status=?"; args.append(status)
    q += " ORDER BY priority DESC, created_at DESC"
    return rows(conn, q, tuple(args))


def get_hypotheses(conn):
    return rows(conn, "SELECT * FROM hypotheses ORDER BY updated_at DESC")

def get_hypothesis_detail(conn, hid: str):
    h = conn.execute("SELECT * FROM hypotheses WHERE id=?", (hid,)).fetchone()
    if not h:
        return None
    return {"hypothesis": dict(h), "experiments": rows(conn, "SELECT * FROM experiments WHERE hypothesis_id=? ORDER BY created_at DESC", (hid,)), "pipeline_runs": rows(conn, "SELECT * FROM pipeline_runs WHERE hypothesis_id=? ORDER BY started_at DESC", (hid,)), "verdicts": rows(conn, "SELECT * FROM verdicts WHERE hypothesis_id=? ORDER BY created_at DESC", (hid,)), "artifacts": rows(conn, "SELECT * FROM artifacts WHERE hypothesis_id=? ORDER BY created_at DESC", (hid,)), "state_findings": rows(conn, "SELECT * FROM state_findings WHERE hypothesis_id=? ORDER BY updated_at DESC", (hid,))}

def get_pipeline_runs(conn): return rows(conn, "SELECT * FROM pipeline_runs ORDER BY started_at DESC")

def get_verdicts(conn):
    return rows(conn, "SELECT v.*, h.name as hypothesis_name FROM verdicts v LEFT JOIN hypotheses h on h.id=v.hypothesis_id ORDER BY v.created_at DESC")

def get_verdict_detail(conn, vid: str):
    r = conn.execute("SELECT v.*, h.name as hypothesis_name FROM verdicts v LEFT JOIN hypotheses h on h.id=v.hypothesis_id WHERE v.id=?", (vid,)).fetchone()
    if not r: return None
    d = dict(r)
    if d.get("evidence_json"):
        try:d["evidence_json"] = json.loads(d["evidence_json"])
        except Exception: pass
    d["artifacts"] = rows(conn, "SELECT * FROM artifacts WHERE hypothesis_id=? OR pipeline_run_id=? ORDER BY created_at DESC", (d["hypothesis_id"], d.get("pipeline_run_id")))
    return d

def get_state_findings(conn, state_variable=None, dataset_type=None, polarity=None, min_trades=0):
    q = "SELECT * FROM state_findings WHERE COALESCE(n_trades,0) >= ?"; args=[min_trades]
    if state_variable: q+=" AND state_variable=?"; args.append(state_variable)
    if dataset_type: q+=" AND dataset_type=?"; args.append(dataset_type)
    if polarity == "positive": q+=" AND COALESCE(ev_r_net,0) > 0"
    if polarity == "negative": q+=" AND COALESCE(ev_r_net,0) < 0"
    q += " ORDER BY COALESCE(ev_r_net,0) DESC, COALESCE(n_trades,0) DESC LIMIT 500"
    return rows(conn, q, tuple(args))

def get_alpha_zoo(conn, repo_root: Path):
    if table_exists(conn, "alpha_candidates"):
        return rows(conn, "SELECT * FROM alpha_candidates ORDER BY promotion_score DESC")
    for p in [repo_root/"research/alpha_zoo/alpha_candidates.json", repo_root/"research/alpha_zoo/alpha_candidates.csv"]:
        if p.exists():
            if p.suffix == ".json": return json.loads(p.read_text(encoding='utf-8'))
    return []

def get_approvals(conn):
    return rows(conn, "SELECT * FROM queues WHERE queue_name='approval_queue' AND status='WAITING_FOR_APPROVAL' ORDER BY created_at DESC")

def set_approval(conn, queue_id: str, approve: bool):
    item = conn.execute("SELECT * FROM queues WHERE id=?", (queue_id,)).fetchone()
    if not item: return False
    status = "DONE" if approve else "CANCELLED"
    conn.execute("UPDATE queues SET status=?, updated_at=datetime('now') WHERE id=?", (status, queue_id))
    if approve:
        payload = json.loads(item["payload_json"] or "{}")
        if payload.get("verdict_id"):
            conn.execute("UPDATE verdicts SET approved_by_user=1, updated_at=datetime('now') WHERE id=?", (payload["verdict_id"],))
    conn.commit(); return True
