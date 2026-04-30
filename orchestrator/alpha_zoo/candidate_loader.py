from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

from orchestrator.db import ResearchDB


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _f(v: Any) -> float | None:
    try:
        return float(v)
    except Exception:
        return None


def load_candidates(args: Any) -> list[dict]:
    verdict_dir = Path(args.verdicts_dir)
    outputs_root = Path(args.experiments_root)
    candidates: list[dict] = []
    allowed = {"PROMOTE_SINGLE_RUN_TIER3","PROMOTE_MULTIPLE_RUNS_TIER3","PROMOTE_FAMILY_TIER3","ADD_TO_ALPHA_ZOO","PROMOTE_FORWARD_TEST"}
    verdict_names: set[str] = set()
    for vpath in verdict_dir.glob("*_verdict.json"):
        try:
            payload = json.loads(vpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        verdict = payload.get("verdict")
        name = vpath.name.replace("_verdict.json", "")
        if args.name and args.name != name:
            continue
        if verdict in allowed or (args.include_inconclusive and "INCONCLUSIVE" in str(verdict)):
            verdict_names.add(name)
    for summary in outputs_root.glob("*/summaries/run_summary.csv"):
        root = summary.parents[1]
        name = root.name
        if args.name and args.name not in name:
            continue
        rows = _read_csv(summary)
        for row in rows:
            ev = _f(row.get("ev_r_net"))
            n_trades = int(float(row.get("n_trades", 0) or 0))
            dd = abs(_f(row.get("max_drawdown", 0)) or 0)
            qualifies = (name in verdict_names) or (ev is not None and ev >= args.min_ev and n_trades >= args.min_trades and dd <= args.max_drawdown)
            if not qualifies and not args.include_negative_but_informative:
                continue
            run_id = row.get("run_id") or row.get("run") or f"run_{len(candidates)}"
            cid = hashlib.md5(f"{name}:{run_id}".encode()).hexdigest()[:16]
            candidates.append({
                "identity": {"candidate_id": cid, "hypothesis_id": None, "hypothesis_name": name, "hypothesis_family": row.get("family"), "layer": row.get("layer"), "run_id": run_id, "config_hash": row.get("config_hash"), "parameter_set_id": row.get("parameter_set_id"), "dataset_type": row.get("dataset_type"), "experiment_root": str(root), "manifest_path": None, "source_verdict_path": None, "created_at": datetime.now(timezone.utc).isoformat()},
                "performance": {k: _f(row.get(k)) for k in ["ev_r_net","ev_r_gross","win_rate","avg_r_win","avg_r_loss","payoff_ratio","max_drawdown","max_drawdown_duration","median_r","p95_r","p99_r","max_r","min_r"]} | {"n_trades": n_trades},
                "tail": {k: _f(row.get(k)) for k in ["tail_2r_count","tail_3r_count","tail_5r_count","tail_10r_count","tail_2r_rate","tail_3r_rate","tail_5r_rate","tail_10r_rate","avg_mfe_r","avg_mae_r","exit_efficiency"]},
                "cost": {k: _f(row.get(k)) for k in ["avg_cost_drag_r","avg_fee_drag_r","avg_slippage_drag_r","avg_spread_drag_r","gross_to_net_drag"]},
                "state_profile": {},
                "zoo_metadata": {"candidate_status": "CANDIDATE"},
            })
    return candidates


def write_candidates_to_db(db_path: Path, candidates: list[dict], artifact_paths: dict[str, Path]) -> None:
    db = ResearchDB(db_path)
    db.init_schema()
    conn = db.connect()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS alpha_candidates (
      id TEXT PRIMARY KEY, hypothesis_id TEXT, hypothesis_name TEXT, run_id TEXT, dataset_type TEXT,
      experiment_root TEXT, candidate_status TEXT, rank_score REAL, promotion_score REAL,
      ev_r_net REAL, ev_r_gross REAL, n_trades INTEGER, win_rate REAL, max_drawdown REAL,
      tail_5r_count INTEGER, tail_10r_count INTEGER, setup_class TEXT, state_profile_json TEXT,
      performance_json TEXT, cost_json TEXT, tail_json TEXT, recommended_action TEXT, notes TEXT,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )
    """)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for c in candidates:
        i,p,t,co,s,z = c["identity"],c["performance"],c["tail"],c["cost"],c.get("state_profile",{}),c.get("zoo_metadata",{})
        conn.execute("""INSERT OR REPLACE INTO alpha_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
            i.get("candidate_id"),i.get("hypothesis_id"),i.get("hypothesis_name"),i.get("run_id"),i.get("dataset_type"),i.get("experiment_root"),z.get("candidate_status"),z.get("rank_score"),z.get("promotion_score"),p.get("ev_r_net"),p.get("ev_r_gross"),p.get("n_trades"),p.get("win_rate"),p.get("max_drawdown"),t.get("tail_5r_count"),t.get("tail_10r_count"),s.get("setup_class"),json.dumps(s),json.dumps(p),json.dumps(co),json.dumps(t),z.get("recommended_action"),z.get("notes"),i.get("created_at") or now,now
        ))
        if z.get("candidate_status") == "PROMOTE_TIER3":
            db.enqueue(queue_name="approval_queue", item_type="alpha_candidate", item_id=i.get("candidate_id"), status="WAITING_FOR_APPROVAL", priority=80, payload={"type":"PROMOTE_TIER3_CANDIDATE","candidate_id":i.get("candidate_id"),"hypothesis_name":i.get("hypothesis_name"),"run_id":i.get("run_id"),"dataset_type":i.get("dataset_type"),"experiment_root":i.get("experiment_root"),"recommended_action":"PROMOTE_TIER3","evidence":{"promotion_score":z.get("promotion_score"),"ev_r_net":p.get("ev_r_net"),"n_trades":p.get("n_trades")}})
    conn.commit()
    for k,p in artifact_paths.items():
        db.register_artifact(artifact_type=k, path=p, description="alpha zoo output")
    db.close()
