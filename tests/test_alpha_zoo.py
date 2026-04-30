from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from orchestrator.alpha_zoo.candidate_loader import load_candidates, write_candidates_to_db
from orchestrator.alpha_zoo.candidate_ranker import rank_candidates
from orchestrator.alpha_zoo.correlation_analyzer import analyze_redundancy
from orchestrator.alpha_zoo.promotion_rules import apply_promotion_rules
from orchestrator.alpha_zoo.state_profile_analyzer import enrich_state_profiles


class A: pass


def _write_summary(root: Path, name: str, rows: list[dict]) -> None:
    p = root / name / "summaries"
    p.mkdir(parents=True, exist_ok=True)
    import csv
    with (p / "run_summary.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=sorted(set().union(*[r.keys() for r in rows])))
        w.writeheader(); w.writerows(rows)


def _args(tmp_path: Path):
    a=A(); a.verdicts_dir=str(tmp_path/"verdicts"); a.experiments_root=str(tmp_path/"outputs"); a.name=None; a.include_inconclusive=False; a.include_negative_but_informative=False; a.min_ev=0.05; a.min_trades=50; a.max_drawdown=999
    return a


def test_positive_ev_run_becomes_candidate(tmp_path: Path):
    _write_summary(tmp_path/"outputs", "h1", [{"run_id":"r1","ev_r_net":0.1,"n_trades":60}])
    a=_args(tmp_path); (tmp_path/"verdicts").mkdir()
    c=load_candidates(a)
    assert len(c)==1


def test_low_sample_watchlist(tmp_path: Path):
    _write_summary(tmp_path/"outputs", "h1", [{"run_id":"r1","ev_r_net":0.1,"n_trades":10}])
    a=_args(tmp_path); (tmp_path/"verdicts").mkdir(); a.min_trades=50; a.min_ev=0; a.include_negative_but_informative=True
    c=apply_promotion_rules(rank_candidates(load_candidates(a)), min_trades=50)
    assert c[0]["zoo_metadata"]["candidate_status"] == "WATCHLIST"


def test_one_lucky_tail_fragile():
    c=[{"identity":{},"performance":{"ev_r_net":0.2,"n_trades":100,"max_r":10,"p95_r":1},"tail":{"tail_5r_count":0},"cost":{},"state_profile":{},"zoo_metadata":{}}]
    out=rank_candidates(c)
    assert out[0]["zoo_metadata"]["fragile_one_lucky_trade"] is True


def test_redundant_detection():
    cs=[{"identity":{"candidate_id":"a","hypothesis_family":"f","dataset_type":"d","config_hash":"x"},"state_profile":{"setup_class":"s"},"zoo_metadata":{"promotion_score":1}}, {"identity":{"candidate_id":"b","hypothesis_family":"f","dataset_type":"d","config_hash":"x"},"state_profile":{"setup_class":"s"},"zoo_metadata":{"promotion_score":2}}]
    c,_=analyze_redundancy(cs)
    assert any(x["zoo_metadata"].get("candidate_status")=="REDUNDANT" for x in c)


def test_cost_killed_refine():
    cs=[{"identity":{},"performance":{"ev_r_gross":0.1,"ev_r_net":-0.01,"n_trades":80},"tail":{},"state_profile":{},"zoo_metadata":{}}]
    out=apply_promotion_rules(cs)
    assert out[0]["zoo_metadata"]["candidate_status"]=="REFINE"


def test_state_profile_attached(tmp_path: Path):
    cs=[{"identity":{"hypothesis_name":"h1"},"state_profile":{}}]
    d=tmp_path/"sf"; d.mkdir()
    (d/"h1_state_findings.json").write_text(json.dumps({"findings":[{"state_variable":"vol","bucket":"high","ev_r_net":0.1}]}), encoding="utf-8")
    out=enrich_state_profiles(cs,d)
    assert out[0]["state_profile"]["state_profile_status"]=="ok"


def test_write_db_and_approval_queue(tmp_path: Path):
    db=tmp_path/"r.sqlite"
    cs=[{"identity":{"candidate_id":"c1","hypothesis_name":"h1","run_id":"r1","dataset_type":"stable","experiment_root":"o","created_at":"2026-01-01T00:00:00+00:00"},"performance":{"ev_r_net":0.1,"ev_r_gross":0.2,"n_trades":100},"tail":{},"cost":{},"state_profile":{},"zoo_metadata":{"candidate_status":"PROMOTE_TIER3","promotion_score":1}}]
    write_candidates_to_db(db,cs,{})
    con=sqlite3.connect(db)
    assert con.execute("select count(*) from alpha_candidates").fetchone()[0]==1
    assert con.execute("select count(*) from queues where queue_name='approval_queue' and status='WAITING_FOR_APPROVAL'").fetchone()[0]==1
