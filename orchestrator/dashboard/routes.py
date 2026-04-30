from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from orchestrator.db import ResearchDB
from .artifact_reader import read_artifact_preview
from .db_queries import get_alpha_zoo, get_approvals, get_hypotheses, get_hypothesis_detail, get_pipeline_runs, get_queue, get_state_findings, get_summary, get_verdict_detail, get_verdicts, rows, set_approval
from .schemas import DashboardConfig


def build_router(config: DashboardConfig) -> APIRouter:
    router = APIRouter()
    repo_root = Path(config.repo_root)
    db = ResearchDB(config.db_path, repo_root=repo_root)
    templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

    def conn(): return db.connect()

    def render_template(request: Request, template_name: str, **context):
        return templates.TemplateResponse(
            request=request,
            name=template_name,
            context=context,
        )

    @router.get("/")
    def index(request: Request):
        hb = repo_root / "logs/research_daemon_heartbeat.json"
        daemon = json.loads(hb.read_text(encoding="utf-8")) if hb.exists() else {}
        return render_template(request, "index.html", summary=get_summary(conn()), daemon=daemon)

    @router.get("/queue")
    def queue(request: Request, queue_name: str | None = None, status: str | None = None):
        return render_template(request, "queue.html", items=get_queue(conn(), queue_name, status))

    @router.get("/hypotheses")
    def hypotheses(request: Request): return render_template(request, "hypotheses.html", items=get_hypotheses(conn()))

    @router.get("/hypotheses/{hypothesis_id}")
    def hypothesis_detail(request: Request, hypothesis_id: str):
        d = get_hypothesis_detail(conn(), hypothesis_id)
        if not d: raise HTTPException(404)
        return render_template(request, "hypothesis_detail.html", **d)

    @router.get("/pipeline-runs")
    def pipeline_runs(request: Request): return render_template(request, "pipeline_runs.html", items=get_pipeline_runs(conn()))

    @router.get("/verdicts")
    def verdicts(request: Request): return render_template(request, "verdicts.html", items=get_verdicts(conn()))

    @router.get("/verdicts/{verdict_id}")
    def verdict_detail(request: Request, verdict_id: str):
        d = get_verdict_detail(conn(), verdict_id)
        if not d: raise HTTPException(404)
        memo_preview = read_artifact_preview(repo_root, d["memo_path"]) if d.get("memo_path") else None
        return render_template(request, "verdict_detail.html", item=d, memo_preview=memo_preview)

    @router.get("/state-findings")
    def state_findings(request: Request, state_variable: str | None = None, dataset_type: str | None = None, polarity: str | None = None, min_trades: int = 0):
        items = get_state_findings(conn(), state_variable, dataset_type, polarity, min_trades)
        return render_template(request, "state_findings.html", items=items)

    @router.get("/alpha-zoo")
    def alpha_zoo(request: Request): return render_template(request, "alpha_zoo.html", items=get_alpha_zoo(conn(), repo_root))

    @router.get("/approvals")
    def approvals(request: Request): return render_template(request, "approvals.html", items=get_approvals(conn()), enable_actions=config.enable_actions)

    @router.post("/approvals/{queue_id}/approve")
    def approve(queue_id: str, confirm: str = Form("no")):
        if not config.enable_actions: raise HTTPException(403)
        if confirm == "yes": set_approval(conn(), queue_id, True)
        return RedirectResponse("/approvals", status_code=303)

    @router.post("/approvals/{queue_id}/reject")
    def reject(queue_id: str, confirm: str = Form("no")):
        if not config.enable_actions: raise HTTPException(403)
        if confirm == "yes": set_approval(conn(), queue_id, False)
        return RedirectResponse("/approvals", status_code=303)

    @router.get("/daemon")
    def daemon(request: Request):
        hb = repo_root / "logs/research_daemon_heartbeat.json"
        log = repo_root / "logs/research_daemon.log"
        status = json.loads(hb.read_text(encoding="utf-8")) if hb.exists() else {}
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines()[-200:] if log.exists() else []
        return render_template(request, "daemon.html", status=status, log_lines=lines)

    @router.get('/api/summary')
    def api_summary(): return JSONResponse(get_summary(conn()))
    @router.get('/api/queue')
    def api_queue(queue_name: str | None = None, status: str | None = None): return JSONResponse(get_queue(conn(), queue_name, status))
    @router.get('/api/hypotheses')
    def api_hyp(): return JSONResponse(get_hypotheses(conn()))
    @router.get('/api/hypotheses/{id}')
    def api_hyp_d(id: str): return JSONResponse(get_hypothesis_detail(conn(), id) or {})
    @router.get('/api/pipeline-runs')
    def api_pr(): return JSONResponse(get_pipeline_runs(conn()))
    @router.get('/api/verdicts')
    def api_ver(): return JSONResponse(get_verdicts(conn()))
    @router.get('/api/verdicts/{id}')
    def api_ver_d(id: str): return JSONResponse(get_verdict_detail(conn(), id) or {})
    @router.get('/api/state-findings')
    def api_sf(state_variable: str | None = None, dataset_type: str | None = None, polarity: str | None = None, min_trades: int = 0): return JSONResponse(get_state_findings(conn(), state_variable, dataset_type, polarity, min_trades))
    @router.get('/api/alpha-zoo')
    def api_az(): return JSONResponse(get_alpha_zoo(conn(), repo_root))
    @router.get('/api/daemon')
    def api_d():
        hb = repo_root / "logs/research_daemon_heartbeat.json"
        return JSONResponse(json.loads(hb.read_text(encoding='utf-8')) if hb.exists() else {})
    @router.post('/api/approvals/{queue_id}/approve')
    def api_a(queue_id: str):
        if not config.enable_actions: raise HTTPException(403)
        return {"ok": set_approval(conn(), queue_id, True)}
    @router.post('/api/approvals/{queue_id}/reject')
    def api_r(queue_id: str):
        if not config.enable_actions: raise HTTPException(403)
        return {"ok": set_approval(conn(), queue_id, False)}

    return router
