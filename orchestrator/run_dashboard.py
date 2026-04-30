#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def parse_args():
    p=argparse.ArgumentParser()
    p.add_argument('--db', default='research_db/research.sqlite')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=8765)
    p.add_argument('--enable-actions', action='store_true', default=False)
    return p.parse_args()

def main():
    args=parse_args()
    if args.host=='0.0.0.0':
        print('WARNING: Dashboard is being exposed beyond localhost. Do not expose without firewall/authentication.')
    try:
        import uvicorn
    except Exception:
        print('FastAPI dashboard dependencies missing. Run: pip install fastapi uvicorn jinja2')
        raise SystemExit(1)
    from orchestrator.dashboard.app import create_app
    from orchestrator.dashboard.schemas import DashboardConfig
    app=create_app(DashboardConfig(db_path=args.db, repo_root=str(PROJECT_ROOT), enable_actions=args.enable_actions))
    uvicorn.run(app, host=args.host, port=args.port)

if __name__=='__main__':
    main()
