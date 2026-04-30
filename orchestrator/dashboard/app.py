from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from .routes import build_router
from .schemas import DashboardConfig


def create_app(config: DashboardConfig) -> FastAPI:
    app = FastAPI(title="Research Dashboard")
    app.include_router(build_router(config))
    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    return app
