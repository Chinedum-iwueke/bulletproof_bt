#!/usr/bin/env python3
"""Build missing L7-H1 kernel columns for volatile member panels.

This is a maintenance helper for the column-backed volatile fast path. It is
resumable: each symbol panel is rewritten atomically, and already-stamped
symbols are skipped on rerun.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
from pathlib import Path
import sys
import time
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import pandas as pd
import pyarrow.parquet as pq

from bt.research_data.jobs.state_features import build_l7_h1_kernel_features
from bt.research_data.storage import ResearchDataStore


def _panel_path(root: Path, exchange: str, symbol: str, timeframe: str) -> Path | None:
    candidates = [
        root / "canonical" / "perp" / exchange / symbol / f"timeframe={timeframe}" / "research_panel.parquet",
        root / "canonical" / exchange / symbol / f"timeframe={timeframe}" / "research_panel.parquet",
    ]
    return next((path for path in candidates if path.exists()), None)


def _has_l7h1_columns(path: Path) -> bool:
    cols = pq.ParquetFile(path).schema_arrow.names
    return any(col.startswith("l7h1_15m_") for col in cols) and any(col.startswith("l7h1_1h_") for col in cols)


def _membership_symbols(root: Path, exchange: str) -> list[str]:
    path = root / "manifests" / "volatile_universe_membership.parquet"
    membership = pd.read_parquet(path, columns=["exchange", "symbol"])
    membership = membership[membership["exchange"].astype(str).eq(exchange)]
    return sorted(membership["symbol"].dropna().astype(str).unique().tolist())


def _discover_missing(root: Path, exchange: str, timeframe: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in _membership_symbols(root, exchange):
        path = _panel_path(root, exchange, symbol, timeframe)
        if path is None:
            rows.append({"symbol": symbol, "rows": 0, "status": "missing_panel", "path": ""})
            continue
        parquet = pq.ParquetFile(path)
        if _has_l7h1_columns(path):
            continue
        rows.append({"symbol": symbol, "rows": int(parquet.metadata.num_rows), "status": "pending", "path": str(path)})
    return sorted(rows, key=lambda row: int(row["rows"]), reverse=True)


def _build_one(payload: tuple[str, str, str, str]) -> dict[str, Any]:
    root, exchange, timeframe, symbol = payload
    started = time.monotonic()
    report = build_l7_h1_kernel_features(
        exchange,
        timeframe,
        symbols=[symbol],
        store=ResearchDataStore(Path(root)),
    )
    row = report.iloc[0].to_dict() if not report.empty else {"exchange": exchange, "symbol": symbol, "status": "empty_report", "rows": 0}
    row["duration_seconds"] = round(time.monotonic() - started, 3)
    return row


def main() -> int:
    parser = argparse.ArgumentParser(description="Build missing volatile L7-H1 kernel feature columns")
    parser.add_argument("--root", default="research_data")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--report", default="research_data/reports/l7h1_volatile_kernel_feature_build.jsonl")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    missing = _discover_missing(root, args.exchange, args.timeframe)
    if args.limit and args.limit > 0:
        missing = missing[: args.limit]
    total_rows = sum(int(row["rows"]) for row in missing if row["status"] == "pending")
    print(f"missing_symbols={len(missing)} pending_rows={total_rows} workers={args.workers}", flush=True)
    if args.dry_run or not missing:
        for row in missing[:20]:
            print(json.dumps(row, sort_keys=True), flush=True)
        return 0

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pending = [row for row in missing if row["status"] == "pending"]
    skipped = [row for row in missing if row["status"] != "pending"]
    with report_path.open("a", encoding="utf-8") as handle:
        for row in skipped:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
        done = 0
        failed = 0
        t0 = time.monotonic()
        payloads = [(str(root), args.exchange, args.timeframe, str(row["symbol"])) for row in pending]
        with ProcessPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = {pool.submit(_build_one, payload): payload[-1] for payload in payloads}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    row = future.result()
                except Exception as exc:  # pragma: no cover - operational path
                    failed += 1
                    row = {"exchange": args.exchange, "symbol": symbol, "status": "failed", "error": str(exc)}
                else:
                    done += 1 if row.get("status") == "ok" else 0
                    failed += 1 if row.get("status") not in {"ok"} else 0
                handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
                handle.flush()
                elapsed = max(time.monotonic() - t0, 1.0)
                completed = done + failed
                rate = completed / elapsed
                remaining = (len(pending) - completed) / rate if rate > 0 else 0.0
                print(
                    f"completed={completed}/{len(pending)} ok={done} failed={failed} "
                    f"symbol={symbol} status={row.get('status')} eta_minutes={remaining/60:.1f}",
                    flush=True,
                )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
