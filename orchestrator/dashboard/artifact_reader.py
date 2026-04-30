from __future__ import annotations

import csv
import json
from pathlib import Path

ALLOWED_DIRS = ("research", "outputs", "logs")
ALLOWED_SUFFIXES = {".md", ".json", ".csv", ".log", ".txt"}


def safe_resolve_artifact(repo_root: Path, rel_path: str | Path) -> Path:
    path = Path(rel_path)
    if path.is_absolute():
        raise ValueError("Absolute paths are not allowed")
    resolved = (repo_root / path).resolve()
    for base in ALLOWED_DIRS:
        allowed_root = (repo_root / base).resolve()
        try:
            resolved.relative_to(allowed_root)
            return resolved
        except ValueError:
            continue
    raise ValueError(f"Artifact path outside allowed directories: {rel_path}")


def read_artifact_preview(repo_root: Path, rel_path: str | Path) -> dict:
    p = safe_resolve_artifact(repo_root, rel_path)
    if not p.exists():
        return {"type": "missing", "path": str(rel_path), "content": None}
    suffix = p.suffix.lower()
    if suffix not in ALLOWED_SUFFIXES:
        return {"type": "unsupported", "path": str(rel_path), "content": None}

    if suffix in {".md", ".txt"}:
        return {"type": "text", "path": str(rel_path), "content": p.read_text(encoding="utf-8", errors="replace")[:20000]}
    if suffix == ".json":
        return {"type": "json", "path": str(rel_path), "content": json.loads(p.read_text(encoding="utf-8"))}
    if suffix == ".csv":
        rows = []
        with p.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for i, row in enumerate(reader):
                if i >= 100:
                    break
                rows.append(row)
        return {"type": "csv", "path": str(rel_path), "content": rows}
    if suffix == ".log":
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        return {"type": "log", "path": str(rel_path), "content": "\n".join(lines[-200:])}
    return {"type": "unsupported", "path": str(rel_path), "content": None}
