"""Deterministic precompute registry for parallel experiment runs."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PrecomputeArtifact:
    artifact_id: str
    cache_key: str
    kind: str
    status: str
    path: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class PrecomputeRegistry:
    registry_path: Path
    artifacts: dict[str, PrecomputeArtifact]


def stable_cache_key(*, dataset_id: str, timeframe: str, family: str, params: dict[str, Any], engine_version: str) -> str:
    canonical = {
        "dataset_id": dataset_id,
        "timeframe": timeframe,
        "family": family,
        "params": params,
        "engine_version": engine_version,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _artifact_path(root: Path, kind: str, cache_key: str) -> Path:
    return root / "shared_cache" / kind / f"{cache_key}.json"


def build_registry(
    *,
    experiment_root: Path,
    dataset_id: str,
    preprocessing_signatures: list[dict[str, Any]],
    engine_version: str,
) -> PrecomputeRegistry:
    artifacts: dict[str, PrecomputeArtifact] = {}
    for signature in preprocessing_signatures:
        timeframe = str(signature.get("signal_timeframe", "1m"))
        family = str(signature.get("strategy", "unknown"))
        params = dict(signature.get("invariants", {}))
        cache_key = stable_cache_key(
            dataset_id=dataset_id,
            timeframe=timeframe,
            family=family,
            params=params,
            engine_version=engine_version,
        )
        path = _artifact_path(experiment_root, "runtime_overrides", cache_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        status = "attached_from_cache" if path.exists() else "cold_built"
        payload = {
            "cache_key": cache_key,
            "signature": signature,
            "engine_version": engine_version,
        }
        if not path.exists():
            path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        artifact = PrecomputeArtifact(
            artifact_id=f"runtime_override:{cache_key[:12]}",
            cache_key=cache_key,
            kind="runtime_override",
            status=status,
            path=str(path),
            metadata=payload,
        )
        artifacts[cache_key] = artifact

    registry_path = experiment_root / "summaries" / "precompute_registry.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps({"artifacts": [asdict(item) for item in artifacts.values()]}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return PrecomputeRegistry(registry_path=registry_path, artifacts=artifacts)
