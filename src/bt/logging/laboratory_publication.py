"""Digest-bound publication of certified run bundles into Hermes and research memory."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from bt.logging.run_bundle import (
    RunBundleError,
    hermes_run_payload,
    validate_bundle_manifest,
)

PUBLICATION_SCHEMA_VERSION = "laboratory-publication-v1.0.0"
MEMORY_RECEIPT_SCHEMA_VERSION = "bulletproof-memory-publication-receipt-v1.0.0"


def digest_document(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
    ).hexdigest()


def publish_certified_bundle(
    *,
    api_url: str,
    token: str,
    bundle_dir: Path,
    registry_trial_id: str,
    registry_result_id: str,
    memory_database: Path,
    timeout_seconds: float = 120.0,
) -> dict[str, Any]:
    """Create or resume the Hermes publication saga for one immutable bundle."""
    manifest_path = bundle_dir / "run_bundle_manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunBundleError("finalized run bundle manifest is unavailable") from exc
    validate_bundle_manifest(manifest, bundle_dir)
    lineage = manifest["lineage"]
    run_payload = hermes_run_payload(
        {
            "state": "finalized",
            "bundle_digest": manifest["bundle_digest"],
            "manifest_digest": manifest["manifest_digest"],
            "storage_uri": f"bundle://sha256/{manifest['bundle_digest']}",
        },
        lineage,
    )
    _request(
        api_url, token, "/v1/research/evidence/objects", run_payload, timeout_seconds
    )
    publication = {
        "schema_version": PUBLICATION_SCHEMA_VERSION,
        "trial_id": registry_trial_id,
        "result_id": registry_result_id,
        "run_object_id": run_payload["object_id"],
        "repository_commit": lineage["repository_commit"],
        "dataset_digest": lineage["dataset_digest"],
        "market_model_bundle_digest": lineage["market_model_bundle_digest"],
        "representation_contract_digest": lineage["representation_contract_digest"],
        "bundle_digest": manifest["bundle_digest"],
        "bundle_manifest_digest": manifest["manifest_digest"],
    }
    publication["request_digest"] = digest_document(publication)
    response = _request(
        api_url,
        token,
        "/v1/research/laboratory/publications",
        publication,
        timeout_seconds,
    )
    if response.get("state") == "awaiting_memory":
        receipt = record_memory_publication(memory_database, response)
        response = _request(
            api_url,
            token,
            f"/v1/research/laboratory/publications/{response['id']}/memory",
            receipt,
            timeout_seconds,
        )
    return response


def confirm_projections(
    *,
    api_url: str,
    token: str,
    publication_id: str,
    graph_manifest_digest: str,
    graph_source_epoch: int,
    retrieval_corpus_digest: str,
    retrieval_source_epoch: int,
    timeout_seconds: float = 120.0,
) -> dict[str, Any]:
    payload = {
        "schema_version": "laboratory-projection-receipt-v1.0.0",
        "graph_manifest_digest": graph_manifest_digest,
        "graph_source_epoch": graph_source_epoch,
        "retrieval_corpus_digest": retrieval_corpus_digest,
        "retrieval_source_epoch": retrieval_source_epoch,
    }
    return _request(
        api_url,
        token,
        f"/v1/research/laboratory/publications/{publication_id}/projections",
        payload,
        timeout_seconds,
    )


def record_memory_publication(
    database: Path, publication: dict[str, Any]
) -> dict[str, Any]:
    """Idempotently project a completed Hermes canonical receipt into local memory."""
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS research_memory_publications (
                publication_key TEXT PRIMARY KEY,
                bundle_digest TEXT NOT NULL UNIQUE,
                request_digest TEXT NOT NULL UNIQUE,
                trial_id TEXT NOT NULL,
                result_id TEXT NOT NULL,
                canonical_receipt_json TEXT NOT NULL,
                record_digest TEXT NOT NULL UNIQUE
            )
            """
        )
        publication_key = str(publication["id"])
        document = {
            "publication_key": publication_key,
            "bundle_digest": publication["bundle_digest"],
            "request_digest": publication["request_digest"],
            "trial_id": publication["trial_id"],
            "result_id": publication["result_id"],
            "canonical_receipt": publication["canonical_receipt"],
        }
        record_digest = digest_document(document)
        existing = connection.execute(
            "SELECT record_digest FROM research_memory_publications WHERE publication_key = ?",
            (publication_key,),
        ).fetchone()
        if existing is not None and existing[0] != record_digest:
            raise RunBundleError("research memory publication identity is immutable")
        disposition = "existing" if existing is not None else "created"
        if existing is None:
            connection.execute(
                "INSERT INTO research_memory_publications VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    publication_key,
                    publication["bundle_digest"],
                    publication["request_digest"],
                    publication["trial_id"],
                    publication["result_id"],
                    json.dumps(publication["canonical_receipt"], sort_keys=True),
                    record_digest,
                ),
            )
            connection.commit()
        return {
            "schema_version": MEMORY_RECEIPT_SCHEMA_VERSION,
            "bundle_digest": publication["bundle_digest"],
            "memory_database_digest": record_digest,
            "publication_key": publication_key,
            "disposition": disposition,
        }
    except sqlite3.IntegrityError as exc:
        raise RunBundleError(
            "bundle is already bound to different research memory"
        ) from exc
    finally:
        connection.close()


def _request(
    api_url: str,
    token: str,
    path: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}{path}",
        data=json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            document = json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RunBundleError(
            f"Hermes laboratory publication failed with HTTP {exc.code}: {detail}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RunBundleError(
            "Hermes laboratory publication is temporarily unavailable"
        ) from exc
    if not isinstance(document, dict):
        raise RunBundleError(
            "Hermes laboratory publication returned an invalid receipt"
        )
    return document
