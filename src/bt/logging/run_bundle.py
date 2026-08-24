"""Atomic, content-addressed finalization for governed research runs."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import shutil
import uuid
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

import yaml  # type: ignore[import-untyped]

from bt.contracts.canonical_identity import SCHEMA_VERSION as IDENTITY_SCHEMA_VERSION
from bt.contracts.canonical_identity import validate_identity
from bt.execution.model_registry import MarketModelError, validate_model_bundle_document
from bt.experiments.representation_contract import (
    LEAKAGE_REPORT_SCHEMA_VERSION,
    RepresentationContractError,
    validate_representation_document,
)
from bt.logging.run_contract import validate_run_artifacts

BUNDLE_SCHEMA_VERSION = "run-bundle-v1.0.0"
FAILURE_SCHEMA_VERSION = "run-bundle-failure-v1.0.0"
RECEIPT_SCHEMA_VERSION = "run-bundle-receipt-v1.0.0"
_NAMESPACE = uuid.UUID("1445f9ab-0f4b-401d-a242-97579d9d7821")
_VOLATILE_KEYS = {"created_at", "created_at_utc", "generated_at", "run_dir", "run_id", "output_dir"}
_ABSOLUTE_PATH = re.compile(
    r"(?<![A-Za-z0-9])(?:/home/|/Users/|/srv/|/etc/|/tmp/|/var/|/opt/|/root/|[A-Za-z]:\\\\)"
)
_SECRET = re.compile(
    r"(?i)(?:bearer\s+[A-Za-z0-9._~-]{16,}|(?:api[_-]?key|token|secret|password)\s*[:=]\s*[^\s,}\]]{8,}|sk-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9]{16,})"
)


class RunBundleError(ValueError):
    """A run cannot be finalized without weakening the evidence contract."""


def _canonical(document: Any) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _digest_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _digest(document: Any) -> str:
    return _digest_bytes(_canonical(document))


def _semantic_core(lineage: dict[str, Any], artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "lineage": lineage,
        "artifacts": [
            {
                "name": item["name"],
                "media_type": item["media_type"],
                "schema_identity": item["schema_identity"],
                "semantic_digest": item["semantic_digest"],
            }
            for item in artifacts
        ],
    }


def _without_volatile(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _without_volatile(item) for key, item in value.items() if key not in _VOLATILE_KEYS}
    if isinstance(value, list):
        return [_without_volatile(item) for item in value]
    return value


def _legacy_manifest_bytes(path: Path) -> tuple[bytes, str]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise RunBundleError("run_manifest.json must contain an object")
    document["run_dir"] = "run://current"
    document["data_path"] = "dataset://registered-snapshot"
    return json.dumps(document, indent=2, sort_keys=True).encode("utf-8") + b"\n", "stable-runtime-aliases-v1"


def _artifact_bytes(path: Path) -> tuple[bytes, str | None]:
    if path.name == "run_manifest.json":
        return _legacy_manifest_bytes(path)
    return path.read_bytes(), None


def _semantic_content(name: str, content: bytes) -> tuple[bytes, str, str]:
    suffix = Path(name).suffix.lower()
    if suffix == ".json":
        document = json.loads(content)
        normalized = _without_volatile(document)
        schema = str(document.get("schema_version", f"bulletproof.{Path(name).stem}.json-v1")) if isinstance(document, dict) else f"bulletproof.{Path(name).stem}.json-v1"
        return _canonical(normalized), schema, "application/json"
    if suffix == ".jsonl":
        rows = []
        keys: set[str] = set()
        for line in content.decode("utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise RunBundleError(f"{name} contains a non-object JSONL row")
            normalized = _without_volatile(row)
            rows.append(normalized)
            keys.update(normalized)
        schema = f"jsonl-keys-sha256:{_digest(sorted(keys))}"
        return b"\n".join(_canonical(row) for row in rows) + (b"\n" if rows else b""), schema, "application/x-ndjson"
    if suffix == ".csv":
        text = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        fields = [field for field in (reader.fieldnames or []) if field not in _VOLATILE_KEYS]
        if not fields:
            raise RunBundleError(f"{name} has no CSV schema")
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in reader:
            writer.writerow({field: row.get(field, "") for field in fields})
        schema = f"csv-columns-sha256:{_digest(fields)}"
        return output.getvalue().encode("utf-8"), schema, "text/csv"
    if suffix in {".yaml", ".yml"}:
        document = yaml.safe_load(content)
        if not isinstance(document, dict):
            raise RunBundleError(f"{name} must contain a YAML object")
        normalized = _without_volatile(document)
        schema = f"yaml-keys-sha256:{_digest(sorted(normalized))}"
        return _canonical(normalized), schema, "application/yaml"
    if suffix in {".txt", ".md"}:
        lines = [
            line for line in content.decode("utf-8").splitlines()
            if not line.startswith(("Generated: ", "Run Dir: "))
        ]
        return ("\n".join(lines).rstrip() + "\n").encode("utf-8"), "plain-text-v1", "text/plain"
    raise RunBundleError(f"artifact has no registered structural schema: {name}")


def _scan(name: str, content: bytes) -> None:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RunBundleError(f"artifact is not a supported textual canonical format: {name}") from exc
    if _ABSOLUTE_PATH.search(text):
        raise RunBundleError(f"artifact contains an absolute protected path: {name}")
    if _SECRET.search(text):
        raise RunBundleError(f"artifact contains possible sensitive material: {name}")


def _safe_message(error: Exception, run_dir: Path) -> str:
    return str(error).replace(str(run_dir), "<run-dir>")[:1000]


def _atomic_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(_canonical(document) + b"\n")
    os.replace(temporary, path)


def _failure(bundle_root: Path, run_dir: Path, error: Exception) -> Path:
    attempt_id = str(uuid.uuid4())
    path = bundle_root / "failures" / f"{attempt_id}.json"
    _atomic_json(path, {
        "schema_version": FAILURE_SCHEMA_VERSION,
        "attempt_id": attempt_id,
        "state": "failed",
        "error_category": type(error).__name__,
        "message": _safe_message(error, run_dir),
        "source_artifact_names": sorted(item.name for item in run_dir.iterdir() if item.is_file()),
    })
    return path


def _validate_lineage(lineage: dict[str, Any]) -> None:
    required = {
        "repository_commit", "code_digest", "dataset_snapshot_id", "dataset_digest",
        "specification_digest", "environment_digest", "market_model_bundle_digest",
        "representation_contract_digest", "search_plan_digest", "search_family_id", "trial_id", "attempt",
    }
    missing = sorted(required - set(lineage))
    if missing:
        raise RunBundleError(f"lineage missing fields: {missing}")
    for key in (
        "repository_commit", "code_digest", "dataset_digest", "specification_digest",
        "environment_digest", "market_model_bundle_digest",
        "representation_contract_digest", "search_plan_digest", "trial_id",
    ):
        value = str(lineage[key])
        expected = 40 if key == "repository_commit" else 64
        if len(value) != expected or any(char not in "0123456789abcdef" for char in value):
            raise RunBundleError(f"lineage {key} must be lowercase hexadecimal length {expected}")
    try:
        uuid.UUID(str(lineage["dataset_snapshot_id"]))
    except ValueError as exc:
        raise RunBundleError("dataset_snapshot_id must be a UUID") from exc
    if not isinstance(lineage["attempt"], int) or lineage["attempt"] < 1:
        raise RunBundleError("attempt must be a positive integer")
    if not str(lineage["search_family_id"]).strip():
        raise RunBundleError("search_family_id is required")


def validate_bundle_manifest(manifest: dict[str, Any], bundle_dir: Path | None = None) -> None:
    if manifest.get("schema_version") != BUNDLE_SCHEMA_VERSION:
        raise RunBundleError("unsupported run bundle schema version")
    validate_identity(manifest["identity"])
    expected_manifest = _digest({key: value for key, value in manifest.items() if key != "manifest_digest"})
    if manifest.get("manifest_digest") != expected_manifest:
        raise RunBundleError("bundle manifest digest mismatch")
    if manifest.get("bundle_digest") != _digest(
        _semantic_core(manifest["lineage"], manifest["artifacts"])
    ):
        raise RunBundleError("bundle semantic digest mismatch")
    if manifest["identity"]["content_digest"] != manifest["bundle_digest"]:
        raise RunBundleError("bundle identity digest mismatch")
    if bundle_dir is not None:
        for artifact in manifest["artifacts"]:
            path = bundle_dir / "artifacts" / artifact["name"]
            if not path.is_file() or _digest_bytes(path.read_bytes()) != artifact["content_digest"]:
                raise RunBundleError(f"bundle artifact integrity mismatch: {artifact['name']}")


def _validate_representation_evidence(run_dir: Path, lineage: dict[str, Any]) -> None:
    contract_path = run_dir / "representation_contract.json"
    report_path = run_dir / "representation_leakage_report.json"
    if not contract_path.is_file() or not report_path.is_file():
        raise RunBundleError(
            "representation_contract.json and representation_leakage_report.json are required"
        )
    try:
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        report = json.loads(report_path.read_text(encoding="utf-8"))
        validate_representation_document(contract)
    except (json.JSONDecodeError, RepresentationContractError) as exc:
        raise RunBundleError(f"invalid representation evidence: {exc}") from exc
    contract_digest = contract.get("representation_digest")
    if contract_digest != lineage["representation_contract_digest"]:
        raise RunBundleError("representation contract does not match lineage digest")
    if report.get("schema_version") != LEAKAGE_REPORT_SCHEMA_VERSION:
        raise RunBundleError("unsupported representation leakage report schema")
    expected = _digest({key: value for key, value in report.items() if key != "report_digest"})
    if report.get("report_digest") != expected:
        raise RunBundleError("representation leakage report digest mismatch")
    if report.get("representation_digest") != contract_digest or report.get("status") != "certified":
        raise RunBundleError("representation leakage report is not certified for this contract")


def finalize_run_bundle(
    run_dir: Path,
    bundle_root: Path,
    *,
    lineage: dict[str, Any],
    before_commit: Callable[[Path], None] | None = None,
) -> dict[str, Any]:
    """Finalize a run into an immutable directory, or retain a failed attempt."""
    run_dir = run_dir.resolve(strict=True)
    bundle_root.mkdir(parents=True, exist_ok=True)
    staging = bundle_root / f".staging-{uuid.uuid4()}"
    try:
        _validate_lineage(lineage)
        validate_run_artifacts(run_dir)
        model_bundle_path = run_dir / "market_model_bundle.json"
        if not model_bundle_path.is_file():
            raise RunBundleError("market_model_bundle.json is required")
        try:
            model_bundle = json.loads(model_bundle_path.read_text(encoding="utf-8"))
            validate_model_bundle_document(model_bundle)
        except (json.JSONDecodeError, MarketModelError) as exc:
            raise RunBundleError(f"invalid market_model_bundle.json: {exc}") from exc
        if model_bundle["bundle_digest"] != lineage["market_model_bundle_digest"]:
            raise RunBundleError("market-model bundle does not match lineage digest")
        _validate_representation_evidence(run_dir, lineage)
        staging_artifacts = staging / "artifacts"
        staging_artifacts.mkdir(parents=True)
        entries: list[dict[str, Any]] = []
        for source in sorted((item for item in run_dir.iterdir() if item.is_file()), key=lambda item: item.name):
            if source.name.startswith(".") or source.name in {"run_bundle_manifest.json", "run_bundle_receipt.json"}:
                continue
            if source.is_symlink():
                raise RunBundleError(f"symlinked artifacts are forbidden: {source.name}")
            source_content = source.read_bytes()
            published_content, normalization = _artifact_bytes(source)
            _scan(source.name, published_content)
            semantic, schema_identity, media_type = _semantic_content(source.name, published_content)
            destination = staging_artifacts / source.name
            destination.write_bytes(published_content)
            entries.append({
                "name": source.name,
                "media_type": media_type,
                "schema_identity": schema_identity,
                "byte_size": len(published_content),
                "source_content_digest": _digest_bytes(source_content),
                "content_digest": _digest_bytes(published_content),
                "semantic_digest": _digest_bytes(semantic),
                "normalization": normalization,
            })
        core: dict[str, Any] = {"lineage": lineage, "artifacts": entries}
        bundle_digest = _digest(_semantic_core(lineage, entries))
        object_id = str(uuid.uuid5(_NAMESPACE, bundle_digest))
        identity = {
            "schema_version": IDENTITY_SCHEMA_VERSION,
            "object_id": object_id,
            "object_type": "run-bundle",
            "content_version": "1",
            "content_digest": bundle_digest,
            "producer": {"system": "bulletproof-bt", "native_type": "run-bundle", "native_id": object_id, "schema_version": BUNDLE_SCHEMA_VERSION},
            "aliases": [{"namespace": "bulletproof-bt", "object_type": "run-bundle", "value": object_id}],
            "supersedes_object_id": None,
        }
        manifest: dict[str, Any] = {
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "state": "finalized",
            "identity": identity,
            "bundle_digest": bundle_digest,
            **core,
        }
        manifest["manifest_digest"] = _digest(manifest)
        _atomic_json(staging / "run_bundle_manifest.json", manifest)
        validate_bundle_manifest(manifest, staging)
        if before_commit is not None:
            before_commit(staging)
        final_dir = bundle_root / "bundles" / bundle_digest
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            existing: dict[str, Any] = json.loads(
                (final_dir / "run_bundle_manifest.json").read_text(encoding="utf-8")
            )
            validate_bundle_manifest(existing, final_dir)
            if _semantic_core(existing["lineage"], existing["artifacts"]) != _semantic_core(
                manifest["lineage"], manifest["artifacts"]
            ):
                raise RunBundleError("bundle digest collision with different semantics")
            shutil.rmtree(staging)
            disposition = "existing-semantic-equivalent"
        else:
            os.replace(staging, final_dir)
            try:
                directory_fd = os.open(final_dir.parent, os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                pass
            disposition = "created"
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "state": "finalized",
            "disposition": disposition,
            "bundle_id": object_id,
            "bundle_digest": bundle_digest,
            "manifest_digest": manifest["manifest_digest"],
            "storage_uri": f"bundle://sha256/{bundle_digest}",
        }
        receipt_path = bundle_root / "receipts" / f"{bundle_digest}.json"
        if disposition.startswith("existing") and receipt_path.exists():
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        else:
            _atomic_json(receipt_path, receipt)
        return receipt
    except Exception as exc:
        if staging.exists():
            shutil.rmtree(staging)
        failure_path = _failure(bundle_root, run_dir, exc)
        if isinstance(exc, RunBundleError):
            raise
        raise RunBundleError(f"bundle finalization failed; failure retained as {failure_path.name}") from exc


def hermes_run_payload(receipt: dict[str, Any], lineage: dict[str, Any]) -> dict[str, Any]:
    """Build the canonical Hermes run object for a finalized bundle receipt."""
    if receipt.get("state") != "finalized":
        raise RunBundleError("only finalized bundles can be published")
    run = {
        "kind": "run",
        "dataset_object_ids": [str(lineage["dataset_snapshot_id"])],
        "specification_digest": str(lineage["specification_digest"]),
        "code_digest": str(lineage["code_digest"]),
        "environment_digest": str(lineage["environment_digest"]),
        "attempt": int(lineage["attempt"]),
        "bundle_digest": str(receipt["bundle_digest"]),
        "bundle_manifest_digest": str(receipt["manifest_digest"]),
        "bundle_uri": str(receipt["storage_uri"]),
    }
    content_digest = _digest(run)
    object_id = str(uuid.uuid5(_NAMESPACE, f"hermes-run:{content_digest}"))
    return {
        "schema_version": IDENTITY_SCHEMA_VERSION,
        "object_schema_version": "canonical-evidence-v1.0.0",
        "object_id": object_id,
        "object_type": "run",
        "content_version": "1",
        "content_digest": content_digest,
        "producer": {"system": "bulletproof-bt", "native_type": "run", "native_id": object_id, "schema_version": BUNDLE_SCHEMA_VERSION},
        "aliases": [{"namespace": "bulletproof-bt", "object_type": "run", "value": object_id}],
        "supersedes_object_id": None,
        "project": "bulletproof-bt",
        "access_class": "restricted",
        "authority_class": "operational",
        "payload": run,
        "created_by": "bulletproof-producer",
    }


def publish_to_hermes(
    api_url: str,
    token: str,
    receipt: dict[str, Any],
    lineage: dict[str, Any],
    *,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """Idempotently register the finalized run through Hermes' producer endpoint."""
    payload = hermes_run_payload(receipt, lineage)
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}/v1/research/evidence/objects",
        data=_canonical(payload),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            document = json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise RunBundleError(f"Hermes publication failed with HTTP {exc.code}: {detail}") from exc
    if not isinstance(document, dict) or str(document.get("object_id")) != payload["object_id"]:
        raise RunBundleError("Hermes publication receipt does not match the submitted identity")
    return {
        "schema_version": "hermes-run-publication-receipt-v1.0.0",
        "state": "published",
        "bundle_digest": receipt["bundle_digest"],
        "object_id": payload["object_id"],
        "content_digest": payload["content_digest"],
    }
