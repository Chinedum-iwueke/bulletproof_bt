"""Canonical identity compatibility helpers shared with evidence consumers."""

# This contract intentionally has the same implementation as Hermes. Keep changes
# synchronized until it moves into a separately versioned shared package.
from __future__ import annotations

import re
import uuid
from typing import Any

SCHEMA_VERSION = "canonical-identity-v1.0.0"
_SCHEMA_RE = re.compile(r"^canonical-identity-v(?P<major>[0-9]+)\.[0-9]+\.[0-9]+$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9._-]{0,99}$")


class IdentityCompatibilityError(ValueError):
    """An identity record cannot be consumed without changing its meaning."""


def _major(version: str) -> int:
    match = _SCHEMA_RE.fullmatch(version)
    if match is None or int(match.group("major")) != 1:
        raise IdentityCompatibilityError(
            f"unsupported canonical identity schema version: {version!r}"
        )
    return 1


def _require_name(value: str, label: str) -> str:
    if _NAME_RE.fullmatch(value) is None:
        raise IdentityCompatibilityError(f"invalid {label}: {value!r}")
    return value


def _require_digest(value: str) -> str:
    if _DIGEST_RE.fullmatch(value) is None:
        raise IdentityCompatibilityError("content digest must be lowercase SHA-256")
    return value


def normalize_identity(
    document: dict[str, Any],
    *,
    object_type: str | None = None,
    namespace: str | None = None,
    id_field: str = "id",
    digest_field: str = "record_digest",
    producer_schema_version: str = "legacy-v0",
    content_version: str = "1",
    canonical_object_id: str | None = None,
) -> dict[str, Any]:
    """Return a v1 envelope from a canonical or explicitly described legacy record."""
    if "schema_version" in document and "object_id" in document:
        version = str(document["schema_version"])
        _major(version)
        normalized = dict(document)
    else:
        if object_type is None or namespace is None:
            raise IdentityCompatibilityError(
                "legacy identity requires object_type and namespace"
            )
        try:
            native_id = str(document[id_field])
            object_id = str(uuid.UUID(canonical_object_id or native_id))
            digest = str(document[digest_field])
        except (KeyError, TypeError, ValueError) as exc:
            raise IdentityCompatibilityError("legacy identity fields are invalid") from exc
        normalized = {
            "schema_version": SCHEMA_VERSION,
            "object_id": object_id,
            "object_type": object_type,
            "content_version": content_version,
            "content_digest": digest,
            "producer": {
                "system": namespace,
                "native_type": object_type,
                "native_id": native_id,
                "schema_version": producer_schema_version,
            },
            "aliases": [
                {
                    "namespace": namespace,
                    "object_type": object_type,
                    "value": native_id,
                }
            ],
            "supersedes_object_id": None,
        }
    validate_identity(normalized)
    return normalized


def validate_identity(document: dict[str, Any]) -> None:
    required = {
        "schema_version", "object_id", "object_type", "content_version",
        "content_digest", "producer", "aliases", "supersedes_object_id",
    }
    if set(document) != required:
        raise IdentityCompatibilityError("canonical identity fields do not match v1")
    _major(str(document["schema_version"]))
    try:
        uuid.UUID(str(document["object_id"]))
        if document["supersedes_object_id"] is not None:
            uuid.UUID(str(document["supersedes_object_id"]))
    except (TypeError, ValueError) as exc:
        raise IdentityCompatibilityError("canonical object IDs must be UUIDs") from exc
    object_type = _require_name(str(document["object_type"]), "object_type")
    if not str(document["content_version"]):
        raise IdentityCompatibilityError("content_version cannot be empty")
    _require_digest(str(document["content_digest"]))
    producer = document["producer"]
    if not isinstance(producer, dict) or set(producer) != {"system", "native_type", "native_id", "schema_version"}:
        raise IdentityCompatibilityError("producer fields do not match v1")
    _require_name(str(producer["system"]), "producer system")
    _require_name(str(producer["native_type"]), "producer native_type")
    if not str(producer["native_id"]) or not str(producer["schema_version"]):
        raise IdentityCompatibilityError("producer identity fields cannot be empty")
    aliases = document["aliases"]
    if not isinstance(aliases, list) or not aliases:
        raise IdentityCompatibilityError("at least one immutable alias is required")
    seen: set[tuple[str, str, str]] = set()
    for alias in aliases:
        if not isinstance(alias, dict) or set(alias) != {"namespace", "object_type", "value"}:
            raise IdentityCompatibilityError("alias fields do not match v1")
        key = (_require_name(str(alias["namespace"]), "alias namespace"), _require_name(str(alias["object_type"]), "alias object_type"), str(alias["value"]))
        if not key[2] or key in seen:
            raise IdentityCompatibilityError("aliases must be non-empty and unique")
        seen.add(key)
    if object_type != str(producer["native_type"]):
        raise IdentityCompatibilityError("producer native_type must match object_type")
    producer_alias = (str(producer["system"]), str(producer["native_type"]), str(producer["native_id"]))
    if producer_alias not in seen:
        raise IdentityCompatibilityError("producer native identity must be an alias")


def assert_immutable_identity(previous: dict[str, Any], current: dict[str, Any]) -> None:
    """Reject identity reassignment or digest mutation within one content version."""
    validate_identity(previous)
    validate_identity(current)
    for field in ("object_id", "object_type"):
        if previous[field] != current[field]:
            raise IdentityCompatibilityError(f"canonical {field} is immutable")
    if previous["producer"] != current["producer"]:
        raise IdentityCompatibilityError("producer identity is immutable")
    if not set(map(_alias_key, previous["aliases"])).issubset(set(map(_alias_key, current["aliases"]))):
        raise IdentityCompatibilityError("canonical aliases cannot be removed")
    if previous["content_version"] != current["content_version"]:
        raise IdentityCompatibilityError("content versions are immutable; create a successor object")
    if previous["content_digest"] != current["content_digest"]:
        raise IdentityCompatibilityError("content digest mutation is forbidden")


def _alias_key(alias: dict[str, str]) -> tuple[str, str, str]:
    return alias["namespace"], alias["object_type"], alias["value"]
