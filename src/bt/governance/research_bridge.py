"""Governed compilation boundary between Hermes prose and Bulletproof execution."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any
import urllib.error
import urllib.request

from bt.hypotheses.contract import HypothesisContract


SCHEMA_VERSION = "governed-research-bridge-v1.0.0"
_HIDDEN_SEARCH = re.compile(
    r"\b(?:optimi[sz]e|find\s+the\s+best|keep\s+trying|until\s+profitable|"
    r"bayesian\s+search|genetic\s+search|random\s+search)\b",
    re.IGNORECASE,
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class BridgeError(ValueError):
    """A submission cannot cross the governed execution boundary."""


class ResearchTier(str, Enum):
    TIER_2A = "Tier2A"
    TIER_2B = "Tier2B"
    TIER_3 = "Tier3"


@dataclass(frozen=True)
class DatasetBinding:
    snapshot_id: str
    digest: str
    available_fields: tuple[str, ...]
    universe: str
    timeframe: str

    def validate(self) -> None:
        if not self.snapshot_id.strip():
            raise BridgeError("dataset snapshot_id is required")
        if not _SHA256.fullmatch(self.digest):
            raise BridgeError("dataset digest must be lowercase sha256")
        if not self.universe.strip() or not self.timeframe.strip():
            raise BridgeError("dataset universe and timeframe are required")


@dataclass(frozen=True)
class HypothesisSubmission:
    original_text: str
    hypothesis: str
    tier: str
    grid: dict[str, tuple[Any, ...]]
    dataset: DatasetBinding
    legacy_tier_resolution: str | None = None


BRIDGE_STAGES = (
    "approved",
    "registry_bound",
    "executed",
    "truth_validated",
    "bundle_finalized",
    "independently_reviewed",
    "published",
    "memory_confirmed",
    "complete",
)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def resolve_tier(value: str, *, legacy_resolution: str | None = None) -> tuple[ResearchTier, str | None]:
    normalized = value.strip().lower().replace("_", "").replace("-", "")
    direct = {
        "tier2a": ResearchTier.TIER_2A,
        "2a": ResearchTier.TIER_2A,
        "tier2b": ResearchTier.TIER_2B,
        "2b": ResearchTier.TIER_2B,
        "tier3": ResearchTier.TIER_3,
        "3": ResearchTier.TIER_3,
    }
    if normalized in direct:
        return direct[normalized], None
    if normalized != "tier2":
        raise BridgeError(f"unknown research tier: {value!r}")
    if legacy_resolution is None:
        raise BridgeError("legacy Tier2 is ambiguous; choose Tier2A or Tier2B")
    resolved, _ = resolve_tier(legacy_resolution)
    if resolved not in {ResearchTier.TIER_2A, ResearchTier.TIER_2B}:
        raise BridgeError("legacy Tier2 can resolve only to Tier2A or Tier2B")
    return resolved, f"Tier2 resolved explicitly to {resolved.value}"


def _strategy_identity(contract: HypothesisContract) -> str:
    identity = contract.schema.entry.get("strategy")
    if not isinstance(identity, str) or not identity.strip():
        raise BridgeError("registered hypothesis has no strategy identity")
    return identity


def _validate_grid(
    contract: HypothesisContract,
    requested: dict[str, tuple[Any, ...]],
    *,
    max_variants: int,
) -> tuple[dict[str, tuple[Any, ...]], int]:
    if not requested:
        raise BridgeError("an explicit finite parameter grid is required")
    registered = contract.schema.parameter_grid
    unknown = sorted(set(requested) - set(registered))
    if unknown:
        raise BridgeError(f"unknown parameters: {unknown}")
    normalized: dict[str, tuple[Any, ...]] = {}
    count = 1
    for name in sorted(requested):
        values = tuple(requested[name])
        if not values:
            raise BridgeError(f"parameter {name} has no values")
        if len(values) != len({_canonical(value) for value in values}):
            raise BridgeError(f"parameter {name} contains duplicate values")
        unavailable = [value for value in values if value not in registered[name]]
        if unavailable:
            raise BridgeError(f"parameter {name} has unregistered values: {unavailable}")
        normalized[name] = values
        count *= len(values)
    if count > max_variants:
        raise BridgeError(f"Cartesian grid has {count} variants; budget permits {max_variants}")
    return normalized, count


def compile_submission(
    submission: HypothesisSubmission,
    *,
    repository_root: Path,
    repository_commit: str,
    max_variants: int = 64,
) -> dict[str, Any]:
    """Compile founder input to an immutable, non-executable proposal."""
    if _HIDDEN_SEARCH.search(submission.original_text):
        raise BridgeError("hidden or outcome-dependent optimization language is forbidden")
    if not re.fullmatch(r"[0-9a-f]{40}", repository_commit):
        raise BridgeError("repository commit must be a lowercase Git SHA-1")
    submission.dataset.validate()
    tier, resolution = resolve_tier(
        submission.tier,
        legacy_resolution=submission.legacy_tier_resolution,
    )
    registry = {
        "csi-gated displacement trend": "research/hypotheses/l7_h1_csi_gated_displacement_trend.yaml",
        "l7-h1": "research/hypotheses/l7_h1_csi_gated_displacement_trend.yaml",
        "l7_h1_csi_gated_displacement_trend": "research/hypotheses/l7_h1_csi_gated_displacement_trend.yaml",
    }
    key = submission.hypothesis.strip().lower()
    relative = registry.get(key)
    if relative is None:
        raise BridgeError("hypothesis is not registered; bounded engineering generation is required")
    path = repository_root / relative
    contract = HypothesisContract.from_yaml(path)
    grid, variant_count = _validate_grid(contract, submission.grid, max_variants=max_variants)
    required_sources = set()
    if _strategy_identity(contract) == "l7_h1_csi_gated_displacement_trend":
        required_sources = {"ohlcv", "funding", "open_interest", "volume"}
    available = set(submission.dataset.available_fields)
    missing = sorted(required_sources - available)
    if missing:
        raise BridgeError(f"dataset lacks required auxiliary fields: {missing}")
    hypothesis_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    core = {
        "schema_version": SCHEMA_VERSION,
        "state": "awaiting_approval",
        "authority": {
            "capital": "prohibited",
            "live_orders": "prohibited",
            "self_approval": "prohibited",
            "production_promotion": "prohibited",
        },
        "source": {
            "original_text_digest": hashlib.sha256(submission.original_text.encode()).hexdigest(),
            "original_text": submission.original_text,
        },
        "resolution": {
            "disposition": "reuse_registered_strategy",
            "hypothesis_path": relative,
            "hypothesis_digest": hypothesis_digest,
            "strategy_identity": _strategy_identity(contract),
            "repository_commit": repository_commit,
            "tier": tier.value,
            "legacy_tier_resolution": resolution,
        },
        "dataset": {
            "snapshot_id": submission.dataset.snapshot_id,
            "digest": submission.dataset.digest,
            "available_fields": sorted(available),
            "universe": submission.dataset.universe,
            "timeframe": submission.dataset.timeframe,
        },
        "search": {
            "parameter_grid": {name: list(values) for name, values in grid.items()},
            "variant_count": variant_count,
            "stopping_rule": "exhaustive",
            "max_variants": max_variants,
        },
        "required_gates": [
            "founder_specification_approval",
            "schema_and_causality_review",
            "prospective_registry_binding",
            "native_classic_execution",
            "truth_validation",
            "atomic_bundle_finalization",
            "independent_statistical_review",
            "independent_adversarial_review",
            "hermes_laboratory_publication",
        ],
    }
    digest_core = {key: value for key, value in core.items() if key != "state"}
    return core | {"proposal_digest": _digest(digest_core)}


def validate_approved_proposal(document: dict[str, Any], approved_digest: str) -> None:
    embedded = document.get("proposal_digest")
    core = {
        key: value
        for key, value in document.items()
        if key not in {"proposal_digest", "state"}
    }
    actual = _digest(core)
    if embedded != actual or approved_digest != actual:
        raise BridgeError("proposal changed after approval")
    if document.get("state") != "approved":
        raise BridgeError("proposal is not approved")
    authority = document.get("authority", {})
    if any(authority.get(key) != "prohibited" for key in ("capital", "live_orders", "self_approval", "production_promotion")):
        raise BridgeError("proposal weakens the no-capital authority boundary")


class BridgeLedger:
    """Durable monotone lifecycle; it stores receipts, never research artifacts."""

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path)
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS governed_research_bridges (
                proposal_digest TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                proposal_json TEXT NOT NULL,
                receipts_json TEXT NOT NULL
            )
            """
        )
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def register(self, proposal: dict[str, Any]) -> dict[str, Any]:
        digest = str(proposal.get("proposal_digest", ""))
        if not _SHA256.fullmatch(digest):
            raise BridgeError("proposal digest is invalid")
        encoded = _canonical(proposal).decode("ascii")
        existing = self._db.execute(
            "SELECT proposal_json FROM governed_research_bridges WHERE proposal_digest = ?",
            (digest,),
        ).fetchone()
        if existing is not None and existing[0] != encoded:
            raise BridgeError("proposal digest collision")
        self._db.execute(
            "INSERT OR IGNORE INTO governed_research_bridges VALUES (?, ?, ?, ?)",
            (digest, "awaiting_approval", encoded, "{}"),
        )
        self._db.commit()
        return self.status(digest)

    def advance(
        self,
        proposal_digest: str,
        *,
        expected_state: str,
        next_state: str,
        receipt: dict[str, Any],
    ) -> dict[str, Any]:
        if next_state not in BRIDGE_STAGES:
            raise BridgeError(f"unknown bridge state: {next_state}")
        row = self._db.execute(
            "SELECT state, receipts_json FROM governed_research_bridges WHERE proposal_digest = ?",
            (proposal_digest,),
        ).fetchone()
        if row is None:
            raise BridgeError("bridge proposal is not registered")
        state, encoded_receipts = row
        if state == next_state:
            receipts = json.loads(encoded_receipts)
            if receipts.get(next_state) != receipt:
                raise BridgeError("stage receipt identity is immutable")
            return self.status(proposal_digest)
        if state != expected_state:
            raise BridgeError(f"bridge is {state}, expected {expected_state}")
        expected_index = -1 if expected_state == "awaiting_approval" else BRIDGE_STAGES.index(expected_state)
        if BRIDGE_STAGES.index(next_state) != expected_index + 1:
            raise BridgeError("bridge stages cannot be skipped or reordered")
        if not receipt or not isinstance(receipt, dict):
            raise BridgeError("a digest-bound stage receipt is required")
        receipts = json.loads(encoded_receipts)
        receipts[next_state] = receipt
        updated = self._db.execute(
            "UPDATE governed_research_bridges SET state = ?, receipts_json = ? "
            "WHERE proposal_digest = ? AND state = ?",
            (next_state, _canonical(receipts).decode("ascii"), proposal_digest, expected_state),
        )
        if updated.rowcount != 1:
            raise BridgeError("concurrent bridge state change detected")
        self._db.commit()
        return self.status(proposal_digest)

    def status(self, proposal_digest: str) -> dict[str, Any]:
        row = self._db.execute(
            "SELECT state, receipts_json FROM governed_research_bridges WHERE proposal_digest = ?",
            (proposal_digest,),
        ).fetchone()
        if row is None:
            raise BridgeError("bridge proposal is not registered")
        return {
            "proposal_digest": proposal_digest,
            "state": row[0],
            "receipts": json.loads(row[1]),
        }


def register_with_hermes(*, api_url: str, token: str, proposal: dict[str, Any]) -> dict[str, Any]:
    return _hermes_request(api_url, token, "POST", "/v1/research/governed-bridges", {"proposal": proposal})


def advance_in_hermes(
    *, api_url: str, token: str, bridge_id: str, expected_state: str,
    next_state: str, receipt: dict[str, Any],
) -> dict[str, Any]:
    return _hermes_request(
        api_url, token, "POST", f"/v1/research/governed-bridges/{bridge_id}/advance",
        {"expected_state": expected_state, "next_state": next_state, "receipt": receipt},
    )


def _hermes_request(api_url: str, token: str, method: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}{path}", method=method, data=_canonical(payload),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise BridgeError(f"Hermes bridge request failed with HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise BridgeError("Hermes bridge is temporarily unavailable") from exc
    if not isinstance(result, dict):
        raise BridgeError("Hermes bridge returned an invalid receipt")
    return result
