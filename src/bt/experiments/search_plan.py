"""Prospective finite-search plans and exactly-once trial accounting."""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal


SEARCH_PLAN_SCHEMA_VERSION = "search-plan-v1.0.0"
SEARCH_LEDGER_SCHEMA_VERSION = "search-ledger-v1.0.0"
TrialStatus = Literal["registered", "leased", "succeeded", "failed", "cancelled"]


class SearchPlanError(ValueError):
    """A search cannot be registered without weakening prospective accounting."""


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _require_digest(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise SearchPlanError(f"{name} must be lowercase sha256")


def _validate_scalar(value: Any, *, path: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise SearchPlanError(f"{path} must be finite")
    if isinstance(value, (dict, list, tuple, set)):
        raise SearchPlanError(f"{path} must be a scalar")


@dataclass(frozen=True)
class SearchBudget:
    max_trials: int
    max_attempts_per_trial: int
    max_wallclock_seconds: int
    max_workers: int

    def validate(self) -> None:
        for name, value in asdict(self).items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise SearchPlanError(f"budget.{name} must be a positive integer")


@dataclass(frozen=True)
class StoppingRule:
    kind: Literal["exhaustive", "budget_only"]
    allow_early_success_stop: bool = False

    def validate(self) -> None:
        if self.allow_early_success_stop:
            raise SearchPlanError("outcome-dependent early stopping is not supported")


@dataclass(frozen=True)
class SearchPlan:
    family_id: str
    hypothesis_id: str
    hypothesis_digest: str
    dataset_snapshot_id: str
    dataset_digest: str
    repository_commit: str
    code_digest: str
    market_model_bundle_digest: str
    parameter_values: dict[str, tuple[Any, ...]]
    included_variants: tuple[dict[str, Any], ...]
    tiers: tuple[str, ...]
    seeds: tuple[int, ...]
    resources: dict[str, str | int | float | bool]
    budget: SearchBudget
    stopping_rule: StoppingRule

    def document(self) -> dict[str, Any]:
        self.validate()
        document = {"schema_version": SEARCH_PLAN_SCHEMA_VERSION, **asdict(self)}
        document["declared_cartesian_variants"] = self.declared_cartesian_variants
        document["included_variant_count"] = len(self.included_variants)
        document["excluded_variant_count"] = self.declared_cartesian_variants - len(self.included_variants)
        document["registered_trials"] = self.trial_count
        document["plan_digest"] = _digest(document)
        return document

    @property
    def digest(self) -> str:
        return str(self.document()["plan_digest"])

    @property
    def trial_count(self) -> int:
        return len(self.included_variants) * len(self.tiers) * len(self.seeds)

    @property
    def declared_cartesian_variants(self) -> int:
        return math.prod(len(values) for values in self.parameter_values.values())

    def validate(self) -> None:
        if not self.family_id.strip() or not self.hypothesis_id.strip():
            raise SearchPlanError("family_id and hypothesis_id are required")
        for name in (
            "hypothesis_digest", "dataset_digest", "code_digest", "market_model_bundle_digest"
        ):
            _require_digest(name, str(getattr(self, name)))
        if len(self.repository_commit) != 40 or any(
            character not in "0123456789abcdef" for character in self.repository_commit
        ):
            raise SearchPlanError("repository_commit must be lowercase hexadecimal length 40")
        if not self.dataset_snapshot_id.strip():
            raise SearchPlanError("dataset_snapshot_id is required")
        if not self.parameter_values:
            raise SearchPlanError("parameter_values must be non-empty")
        for key, values in self.parameter_values.items():
            if not key.strip() or not values:
                raise SearchPlanError("every parameter requires a name and finite value set")
            canonical_values: set[bytes] = set()
            for index, value in enumerate(values):
                _validate_scalar(value, path=f"parameter_values.{key}[{index}]")
                encoded = _canonical(value)
                if encoded in canonical_values:
                    raise SearchPlanError(f"parameter_values.{key} contains duplicates")
                canonical_values.add(encoded)
        if not self.included_variants:
            raise SearchPlanError("included_variants must be non-empty")
        allowed_keys = set(self.parameter_values)
        seen_variants: set[bytes] = set()
        for index, variant in enumerate(self.included_variants):
            if set(variant) != allowed_keys:
                raise SearchPlanError(f"included_variants[{index}] must contain every declared parameter exactly once")
            for key, value in variant.items():
                if not any(_canonical(value) == _canonical(allowed) for allowed in self.parameter_values[key]):
                    raise SearchPlanError(f"included_variants[{index}].{key} is outside declared values")
            encoded = _canonical(variant)
            if encoded in seen_variants:
                raise SearchPlanError("included_variants contains duplicates")
            seen_variants.add(encoded)
        if len(self.included_variants) > self.declared_cartesian_variants:
            raise SearchPlanError("included variants exceed the declared Cartesian grid")
        if not self.tiers or len(self.tiers) != len(set(self.tiers)):
            raise SearchPlanError("tiers must be non-empty and unique")
        if any(tier not in {"Tier1", "Tier2", "Tier3"} for tier in self.tiers):
            raise SearchPlanError("tiers contain an unsupported value")
        if not self.seeds or len(self.seeds) != len(set(self.seeds)):
            raise SearchPlanError("seeds must be non-empty and unique")
        if any(isinstance(seed, bool) or not isinstance(seed, int) or seed < 0 for seed in self.seeds):
            raise SearchPlanError("seeds must be non-negative integers")
        for key, value in self.resources.items():
            _validate_scalar(value, path=f"resources.{key}")
        self.budget.validate()
        self.stopping_rule.validate()
        if self.trial_count > self.budget.max_trials:
            raise SearchPlanError(
                f"cartesian trial count {self.trial_count} exceeds budget.max_trials={self.budget.max_trials}"
            )

    def trials(self) -> list[dict[str, Any]]:
        document = self.document()
        rows: list[dict[str, Any]] = []
        ordinal = 0
        for raw_parameters in self.included_variants:
            parameters = {key: raw_parameters[key] for key in sorted(raw_parameters)}
            for tier in self.tiers:
                for seed in self.seeds:
                    identity = {
                        "plan_digest": document["plan_digest"],
                        "parameters": parameters,
                        "tier": tier,
                        "seed": seed,
                    }
                    rows.append(
                        {
                            "ordinal": ordinal,
                            "trial_id": _digest(identity),
                            "plan_digest": document["plan_digest"],
                            "family_id": self.family_id,
                            "parameters": parameters,
                            "tier": tier,
                            "seed": seed,
                        }
                    )
                    ordinal += 1
        return rows


def compile_hypothesis_search_plan(
    *,
    contract: Any,
    family_id: str,
    hypothesis_digest: str,
    dataset_snapshot_id: str,
    dataset_digest: str,
    repository_commit: str,
    code_digest: str,
    market_model_bundle_digest: str,
    tiers: tuple[str, ...],
    seeds: tuple[int, ...],
    resources: dict[str, str | int | float | bool],
    budget: SearchBudget,
    stopping_rule: StoppingRule,
) -> SearchPlan:
    """Compile the existing hypothesis contract's filtered variants into a locked plan."""
    materialized = contract.materialize_grid()
    return SearchPlan(
        family_id=family_id,
        hypothesis_id=contract.schema.metadata.hypothesis_id,
        hypothesis_digest=hypothesis_digest,
        dataset_snapshot_id=dataset_snapshot_id,
        dataset_digest=dataset_digest,
        repository_commit=repository_commit,
        code_digest=code_digest,
        market_model_bundle_digest=market_model_bundle_digest,
        parameter_values=contract.schema.parameter_grid,
        included_variants=tuple(dict(row["params"]) for row in materialized),
        tiers=tiers,
        seeds=seeds,
        resources=resources,
        budget=budget,
        stopping_rule=stopping_rule,
    )


def bind_manifest_rows(plan: SearchPlan, manifest_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Expand existing variant/tier rows by seed and attach registered identities."""
    indexed: dict[tuple[bytes, str], dict[str, str]] = {}
    for row in manifest_rows:
        parameters = json.loads(row["params_json"])
        key = (_canonical(parameters), row["tier"])
        if key in indexed:
            raise SearchPlanError("manifest contains duplicate parameter/tier rows")
        indexed[key] = row
    output: list[dict[str, str]] = []
    for trial in plan.trials():
        key = (_canonical(trial["parameters"]), trial["tier"])
        source = indexed.get(key)
        if source is None:
            raise SearchPlanError("registered trial has no matching manifest row")
        parameters = {**trial["parameters"], "seed": trial["seed"]}
        ordinal = int(trial["ordinal"]) + 1
        seed_suffix = f"__seed-{trial['seed']}"
        output.append(
            {
                **source,
                "row_id": f"row_{ordinal:05d}",
                "params_json": json.dumps(parameters, sort_keys=True, separators=(",", ":")),
                "run_slug": f"{source['run_slug']}{seed_suffix}",
                "output_dir": f"{source['output_dir']}{seed_suffix}",
                "search_plan_digest": plan.digest,
                "trial_id": str(trial["trial_id"]),
                "search_family_id": plan.family_id,
                "seed": str(trial["seed"]),
                "attempt": "1",
            }
        )
    return output


def validate_registered_manifest_rows(plan: SearchPlan, rows: list[dict[str, str]]) -> None:
    expected = {trial["trial_id"] for trial in plan.trials()}
    actual: set[str] = set()
    for row in rows:
        if row.get("search_plan_digest") != plan.digest:
            raise SearchPlanError("manifest row search-plan digest mismatch")
        if row.get("search_family_id") != plan.family_id:
            raise SearchPlanError("manifest row search-family mismatch")
        trial_id = row.get("trial_id", "")
        if not trial_id or trial_id in actual:
            raise SearchPlanError("manifest trial identities must be present and unique")
        actual.add(trial_id)
        if row.get("attempt") != "1":
            raise SearchPlanError("initial registered manifest attempt must be 1")
    if actual != expected:
        raise SearchPlanError("manifest trial set does not match registered search plan")


def validate_search_plan_document(document: dict[str, Any]) -> None:
    if document.get("schema_version") != SEARCH_PLAN_SCHEMA_VERSION:
        raise SearchPlanError("unsupported search-plan schema version")
    supplied = document.get("plan_digest")
    expected = _digest({key: value for key, value in document.items() if key != "plan_digest"})
    if supplied != expected:
        raise SearchPlanError("search-plan digest mismatch")


class SearchLedger:
    """SQLite-backed immutable plan and exactly-once trial-attempt ledger."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS plans (
                plan_digest TEXT PRIMARY KEY,
                family_id TEXT NOT NULL,
                document_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trials (
                trial_id TEXT PRIMARY KEY,
                plan_digest TEXT NOT NULL REFERENCES plans(plan_digest),
                ordinal INTEGER NOT NULL,
                identity_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'registered',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                UNIQUE(plan_digest, ordinal)
            );
            CREATE TABLE IF NOT EXISTS attempts (
                trial_id TEXT NOT NULL REFERENCES trials(trial_id),
                attempt INTEGER NOT NULL,
                status TEXT NOT NULL,
                evidence_digest TEXT,
                PRIMARY KEY(trial_id, attempt)
            );
            """
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def register(self, plan: SearchPlan) -> dict[str, Any]:
        document = plan.document()
        encoded = _canonical(document).decode("ascii")
        with self.connection:
            existing = self.connection.execute(
                "SELECT document_json FROM plans WHERE plan_digest=?", (plan.digest,)
            ).fetchone()
            if existing is not None and existing[0] != encoded:
                raise SearchPlanError("plan digest collision with different semantics")
            self.connection.execute(
                "INSERT OR IGNORE INTO plans(plan_digest, family_id, document_json) VALUES (?, ?, ?)",
                (plan.digest, plan.family_id, encoded),
            )
            for trial in plan.trials():
                self.connection.execute(
                    "INSERT OR IGNORE INTO trials(trial_id, plan_digest, ordinal, identity_json) VALUES (?, ?, ?, ?)",
                    (trial["trial_id"], plan.digest, trial["ordinal"], _canonical(trial).decode("ascii")),
                )
        return self.summary(plan.digest)

    def begin_attempt(self, *, trial_id: str) -> dict[str, Any]:
        with self.connection:
            row = self.connection.execute(
                "SELECT t.status, t.attempt_count, p.document_json FROM trials t JOIN plans p USING(plan_digest) WHERE trial_id=?",
                (trial_id,),
            ).fetchone()
            if row is None:
                raise SearchPlanError("unknown trial_id")
            status, attempt_count, plan_json = row
            if status in {"succeeded", "cancelled"}:
                raise SearchPlanError(f"terminal trial cannot be attempted: {status}")
            max_attempts = int(json.loads(plan_json)["budget"]["max_attempts_per_trial"])
            attempt = int(attempt_count) + 1
            if attempt > max_attempts:
                raise SearchPlanError("trial attempt budget exhausted")
            self.connection.execute(
                "UPDATE trials SET status='leased', attempt_count=? WHERE trial_id=?",
                (attempt, trial_id),
            )
            self.connection.execute(
                "INSERT INTO attempts(trial_id, attempt, status) VALUES (?, ?, 'leased')",
                (trial_id, attempt),
            )
        return {"trial_id": trial_id, "attempt": attempt, "status": "leased"}

    def finish_attempt(
        self, *, trial_id: str, attempt: int, status: Literal["succeeded", "failed"], evidence_digest: str
    ) -> dict[str, Any]:
        _require_digest("evidence_digest", evidence_digest)
        with self.connection:
            row = self.connection.execute(
                "SELECT status FROM attempts WHERE trial_id=? AND attempt=?", (trial_id, attempt)
            ).fetchone()
            if row is None or row[0] != "leased":
                raise SearchPlanError("attempt is not the active leased attempt")
            self.connection.execute(
                "UPDATE attempts SET status=?, evidence_digest=? WHERE trial_id=? AND attempt=?",
                (status, evidence_digest, trial_id, attempt),
            )
            self.connection.execute(
                "UPDATE trials SET status=? WHERE trial_id=?", (status, trial_id)
            )
        return {"trial_id": trial_id, "attempt": attempt, "status": status, "evidence_digest": evidence_digest}

    def cancel_unleased(self, *, plan_digest: str) -> int:
        with self.connection:
            cursor = self.connection.execute(
                "UPDATE trials SET status='cancelled' WHERE plan_digest=? AND status='registered' AND attempt_count=0",
                (plan_digest,),
            )
        return int(cursor.rowcount)

    def summary(self, plan_digest: str) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT status, COUNT(*) FROM trials WHERE plan_digest=? GROUP BY status ORDER BY status",
            (plan_digest,),
        ).fetchall()
        attempts = self.connection.execute(
            "SELECT COUNT(*) FROM attempts a JOIN trials t USING(trial_id) WHERE t.plan_digest=?",
            (plan_digest,),
        ).fetchone()
        return {
            "schema_version": SEARCH_LEDGER_SCHEMA_VERSION,
            "plan_digest": plan_digest,
            "trials": {status: count for status, count in rows},
            "attempts_consumed": int(attempts[0] if attempts else 0),
        }
