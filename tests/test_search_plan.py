from __future__ import annotations

from dataclasses import replace

import pytest

from bt.experiments.search_plan import (
    SearchBudget,
    SearchLedger,
    SearchPlan,
    SearchPlanError,
    StoppingRule,
    bind_manifest_rows,
    compile_hypothesis_search_plan,
    validate_registered_manifest_rows,
    validate_search_plan_document,
)
from bt.hypotheses.contract import HypothesisContract


def _plan(**overrides) -> SearchPlan:
    values = {
        "family_id": "family-csi-v1",
        "hypothesis_id": "CSI-GATED-DISPLACEMENT",
        "hypothesis_digest": "1" * 64,
        "dataset_snapshot_id": "snapshot-1",
        "dataset_digest": "2" * 64,
        "repository_commit": "3" * 40,
        "code_digest": "4" * 64,
        "market_model_bundle_digest": "5" * 64,
        "parameter_values": {"d0": (1.8, 2.2), "theta": (0.7, 0.8)},
        "included_variants": (
            {"d0": 1.8, "theta": 0.7},
            {"d0": 1.8, "theta": 0.8},
            {"d0": 2.2, "theta": 0.7},
            {"d0": 2.2, "theta": 0.8},
        ),
        "tiers": ("Tier2", "Tier3"),
        "seeds": (7,),
        "resources": {"worker_class": "cpu", "memory_gb": 8},
        "budget": SearchBudget(max_trials=8, max_attempts_per_trial=2, max_wallclock_seconds=3600, max_workers=2),
        "stopping_rule": StoppingRule(kind="exhaustive"),
    }
    values.update(overrides)
    return SearchPlan(**values)


def test_plan_materializes_finite_deterministic_trial_identities() -> None:
    first = _plan()
    second = _plan(parameter_values={"theta": (0.7, 0.8), "d0": (1.8, 2.2)})
    assert first.digest == second.digest
    assert first.trial_count == 8
    assert first.trials() == second.trials()
    assert len({row["trial_id"] for row in first.trials()}) == 8


def test_unbounded_invalid_duplicate_and_over_budget_plans_fail_closed() -> None:
    with pytest.raises(SearchPlanError, match="non-empty"):
        _plan(parameter_values={}).document()
    with pytest.raises(SearchPlanError, match="duplicates"):
        _plan(parameter_values={"d0": (1.8, 1.8)}).document()
    with pytest.raises(SearchPlanError, match="finite"):
        _plan(parameter_values={"d0": (float("nan"),)}).document()
    with pytest.raises(SearchPlanError, match="exceeds"):
        _plan(budget=SearchBudget(7, 1, 60, 1)).document()
    with pytest.raises(SearchPlanError, match="outcome-dependent"):
        _plan(stopping_rule=StoppingRule(kind="exhaustive", allow_early_success_stop=True)).document()


def test_grid_expansion_creates_new_plan_and_trial_identities() -> None:
    base = _plan()
    expanded = replace(
        base,
        parameter_values={**base.parameter_values, "k_stop": (3, 4)},
        included_variants=tuple(
            {**variant, "k_stop": k_stop}
            for variant in base.included_variants
            for k_stop in (3, 4)
        ),
        budget=replace(base.budget, max_trials=16),
    )
    assert expanded.digest != base.digest
    assert not ({row["trial_id"] for row in base.trials()} & {row["trial_id"] for row in expanded.trials()})


def test_ledger_charges_trials_once_and_retries_retain_identity(tmp_path) -> None:
    plan = _plan()
    ledger = SearchLedger(tmp_path / "search.sqlite3")
    try:
        first = ledger.register(plan)
        second = ledger.register(plan)
        assert first == second
        assert first["trials"] == {"registered": 8}
        trial_id = plan.trials()[0]["trial_id"]

        attempt_one = ledger.begin_attempt(trial_id=trial_id)
        assert attempt_one == {"trial_id": trial_id, "attempt": 1, "status": "leased"}
        ledger.finish_attempt(trial_id=trial_id, attempt=1, status="failed", evidence_digest="a" * 64)
        attempt_two = ledger.begin_attempt(trial_id=trial_id)
        assert attempt_two["trial_id"] == trial_id
        assert attempt_two["attempt"] == 2
        ledger.finish_attempt(trial_id=trial_id, attempt=2, status="succeeded", evidence_digest="b" * 64)

        with pytest.raises(SearchPlanError, match="terminal"):
            ledger.begin_attempt(trial_id=trial_id)
        summary = ledger.summary(plan.digest)
        assert summary["attempts_consumed"] == 2
        assert summary["trials"] == {"registered": 7, "succeeded": 1}
        assert ledger.cancel_unleased(plan_digest=plan.digest) == 7
        assert ledger.summary(plan.digest)["trials"] == {"cancelled": 7, "succeeded": 1}
    finally:
        ledger.close()


def test_attempt_budget_and_evidence_identity_fail_closed(tmp_path) -> None:
    plan = _plan(budget=SearchBudget(8, 1, 60, 1))
    ledger = SearchLedger(tmp_path / "search.sqlite3")
    try:
        ledger.register(plan)
        trial_id = plan.trials()[0]["trial_id"]
        ledger.begin_attempt(trial_id=trial_id)
        ledger.finish_attempt(trial_id=trial_id, attempt=1, status="failed", evidence_digest="a" * 64)
        with pytest.raises(SearchPlanError, match="budget exhausted"):
            ledger.begin_attempt(trial_id=trial_id)
        with pytest.raises(SearchPlanError, match="sha256"):
            ledger.finish_attempt(trial_id=trial_id, attempt=1, status="failed", evidence_digest="bad")
    finally:
        ledger.close()


def test_compiler_binds_filtered_hypothesis_variants() -> None:
    contract = HypothesisContract.from_dict(
        {
            "hypothesis_id": "FILTERED",
            "title": "filtered grid",
            "research_layer": "fixture",
            "hypothesis_family": "fixture",
            "version": "1",
            "parameter_grid": {"z_reentry": [1.0, 2.0], "z_ext": [1.5, 2.5]},
        }
    )
    plan = compile_hypothesis_search_plan(
        contract=contract,
        family_id="filtered-family",
        hypothesis_digest="1" * 64,
        dataset_snapshot_id="snapshot",
        dataset_digest="2" * 64,
        repository_commit="3" * 40,
        code_digest="4" * 64,
        market_model_bundle_digest="5" * 64,
        tiers=("Tier2",),
        seeds=(7,),
        resources={"max_workers": 1},
        budget=SearchBudget(3, 1, 60, 1),
        stopping_rule=StoppingRule(kind="exhaustive"),
    )
    document = plan.document()
    assert document["declared_cartesian_variants"] == 4
    assert document["included_variant_count"] == 3
    assert document["excluded_variant_count"] == 1
    assert document["registered_trials"] == 3


def test_registered_manifest_expands_seeds_and_detects_drift() -> None:
    plan = _plan()
    rows = []
    for index, variant in enumerate(plan.included_variants, start=1):
        for tier in plan.tiers:
            rows.append(
                {
                    "row_id": f"row_{index:05d}",
                    "hypothesis_id": plan.hypothesis_id,
                    "hypothesis_path": "research/hypotheses/fixture.yaml",
                    "phase": "tier2",
                    "tier": tier,
                    "variant_id": f"g{index:05d}",
                    "config_hash": "a" * 64,
                    "params_json": __import__("json").dumps(variant, sort_keys=True, separators=(",", ":")),
                    "run_slug": f"g{index:05d}__{tier.lower()}",
                    "output_dir": f"runs/g{index:05d}__{tier.lower()}",
                    "expected_status": "pending",
                    "enabled": "true",
                    "notes": "",
                }
            )
    bound = bind_manifest_rows(plan, rows)
    validate_registered_manifest_rows(plan, bound)
    assert len(bound) == 8
    assert all(row["attempt"] == "1" for row in bound)
    assert all("seed" in __import__("json").loads(row["params_json"]) for row in bound)
    drifted = [dict(row) for row in bound]
    drifted[0]["search_plan_digest"] = "f" * 64
    with pytest.raises(SearchPlanError, match="digest mismatch"):
        validate_registered_manifest_rows(plan, drifted)


def test_serialized_search_plan_is_tamper_evident() -> None:
    document = _plan().document()
    validate_search_plan_document(document)
    document["budget"]["max_trials"] = 999
    with pytest.raises(SearchPlanError, match="digest mismatch"):
        validate_search_plan_document(document)
