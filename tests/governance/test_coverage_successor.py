from copy import deepcopy

import pytest

from bt.governance.coverage_successor import plan_coverage_successor
from bt.governance.data_requirements import (
    fingerprint,
    plan_validation,
    RequirementsError,
)
from test_data_requirements import inputs


def proposal_inputs():
    frozen = inputs(days=550, instruments=("BTCUSDT", "SOLUSDT"))
    envelope = frozen["approved_window"].copy()
    frozen["approved_window"] = {
        "start": "2025-07-05T00:00:00+00:00",
        "end": envelope["end"],
    }
    lineage = fingerprint({k: v for k, v in frozen.items() if k != "coverage"})
    ledger = {"lineage_digest": lineage, "complete": True, "intervals": []}
    return dict(
        frozen_inputs=frozen,
        parent_plan=plan_validation(**frozen, mode="scientific"),
        mandate_digest="d" * 64,
        policy={
            "envelope_start": envelope["start"],
            "envelope_end": envelope["end"],
            "maximum_successors": 2,
            "selection_policy": "latest_common_eligible_interval",
            "outcome_search_allowed": False,
        },
        prior_successors=[],
        exposure_ledger=ledger | {"ledger_digest": fingerprint(ledger)},
    )


def test_latest_common_year_is_proposed_without_mutation_or_authority():
    args = proposal_inputs()
    before = deepcopy(args)
    result = plan_coverage_successor(**args)
    assert result["status"] == "proposed"
    assert result["scientific_plan"]["status"] == "ready"
    assert result["scientific_plan"]["instruments"] == ["BTCUSDT", "SOLUSDT"]
    assert result["execution_authority"] is False
    assert args == before
    assert plan_coverage_successor(**args) == result


def test_unchanged_inputs_do_not_generate_endless_successors():
    args = proposal_inputs()
    args["prior_successors"] = [plan_coverage_successor(**args)]
    assert plan_coverage_successor(**args)["reasons"] == [
        "unchanged_coverage_and_exposure_already_planned"
    ]


def test_successor_budget_is_enforced():
    args = proposal_inputs()
    args["policy"]["maximum_successors"] = 1
    args["prior_successors"] = [plan_coverage_successor(**args)]
    assert (
        "coverage_successor_budget_exhausted"
        in plan_coverage_successor(**args)["reasons"]
    )


def test_scaffolder_emits_proposal_but_never_authors_on_failed_original_preflight(
    tmp_path, monkeypatch
):
    import importlib.util
    import json
    from pathlib import Path

    args = proposal_inputs()
    frozen = args["frozen_inputs"]
    evidence = {
        "validation_requirements": {
            key: frozen[key]
            for key in (
                "requirements",
                "coverage",
                "maximum_history_days",
                "maximum_bar_variant_work",
            )
        },
        "discovery_candidate": {
            "data": {"research_timeframe": "20m", "instruments": frozen["instruments"]},
            "parameter_budget": {"maximum_variants": frozen["variants"]},
        },
        "dataset_bindings": frozen["dataset_bindings"],
        "window": frozen["approved_window"],
        "coverage_successor_context": {
            key: args[key]
            for key in (
                "mandate_digest",
                "policy",
                "prior_successors",
                "exposure_ledger",
            )
        },
    }
    source = tmp_path / "evidence.json"
    output = tmp_path / "manifest.json"
    source.write_text(json.dumps(evidence))
    script = Path(__file__).resolve().parents[2] / "scripts/scaffold_alpha_strategy.py"
    spec = importlib.util.spec_from_file_location("coverage_scaffold", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.subprocess, "check_output", lambda *a, **kw: "a" * 40)
    monkeypatch.setattr(
        module, "scaffold_strategy_package", lambda *a: pytest.fail("must not author")
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            str(script),
            "--evidence",
            str(source),
            "--output",
            str(output),
            "--repository",
            str(tmp_path),
        ],
    )
    assert module.main() == 2
    report = json.loads(output.read_text())
    assert report["success"] is False
    assert report["retryable"] is False
    assert report["coverage_successor_proposal"]["status"] == "proposed"


def test_exposed_outcomes_are_excluded_before_interval_selection():
    args = proposal_inputs()
    ledger = args["exposure_ledger"]
    ledger["intervals"] = [
        {"start": "2026-01-01T00:00:00+00:00", "end": "2026-07-05T00:00:00+00:00"}
    ]
    ledger["ledger_digest"] = fingerprint(
        {k: v for k, v in ledger.items() if k != "ledger_digest"}
    )
    assert plan_coverage_successor(**args)["status"] == "deferred"


def test_compute_failure_is_not_solved_by_searching_dates():
    args = proposal_inputs()
    args["frozen_inputs"]["maximum_bar_variant_work"] = 1
    args["parent_plan"] = plan_validation(**args["frozen_inputs"], mode="scientific")
    ledger = args["exposure_ledger"]
    ledger["lineage_digest"] = fingerprint(
        {k: v for k, v in args["frozen_inputs"].items() if k != "coverage"}
    )
    ledger["ledger_digest"] = fingerprint(
        {k: v for k, v in ledger.items() if k != "ledger_digest"}
    )
    assert "failure_not_coverage_only" in plan_coverage_successor(**args)["reasons"]


@pytest.mark.parametrize(
    "mutation", ["parent", "ledger", "authority", "lineage", "budget"]
)
def test_invalid_authority_and_lineage_fail_closed(mutation):
    args = proposal_inputs()
    if mutation == "parent":
        args["parent_plan"]["variants"] = 7
    elif mutation == "ledger":
        args["exposure_ledger"]["complete"] = False
    elif mutation == "authority":
        args["policy"]["outcome_search_allowed"] = True
    elif mutation == "budget":
        args["policy"]["maximum_successors"] = True
    else:
        result = plan_coverage_successor(**args)
        result["ordinal"] = 2
        result["successor_digest"] = fingerprint(
            {k: v for k, v in result.items() if k != "successor_digest"}
        )
        args["prior_successors"] = [result]
    with pytest.raises(RequirementsError):
        plan_coverage_successor(**args)
