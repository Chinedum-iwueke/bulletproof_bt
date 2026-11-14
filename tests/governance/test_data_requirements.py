from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from bt.governance.data_requirements import (
    DataRequirements,
    RequirementsError,
    assess_event_support,
    coverage_receipt,
    fingerprint,
    plan_validation,
    preflight_evidence,
    verify_execution_plan,
)


def inputs(days=370, instruments=("TRBUSDT",), minutes=20):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=days)
    bindings = [
        {"instruments": [name], "dataset_digest": fingerprint(name)}
        for name in instruments
    ]
    coverage = [
        coverage_receipt(
            instrument=name,
            binding=binding,
            bar_minutes=minutes,
            eligible_decisions=[
                (start + timedelta(minutes=minutes * i)).isoformat()
                for i in range(days * 1440 // minutes)
            ],
            eligibility_evidence_digest="e" * 64,
        )
        for name, binding in zip(instruments, bindings)
    ]
    return {
        "requirements": {
            "research_bar_minutes": minutes,
            "feature_warmup_bars": 144,
            "minimum_training_bars": 5000,
            "minimum_validation_bars": 1000,
            "minimum_test_bars": 1000,
            "target_horizon_bars": 4,
            "purge_bars": 4,
            "embargo_bars": 2,
            "minimum_scientific_window_days": 365,
            "smoke_bars": 288,
            "minimum_event_support": {
                "positive_direction": 40,
                "negative_direction": 40,
            },
            "rationale": "Approved training support and frozen one-year evaluation",
            "basis_digest": "b" * 64,
        },
        "coverage": coverage,
        "dataset_bindings": bindings,
        "instruments": list(instruments),
        "source_commit": "a" * 40,
        "approved_window": {"start": start.isoformat(), "end": end.isoformat()},
        "maximum_history_days": 400,
        "maximum_bar_variant_work": 10_000_000,
        "variants": 4,
    }


def test_trb_short_smoke_and_long_science_are_separate():
    args = inputs()
    before = deepcopy(args)
    smoke = plan_validation(**args, mode="smoke")
    science = plan_validation(**args, mode="scientific")
    assert args == before
    assert smoke["status"] == science["status"] == "ready"
    assert "training" not in smoke["partitions"]
    assert smoke["scientific_certification"] is False
    partitions = science["partitions"]
    parse = datetime.fromisoformat
    assert parse(partitions["feature_warmup"]["end"]) == parse(
        partitions["training"]["start"]
    )
    assert (
        parse(partitions["training"]["end"]) - parse(partitions["training"]["start"])
    ) / timedelta(minutes=20) >= 5000
    assert parse(partitions["training"]["end"]) < parse(
        partitions["validation"]["start"]
    )
    assert parse(partitions["validation"]["end"]) < parse(partitions["test"]["start"])
    assert science["event_support"]["status"] == "not_yet_observed"
    assert science["execution_authority"] is False
    assert plan_validation(**args, mode="scientific") == science


def test_31_days_never_passes_scientific_support_but_can_smoke():
    args = inputs(days=31)
    assert plan_validation(**args, mode="smoke")["status"] == "ready"
    plan = plan_validation(**args, mode="scientific")
    assert plan["status"] == "deferred"
    assert plan["available_contiguous_bars_per_asset"] == 2232
    assert plan["bar_shortfall"] > 0
    assert not plan["partitions"]


@pytest.mark.parametrize("horizon", [1, 4, 12])
def test_smoke_reserves_outcomes_without_extending_decision_budget(horizon):
    args = inputs()
    args["requirements"]["target_horizon_bars"] = horizon
    plan = plan_validation(**args, mode="smoke")
    parts = plan["partitions"]
    parse = datetime.fromisoformat
    step = timedelta(minutes=20)
    assert parse(parts["smoke"]["end"]) == parse(parts["target_tail"]["start"])
    assert (parse(parts["smoke"]["end"]) - parse(parts["smoke"]["start"])) / step == 288
    assert (parse(parts["target_tail"]["end"]) - parse(parts["target_tail"]["start"])) / step == horizon
    assert plan["required_bars_per_asset"] == 144 + 288 + horizon
    assert plan["bar_variant_work"] == (144 + 288 + horizon) * 4
    assert parse(parts["target_tail"]["end"]) <= parse(args["approved_window"]["end"])


def test_smoke_defers_when_only_warmup_and_decisions_fit():
    args = inputs()
    receipt = args["coverage"][0]
    start = datetime.fromisoformat(receipt["intervals"][0]["start"])
    receipt["intervals"] = [{"start": start.isoformat(), "end": (start + timedelta(minutes=20 * 432)).isoformat()}]
    receipt["receipt_digest"] = fingerprint({k: v for k, v in receipt.items() if k != "receipt_digest"})
    plan = plan_validation(**args, mode="smoke")
    assert plan["status"] == "deferred"
    assert plan["bar_shortfall"] == 4
    assert not plan["partitions"]


def test_smoke_execution_window_includes_frozen_outcome_tail():
    args = inputs()
    plan = plan_validation(**args, mode="smoke")
    immutable = {
        "dataset_bindings": args["dataset_bindings"], "instruments": args["instruments"],
        "window": args["approved_window"], "maximum_variants": args["variants"],
        "validation_requirements": {key: args[key] for key in (
            "requirements", "coverage", "maximum_history_days", "maximum_bar_variant_work")},
    }
    assignment = {
        "base_ref": args["source_commit"], "execution_class": "commissioning",
        "validation_plan": plan, "window_start": plan["partitions"]["smoke"]["start"],
        "window_end": plan["partitions"]["target_tail"]["end"],
    }
    assert verify_execution_plan(assignment, immutable) == plan
    with pytest.raises(RequirementsError, match="execution window differs"):
        verify_execution_plan(assignment | {"window_end": plan["partitions"]["smoke"]["end"]}, immutable)


def test_budget_shortfalls_do_not_expand_authority():
    args = inputs()
    args.update(maximum_history_days=31, maximum_bar_variant_work=100)
    plan = plan_validation(**args, mode="scientific")
    assert plan["reasons"] == [
        "history_budget_insufficient",
        "compute_budget_insufficient",
    ]
    assert not plan["partitions"]


def test_basket_uses_intersection_not_sum_or_extent():
    args = inputs(instruments=("BTCUSDT", "SOLUSDT"))
    receipt = args["coverage"][1]
    receipt["intervals"] = [
        {"start": "2025-06-01T00:00:00+00:00", "end": "2025-07-01T00:00:00+00:00"}
    ]
    receipt["receipt_digest"] = fingerprint(
        {k: v for k, v in receipt.items() if k != "receipt_digest"}
    )
    plan = plan_validation(**args, mode="scientific")
    assert plan["available_contiguous_bars_per_asset"] == 30 * 72
    assert plan["status"] == "deferred"


def test_gaps_are_not_counted_as_valid_observations():
    args = inputs()
    receipt = args["coverage"][0]
    receipt["intervals"] = [
        {"start": "2025-01-01T00:00:00+00:00", "end": "2025-06-01T00:00:00+00:00"},
        {"start": "2025-06-02T00:00:00+00:00", "end": "2026-01-02T00:00:00+00:00"},
    ]
    receipt["receipt_digest"] = fingerprint(
        {k: v for k, v in receipt.items() if k != "receipt_digest"}
    )
    assert plan_validation(**args, mode="scientific")["status"] == "deferred"


@pytest.mark.parametrize("field", ["source_commit", "coverage", "requirements"])
def test_pins_bind_plan_digest(field):
    args = inputs()
    previous = plan_validation(**args, mode="smoke")
    if field == "source_commit":
        args[field] = "c" * 40
    elif field == "requirements":
        args[field]["feature_warmup_bars"] += 1
    else:
        args[field][0]["eligibility_evidence_digest"] = "d" * 64
        args[field][0]["receipt_digest"] = fingerprint(
            {k: v for k, v in args[field][0].items() if k != "receipt_digest"}
        )
    assert (
        plan_validation(**args, mode="smoke")["plan_digest"] != previous["plan_digest"]
    )


def test_tampered_or_wrong_binding_coverage_fails_closed():
    args = inputs()
    args["dataset_bindings"][0]["dataset_digest"] = "different"
    with pytest.raises(RequirementsError, match="identity or digest"):
        plan_validation(**args, mode="smoke")


def test_no_prose_or_implicit_requirements():
    with pytest.raises(RequirementsError, match="data_requirements_missing"):
        preflight_evidence({}, source_commit="a" * 40)
    with pytest.raises(RequirementsError, match="missing or malformed"):
        DataRequirements.parse({"minimum_training_bars": 5000})


@pytest.mark.parametrize("minutes", [5, 7, 12, 120])
def test_arbitrary_minute_timeframes(minutes):
    args = inputs(minutes=minutes)
    assert plan_validation(**args, mode="smoke")["status"] == "ready"


def test_coverage_rejects_duplicate_clock_and_bool_requirements():
    with pytest.raises(RequirementsError, match="sorted and unique"):
        coverage_receipt(
            instrument="BTC",
            binding={},
            bar_minutes=20,
            eligible_decisions=["2025-01-01T00:00:00Z"] * 2,
            eligibility_evidence_digest="e" * 64,
        )
    requirements = inputs(days=1)["requirements"]
    requirements["minimum_training_bars"] = True
    with pytest.raises(RequirementsError, match="minimum_training_bars"):
        DataRequirements.parse(requirements)


def test_execution_replays_frozen_plan_and_rejects_stale_source_or_window():
    args = inputs()
    plan = plan_validation(**args, mode="scientific")
    immutable = {
        "dataset_bindings": args["dataset_bindings"],
        "instruments": args["instruments"],
        "window": args["approved_window"],
        "maximum_variants": args["variants"],
        "validation_requirements": {
            key: args[key]
            for key in (
                "requirements",
                "coverage",
                "maximum_history_days",
                "maximum_bar_variant_work",
            )
        },
    }
    assignment = {
        "base_ref": args["source_commit"],
        "validation_plan": plan,
        "window_start": plan["partitions"]["training"]["start"],
        "window_end": plan["partitions"]["target_tail"]["end"],
    }
    assert verify_execution_plan(assignment, immutable) == plan
    from scripts.run_alpha_research_assignment import BridgeError, execution_scope

    qualification = {
        "window": immutable["window"],
        "artifact_bundle": {
            "engine_hypothesis_yaml": {"immutable_contract": immutable},
            "validation_plan": plan,
        },
    }
    assert (
        execution_scope(assignment, qualification)["validation_plan_digest"]
        == plan["plan_digest"]
    )
    unreviewed = deepcopy(qualification)
    del unreviewed["artifact_bundle"]["validation_plan"]
    with pytest.raises(BridgeError, match="not bound into reviewed artifacts"):
        execution_scope(assignment, unreviewed)
    with pytest.raises(RequirementsError, match="missing, stale or infeasible"):
        verify_execution_plan(assignment | {"base_ref": "f" * 40}, immutable)
    with pytest.raises(RequirementsError, match="window differs"):
        verify_execution_plan(
            assignment | {"window_start": "2025-01-01T00:00:00Z"}, immutable
        )
    assert verify_execution_plan({}, {}) is None


def test_event_support_remains_a_post_run_gate():
    plan = plan_validation(**inputs(), mode="scientific")
    report = assess_event_support(
        plan, {"positive_direction": 50, "negative_direction": 12}
    )
    assert report["status"] == "insufficient_support"
    assert report["shortfalls"] == {"negative_direction": 28}
    report = assess_event_support(
        plan, {"positive_direction": 50, "negative_direction": 40}
    )
    assert report["status"] == "support_met"
    assert report["scientific_certification"] is False


def test_scaffolder_stops_before_authoring_when_requirements_absent(
    tmp_path, monkeypatch
):
    import json
    import sys

    import scripts.scaffold_alpha_strategy as script

    evidence = tmp_path / "evidence.json"
    output = tmp_path / "result.json"
    evidence.write_text("{}")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "scaffold",
            "--evidence",
            str(evidence),
            "--repository",
            str(tmp_path),
            "--output",
            str(output),
        ],
    )
    monkeypatch.setattr(script.subprocess, "check_output", lambda *a, **k: "a" * 40)

    def forbidden(*a, **k):
        pytest.fail("scaffolding must not happen with missing data requirements")

    monkeypatch.setattr(script, "scaffold_strategy_package", forbidden)
    assert script.main() == 2
    result = json.loads(output.read_text())
    assert result["success"] is False and result["retryable"] is False
    assert "data_requirements_missing" in result["reason"]


def test_preflight_runs_without_scientific_python_dependencies(tmp_path):
    import json
    import subprocess
    import sys
    from pathlib import Path

    args = inputs(days=31)
    evidence = {
        "validation_requirements": {
            key: args[key]
            for key in (
                "requirements",
                "coverage",
                "maximum_history_days",
                "maximum_bar_variant_work",
            )
        },
        "discovery_candidate": {
            "data": {"research_timeframe": "20m", "instruments": args["instruments"]},
            "parameter_budget": {"maximum_variants": args["variants"]},
        },
        "dataset_bindings": args["dataset_bindings"],
        "window": args["approved_window"],
    }
    path = tmp_path / "evidence.json"
    path.write_text(json.dumps(evidence))
    source = Path(__file__).resolve().parents[2] / "src"
    program = (
        "import sys,json; sys.path.insert(0,sys.argv[1]); "
        "from bt.governance.data_requirements import preflight_evidence; "
        "result=preflight_evidence(json.load(open(sys.argv[2])),source_commit='a'*40); "
        "assert result['smoke']['status']=='ready'; "
        "assert result['scientific']['status']=='deferred'"
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", program, str(source), str(path)],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
