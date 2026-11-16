from copy import deepcopy
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from bt.governance.data_requirements import RequirementsError
from bt.governance.event_support_planning import assess_event_support
from bt.governance.requirements_feasibility import plan_requirements
from tests.governance.test_requirements_feasibility import evidence, recovery_evidence


def assess(rows=1460, partition="training", q=.95, required=80, tail="upper"):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return assess_event_support({"quantile_support_plan": [{
        "gate": "events", "partition": partition, "quantile": q,
        "threshold_policy": f"fixed_training_linear_strict_{tail}_tail",
    }]}, {"minimum_event_support": {"events": required}, "research_bar_minutes": 240},
        {"partitions": {partition: {"start": start.isoformat(), "end": (start + timedelta(hours=4 * rows)).isoformat()}}})


def test_r10_training_bound_is_not_an_observed_count():
    result = assess()
    item = result["assessments"][0]
    assert item["fit_population_upper_bound"] == 73
    assert item["nominal_events_before_exclusions"] == 73
    assert result["blockers"]
    assert result["observed_event_support"] is result["statistical_power_established"] is False


@pytest.mark.parametrize("rows", [1, 2, 20, 365, 1460, 1600])
@pytest.mark.parametrize("q", [.01, .025, .05, .5, .9, .95, .99])
@pytest.mark.parametrize("tail", ["upper", "lower"])
def test_hard_bound_covers_linear_quantiles_and_ties(rows, q, tail):
    cap = assess(rows=rows, q=q, tail=tail)["assessments"][0]["fit_population_upper_bound"]
    for values in (np.arange(rows), np.arange(rows) // 3, np.ones(rows)):
        threshold = np.quantile(values, q, method="linear")
        count = int(((values < threshold) if tail == "lower" else (values > threshold)).sum())
        assert count <= cap


def test_lower_tail_nominal_uses_q_not_one_minus_q():
    result = assess(rows=2000, partition="test", q=.025, required=40, tail="lower")
    item = result["assessments"][0]
    assert item["nominal_events_before_exclusions"] == 50
    assert item["fit_population_upper_bound"] is None
    assert result["blockers"] == []
    training = assess(rows=730, q=.025, required=40, tail="lower")
    assert training["assessments"][0]["fit_population_upper_bound"] == 19
    assert training["blockers"]


def test_heldout_nominal_shortfall_is_not_an_impossibility_claim():
    result = assess(rows=365, partition="test", required=40)
    assert result["blockers"] == []
    item = result["assessments"][0]
    assert item["fit_population_upper_bound"] is None
    assert item["nominal_events_before_exclusions"] == 18.25
    assert item["nominal_shortfall_requires_review"]


def test_missing_design_makes_no_support_claim():
    assert assess_event_support({}, {}, {})["scope"] == "not_declared_no_capacity_claim"
    with pytest.raises(RequirementsError):
        assess_event_support({"quantile_support_plan": [{"gate": "unknown"}]}, {}, {})


@pytest.mark.parametrize("recovery", [False, True])
def test_native_preflight_defers_before_paid_review_even_with_coverage_successor(recovery):
    value = recovery_evidence() if recovery else evidence()
    value["discovery_candidate"]["scientific_specification"] = {"quantile_support_plan": [{
        "gate": "positive_direction", "partition": "training", "quantile": .999999,
        "threshold_policy": "fixed_training_linear_strict_upper_tail",
    }]}
    before = deepcopy(value)
    result = plan_requirements(value, source_commit="a" * 40)
    assert result["status"] == "deferred"
    assert result["event_support_planning"]["blockers"]
    assert any(x.startswith("event_support:") for x in result["reasons"])
    assert value == before
