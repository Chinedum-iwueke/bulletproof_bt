from copy import deepcopy

import numpy as np
import pandas as pd

from bt.experiments.adaptive_representation import _apply_transform, feature_history
from bt.governance.requirements_feasibility import plan_requirements
from tests.test_adaptive_representation import _plan
from tests.governance.test_data_requirements import inputs


def test_composed_history_matches_native_first_valid_bar():
    plan = _plan()
    plan["transformations"] = [plan["transformations"][0], {
        "output_field": "volatility", "operation": "realized_volatility",
        "input_fields": ["btc_log_return"], "parameters": {"window": 6, "lag": 6},
        "fit_policy": "stateless", "rationale": "Completed six-bar volatility lagged six bars.",
    }]
    history = feature_history(plan)
    frame = pd.DataFrame({"BTCUSDT__close": 100 + np.arange(50, dtype=float) ** 1.2})
    for transform in plan["transformations"]:
        output = transform["output_field"]
        frame[output] = _apply_transform(frame, transform)
        assert frame[output].first_valid_index() == history[output]
    assert history["volatility"] == 12


def evidence(days=370):
    args = inputs(days=days, instruments=("BTCUSDT", "DOGEUSDT"), minutes=15)
    plan = _plan()
    return {
        "representation_plan": plan,
        "validation_requirements": {key: args[key] for key in ("requirements", "coverage", "maximum_history_days", "maximum_bar_variant_work")},
        "discovery_candidate": {"data": {"research_timeframe": "15m", "instruments": args["instruments"]},
                                "parameter_budget": {"maximum_variants": args["variants"]}},
        "dataset_bindings": args["dataset_bindings"], "window": args["approved_window"],
    }


def test_deterministic_ready_plans_and_no_false_scientific_authority():
    value = evidence()
    before = deepcopy(value)
    result = plan_requirements(value, source_commit="a" * 40)
    assert result["status"] == "ready"
    assert result == plan_requirements(value, source_commit="a" * 40)
    assert value == before
    assert set(result["validation_plans"]) == {"smoke", "scientific"}
    assert result["execution_authority"] is result["scientific_certification"] is False
    assert result["validation_plans"]["scientific"]["event_support"]["status"] == "not_yet_observed"


def test_short_clock_and_insufficient_history_defer_without_changing_minima():
    value = evidence(days=31)
    value["validation_requirements"]["requirements"]["feature_warmup_bars"] = 0
    before = deepcopy(value)
    result = plan_requirements(value, source_commit="a" * 40)
    assert result["status"] == "deferred"
    assert "composed_feature_history_exceeds_declared_warmup" in result["reasons"]
    assert "scientific:insufficient_contiguous_eligible_basket_history" in result["reasons"]
    assert result["validation_plans"]["smoke"]["status"] == "ready"
    assert value == before
