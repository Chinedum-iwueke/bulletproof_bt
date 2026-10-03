from __future__ import annotations

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from bt.experiments.adaptive_representation import (
    AdaptiveRepresentationError,
    materialize_adaptive_representation,
    validate_plan,
)


def _panel(symbol: str, *, offset: float = 0.0, periods: int = 180) -> pd.DataFrame:
    ts = pd.date_range("2025-01-01T00:00:00Z", periods=periods, freq="1min")
    close = 100 + offset + np.arange(periods) * 0.01
    return pd.DataFrame(
        {
            "ts": ts,
            "symbol": symbol,
            "open": close - 0.01,
            "high": close + 0.02,
            "low": close - 0.02,
            "close": close,
            "volume": 1000.0,
            "quote_volume": close * 1000.0,
        }
    )


def _plan() -> dict:
    return {
        "schema_version": "adaptive-representation-plan-v1.0.0",
        "candidate_key": "cross-group-lead-lag",
        "instruments": ["BTCUSDT", "DOGEUSDT"],
        "basket_members": [
            {
                "instrument": "BTCUSDT",
                "role": "predictor",
                "legacy_groups": ["stable"],
                "selection_rationale": "Liquid market proxy tests information leadership.",
            },
            {
                "instrument": "DOGEUSDT",
                "role": "primary",
                "legacy_groups": ["volatile"],
                "selection_rationale": "Higher-beta target tests delayed transmission.",
            },
        ],
        "source_timeframe": "1m",
        "research_timeframe": "15m",
        "resampling_policy": "left_closed_left_labeled_complete_bars",
        "transformations": [
            {
                "output_field": "btc_log_return",
                "operation": "log_return",
                "input_fields": ["BTCUSDT__close"],
                "parameters": {"periods": 1},
                "fit_policy": "stateless",
                "rationale": "Returns remove the price-level scale for lead-lag comparison.",
            },
            {
                "output_field": "doge_fracdiff",
                "operation": "fractional_difference",
                "input_fields": ["DOGEUSDT__close"],
                "parameters": {"d": 0.4, "weight_threshold": 0.01},
                "fit_policy": "train_only",
                "rationale": "Retain memory while reducing persistent price-level behavior.",
            },
            {
                "output_field": "relative_return",
                "operation": "spread",
                "input_fields": ["doge_fracdiff", "btc_log_return"],
                "parameters": {},
                "fit_policy": "stateless",
                "rationale": "Measure target movement relative to the market proxy.",
            },
        ],
        "transformation_rationale": (
            "Use completed 15-minute bars to match the proposed transmission horizon."
        ),
        "rejected_alternatives": [
            "One-minute noise is too fine for the stated horizon.",
            "A stable-only basket cannot test cross-group transmission.",
        ],
        "selection_data_boundary": "metadata_predictors_only_no_targets",
        "outcome_data_consulted": False,
    }


def test_mixed_group_plan_materializes_complete_causal_bars_and_fractional_difference() -> None:
    result = materialize_adaptive_representation(
        _plan(),
        {"BTCUSDT": _panel("BTCUSDT"), "DOGEUSDT": _panel("DOGEUSDT", offset=2)},
    )
    assert result.receipt["basket"] == ["BTCUSDT", "DOGEUSDT"]
    assert result.receipt["aligned_rows"] == 12
    assert result.receipt["outcome_data_consulted"] is False
    assert result.receipt["future_fill_used"] is False
    assert result.frame["decision_at"].iloc[0] == pd.Timestamp("2025-01-01T00:15:00Z")
    assert result.frame["doge_fracdiff"].notna().any()
    assert len(result.receipt["receipt_digest"]) == 64


def test_missing_minute_drops_only_affected_complete_bucket_without_filling() -> None:
    baseline = materialize_adaptive_representation(
        _plan(),
        {"BTCUSDT": _panel("BTCUSDT"), "DOGEUSDT": _panel("DOGEUSDT", offset=2)},
    )
    doge = _panel("DOGEUSDT", offset=2).drop(index=20)
    missing = materialize_adaptive_representation(
        _plan(), {"BTCUSDT": _panel("BTCUSDT"), "DOGEUSDT": doge}
    )
    assert len(missing.frame) == len(baseline.frame) - 1
    assert pd.Timestamp("2025-01-01T00:30:00Z") not in set(missing.frame["decision_at"])


def test_future_append_does_not_revise_prior_materialized_rows() -> None:
    short = {
        "BTCUSDT": _panel("BTCUSDT", periods=120),
        "DOGEUSDT": _panel("DOGEUSDT", offset=2, periods=120),
    }
    long = {
        "BTCUSDT": _panel("BTCUSDT", periods=180),
        "DOGEUSDT": _panel("DOGEUSDT", offset=2, periods=180),
    }
    first = materialize_adaptive_representation(_plan(), short).frame
    second = materialize_adaptive_representation(_plan(), long).frame
    pd.testing.assert_frame_equal(first, second.iloc[: len(first)].reset_index(drop=True))


def test_rolling_transform_lag_is_explicit_and_causal() -> None:
    value = deepcopy(_plan())
    value["transformations"].insert(
        1,
        {
            "output_field": "prior_btc_volatility",
            "operation": "realized_volatility",
            "input_fields": ["btc_log_return"],
            "parameters": {"window": 3, "lag": 1},
            "fit_policy": "stateless",
            "rationale": "Exclude the decision bar from the prior-state control.",
        },
    )
    result = materialize_adaptive_representation(
        value,
        {"BTCUSDT": _panel("BTCUSDT"), "DOGEUSDT": _panel("DOGEUSDT", offset=2)},
    )
    expected = (
        result.frame["btc_log_return"]
        .rolling(3, min_periods=3)
        .std(ddof=0)
        .mul(np.sqrt(3))
        .shift(1)
    )
    pd.testing.assert_series_equal(
        result.frame["prior_btc_volatility"], expected, check_names=False
    )


def test_rolling_transform_rejects_negative_lag() -> None:
    value = deepcopy(_plan())
    value["transformations"].append(
        {
            "output_field": "future_leaking_volatility",
            "operation": "realized_volatility",
            "input_fields": ["btc_log_return"],
            "parameters": {"window": 3, "lag": -1},
            "fit_policy": "stateless",
            "rationale": "This deliberately invalid transform must be rejected.",
        }
    )
    with pytest.raises(AdaptiveRepresentationError, match="lag is outside"):
        validate_plan(value)


def test_cross_sectional_rank_retains_the_named_asset_rank() -> None:
    value = deepcopy(_plan())
    value["transformations"].extend(
        [
            {
                "output_field": "btc_identity",
                "operation": "identity",
                "input_fields": ["BTCUSDT__close"],
                "parameters": {},
                "fit_policy": "stateless",
                "rationale": "Retain the completed BTC close for cross-sectional ranking.",
            },
            {
                "output_field": "doge_identity",
                "operation": "identity",
                "input_fields": ["DOGEUSDT__close"],
                "parameters": {},
                "fit_policy": "stateless",
                "rationale": "Retain the completed DOGE close for cross-sectional ranking.",
            },
            {
                "output_field": "btc_cross_sectional_rank",
                "operation": "cross_sectional_rank",
                "input_fields": ["btc_identity", "doge_identity"],
                "parameters": {"target_index": 0},
                "fit_policy": "stateless",
                "rationale": "Retain BTC's asset-specific percentile rank at each decision.",
            },
            {
                "output_field": "doge_cross_sectional_rank",
                "operation": "cross_sectional_rank",
                "input_fields": ["btc_identity", "doge_identity"],
                "parameters": {"target_index": 1},
                "fit_policy": "stateless",
                "rationale": "Retain DOGE's asset-specific percentile rank at each decision.",
            },
        ]
    )
    result = materialize_adaptive_representation(
        value,
        {"BTCUSDT": _panel("BTCUSDT"), "DOGEUSDT": _panel("DOGEUSDT", offset=2)},
    ).frame
    assert set(result["btc_cross_sectional_rank"].dropna()) == {0.5}
    assert set(result["doge_cross_sectional_rank"].dropna()) == {1.0}


def test_cross_sectional_rank_rejects_ambiguous_or_out_of_range_target() -> None:
    value = deepcopy(_plan())
    value["transformations"].append(
        {
            "output_field": "ambiguous_rank",
            "operation": "cross_sectional_rank",
            "input_fields": ["BTCUSDT__close", "DOGEUSDT__close"],
            "parameters": {},
            "fit_policy": "stateless",
            "rationale": "An aggregate of all ranks cannot retain an asset identity.",
        }
    )
    with pytest.raises(AdaptiveRepresentationError, match="target_index"):
        validate_plan(value)
    value["transformations"][-1]["parameters"] = {"target_index": 2}
    with pytest.raises(AdaptiveRepresentationError, match="outside"):
        validate_plan(value)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda plan: plan.update(outcome_data_consulted=True), "outcome blind"),
        (
            lambda plan: plan["transformations"][1]["parameters"].update(d=0.7),
            "between 0 and 0.5",
        ),
        (
            lambda plan: plan["basket_members"].reverse(),
            "order must match",
        ),
        (
            lambda plan: plan["transformations"][0].update(
                input_fields=["ETHUSDT__close"]
            ),
            "unavailable inputs",
        ),
    ],
)
def test_plan_rejects_outcome_selection_unsafe_fractional_difference_and_unbound_assets(
    mutation, message
) -> None:
    plan = deepcopy(_plan())
    mutation(plan)
    with pytest.raises(AdaptiveRepresentationError, match=message):
        validate_plan(plan)
