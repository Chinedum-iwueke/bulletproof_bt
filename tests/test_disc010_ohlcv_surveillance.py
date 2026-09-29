from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bt.institutional.ohlcv_surveillance import (
    OhlcvSurveillanceError,
    ohlcv_signal_surveillance_receipt,
)
from bt.institutional.receipt import verify_receipt


COMMIT = "a" * 40
DATASET = "b" * 64


def panel(symbol: str, close: np.ndarray, start: str = "2025-01-01") -> pd.DataFrame:
    timestamps = pd.date_range(start, periods=len(close), freq="1min", tz="UTC")
    values = np.asarray(close, dtype=float)
    return pd.DataFrame(
        {
            "ts": timestamps,
            "symbol": symbol,
            "open": values,
            "high": values * 1.0001,
            "low": values * 0.9999,
            "close": values,
            "volume": np.linspace(100.0, 200.0, len(values)),
        }
    )


def causal_panels(rows: int = 900) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(7)
    sol_returns = rng.normal(0.0, 0.001, rows)
    sol_returns[::5] *= 8
    sol = 100 * np.exp(np.cumsum(sol_returns))
    eth_returns = np.zeros(rows)
    eth_returns[1:] = 0.85 * sol_returns[:-1] + rng.normal(0.0, 0.00005, rows - 1)
    eth = 2_000 * np.exp(np.cumsum(eth_returns))
    return {"SOLUSDT": panel("SOLUSDT", sol), "ETHUSDT": panel("ETHUSDT", eth)}


def trial(**overrides) -> dict:
    value = {
        "trial_id": "sol-leads-eth",
        "predictor_instrument": "SOLUSDT",
        "target_instrument": "ETHUSDT",
        "predictor": "log_return",
        "lookback_bars": 1,
        "target_horizon_bars": 1,
        "tail_quantile": 0.8,
        "relation": "same_direction",
        "minimum_support": 20,
        "minimum_effect": 0.00001,
        "parameters": {},
    }
    value.update(overrides)
    return value


def specification(*trials: dict, research_timeframe: str = "1m") -> dict:
    return {
        "schema_version": "disc010-signal-screen-spec-v1.0.0",
        "family_id": "cross-asset-lead-lag-v1",
        "instruments": ["SOLUSDT", "ETHUSDT"],
        "source_timeframe": "1m",
        "research_timeframe": research_timeframe,
        "selection_data_boundary": "metadata_predictors_only_no_targets",
        "outcome_data_consulted_during_selection": False,
        "exploration_start": "2025-01-01T00:01:00Z",
        "exploration_end": "2025-01-01T06:00:00Z",
        "validation_end": "2025-01-01T12:00:00Z",
        "sealed_oos_start": "2025-01-01T13:20:00Z",
        "alpha": 0.05,
        "permutation_count": 99,
        "seed": 19,
        "trials": list(trials or (trial(),)),
    }


def produce(spec: dict | None = None, panels=None, *, max_workers: int = 1):
    return ohlcv_signal_surveillance_receipt(
        specification=spec or specification(),
        panels=panels or causal_panels(),
        dataset_digest=DATASET,
        source_commit=COMMIT,
        max_workers=max_workers,
    )


def test_cross_asset_signal_is_screened_without_promotion_authority():
    receipt = produce()

    assert receipt.milestone == "DISC-010"
    assert verify_receipt(receipt)
    assert receipt.result["question_candidate_digests"]
    assert receipt.result["trials"][0]["question_candidate"] is True
    assert receipt.result["final_oos_opened"] is False
    assert receipt.result["final_oos_metrics"] == {}
    assert receipt.result["strategy_authority"] is False
    assert receipt.result["promotion_authority"] is False
    assert receipt.authority == {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    }


def test_null_family_is_retained_without_a_candidate():
    panels = causal_panels()
    panels["ETHUSDT"] = panel("ETHUSDT", np.full(900, 2_000.0))

    receipt = produce(panels=panels)

    assert receipt.result["question_candidate_digests"] == []
    assert receipt.result["trials"][0]["status"] == "evaluated"
    assert receipt.result["trials"][0]["validation_empirical_p_value"] == 1.0


def test_rows_at_or_after_sealed_oos_cannot_change_receipt():
    panels = causal_panels()
    baseline = produce(panels=panels)
    mutated = {name: frame.copy() for name, frame in panels.items()}
    cutoff = pd.Timestamp("2025-01-01T13:20:00Z")
    for frame in mutated.values():
        frame.loc[frame["ts"] >= cutoff, ["open", "high", "low", "close"]] *= 100

    replay = produce(panels=mutated)

    assert replay.receipt_digest == baseline.receipt_digest
    assert replay.result == baseline.result


def test_worker_parallelism_cannot_change_scientific_receipt():
    family = specification(
        trial(),
        trial(
            trial_id="eth-own-return",
            predictor_instrument="ETHUSDT",
            target_instrument="ETHUSDT",
            relation="opposite_direction",
        ),
    )

    serial = produce(family, max_workers=1)
    parallel = produce(family, max_workers=8)

    assert parallel.receipt_digest == serial.receipt_digest
    assert parallel.result == serial.result


def test_incomplete_higher_timeframe_buckets_are_excluded():
    panels = causal_panels()
    panels = {
        name: frame.loc[frame["ts"] != pd.Timestamp("2025-01-01T01:02:00Z")]
        for name, frame in panels.items()
    }
    spec = specification(
        trial(lookback_bars=2, target_horizon_bars=2, minimum_support=10),
        research_timeframe="5m",
    )

    receipt = produce(spec, panels)

    assert receipt.result["complete_rows"]["SOLUSDT"] == 159
    assert receipt.result["complete_rows"]["ETHUSDT"] == 159
    assert receipt.result["aligned_complete_rows"] == 159


def test_fractional_difference_is_bounded_and_population_drift_is_retained():
    spec = specification(
        trial(
            predictor="fractional_difference",
            parameters={"d": 0.35, "weight_threshold": 0.01},
            minimum_support=10,
        )
    )

    receipt = produce(spec)

    screened = receipt.result["trials"][0]
    assert screened["status"] == "invalid"
    assert screened["reason"] == "insufficient_tail_support"
    assert screened["question_candidate"] is False
    assert receipt.result["selection_data_boundary"] == "metadata_predictors_only_no_targets"


def test_semantic_duplicate_trials_are_rejected_before_outcomes():
    second = trial(trial_id="duplicate-name")

    with pytest.raises(OhlcvSurveillanceError, match="semantic duplicate"):
        produce(specification(trial(), second))


@pytest.mark.parametrize("workers", [0, 9, 1.5])
def test_execution_parallelism_is_bounded(workers):
    with pytest.raises(OhlcvSurveillanceError, match="execution workers"):
        produce(max_workers=workers)


@pytest.mark.parametrize(
    "change, message",
    [
        ({"outcome_data_consulted_during_selection": True}, "cannot select"),
        ({"source_timeframe": "5m"}, "one-minute"),
        ({"sealed_oos_start": "2025-01-01T11:00:00Z"}, "ordered"),
    ],
)
def test_causal_boundary_violations_fail_closed(change, message):
    spec = specification()
    spec.update(change)

    with pytest.raises(OhlcvSurveillanceError, match=message):
        produce(spec)
