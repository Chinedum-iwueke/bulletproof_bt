from copy import deepcopy

import numpy as np
import pandas as pd

from bt.institutional.adaptive_representation_certification import (
    adaptive_representation_certification_receipt,
)
from bt.institutional.receipt import digest, verify_receipt


def panel(symbol: str, offset: float = 0) -> pd.DataFrame:
    rows = 300
    close = 100 + offset + np.arange(rows) * 0.01
    return pd.DataFrame({
        "ts": pd.date_range("2025-01-01", periods=rows, freq="1min", tz="UTC"),
        "symbol": symbol,
        "open": close,
        "high": close + 0.1,
        "low": close - 0.1,
        "close": close,
        "volume": 1000.0,
        "quote_volume": close * 1000,
    })


def plan(key: str, timeframe: str, operations: list[dict]) -> dict:
    return {
        "schema_version": "adaptive-representation-plan-v1.0.0",
        "candidate_key": key,
        "instruments": ["BTCUSDT", "DOGEUSDT"],
        "basket_members": [
            {
                "instrument": "BTCUSDT",
                "role": "predictor",
                "legacy_groups": ["stable"],
                "selection_rationale": "Liquid market proxy supplies the leading state.",
            },
            {
                "instrument": "DOGEUSDT",
                "role": "primary",
                "legacy_groups": ["volatile"],
                "selection_rationale": "Higher beta target tests delayed transmission.",
            },
        ],
        "source_timeframe": "1m",
        "research_timeframe": timeframe,
        "resampling_policy": "left_closed_left_labeled_complete_bars",
        "transformations": operations,
        "transformation_rationale": "The clock and transforms follow the mechanism.",
        "rejected_alternatives": ["Raw one-minute levels do not match the mechanism."],
        "selection_data_boundary": "metadata_predictors_only_no_targets",
        "outcome_data_consulted": False,
    }


def operation(output: str, kind: str, inputs: list[str], parameters: dict, fit: str):
    return {
        "output_field": output,
        "operation": kind,
        "input_fields": inputs,
        "parameters": parameters,
        "fit_policy": fit,
        "rationale": "This is required by the frozen causal representation mechanism.",
    }


def certification_case(value: dict, terminal=None) -> dict:
    fields = [item["output_field"] for item in value["transformations"]]
    return {
        "case_key": value["candidate_key"],
        "plan": value,
        "panels": {"BTCUSDT": panel("BTCUSDT"), "DOGEUSDT": panel("DOGEUSDT", 2)},
        "dataset_digests": {"BTCUSDT": "1" * 64, "DOGEUSDT": "2" * 64},
        "strategy_execution_semantics": {
            "adaptive_representation_plan_digest": digest(value),
            "adaptive_representation_fields": fields,
            "required_extra_columns": fields,
        },
        "terminal_evidence": terminal,
    }


def test_adaptive_representation_suite_requires_reasoning_and_terminal_replay():
    first = plan("lead-lag", "15m", [
        operation("btc_return", "log_return", ["BTCUSDT__close"], {"periods": 1}, "stateless"),
        operation("doge_return", "log_return", ["DOGEUSDT__close"], {"periods": 1}, "stateless"),
        operation("relative_return", "spread", ["doge_return", "btc_return"], {}, "stateless"),
    ])
    second = plan("persistent-level", "30m", [
        operation("doge_memory", "fractional_difference", ["DOGEUSDT__close"], {"d": 0.4, "weight_threshold": 0.01}, "train_only"),
    ])
    third = plan("volatility-state", "1h", [
        operation("btc_return", "log_return", ["BTCUSDT__close"], {"periods": 1}, "stateless"),
        operation("btc_volatility", "realized_volatility", ["btc_return"], {"window": 3}, "stateless"),
    ])
    terminal = {
        "bt009_receipt_digest": "3" * 64,
        "bundle_digest": "4" * 64,
        "outcome": "negative",
        "instrument_count": 2,
    }
    receipt = adaptive_representation_certification_receipt(
        cases=[
            certification_case(first, terminal),
            certification_case(second),
            certification_case(third),
        ],
        source_commit="a" * 40,
    )
    assert verify_receipt(receipt)
    assert receipt.result["status"] == "qualified"
    assert all(receipt.result["capability_checks"].values())
    assert receipt.result["capital_or_order_authority"] is False


def test_suite_without_terminal_multi_asset_evidence_fails_honestly():
    base = plan("lead-lag", "15m", [
        operation("btc_return", "log_return", ["BTCUSDT__close"], {"periods": 1}, "stateless"),
        operation("doge_return", "log_return", ["DOGEUSDT__close"], {"periods": 1}, "stateless"),
        operation("relative_return", "spread", ["doge_return", "btc_return"], {}, "stateless"),
        operation("memory", "fractional_difference", ["DOGEUSDT__close"], {"d": 0.4, "weight_threshold": 0.01}, "train_only"),
        operation("vol", "realized_volatility", ["doge_return"], {"window": 3}, "stateless"),
    ])
    cases = []
    for index, timeframe in enumerate(("5m", "15m", "30m"), start=1):
        value = deepcopy(base)
        value["candidate_key"] = f"case-{index}"
        value["research_timeframe"] = timeframe
        cases.append(certification_case(value))
    receipt = adaptive_representation_certification_receipt(
        cases=cases, source_commit="a" * 40
    )
    assert receipt.result["status"] == "not_qualified"
    assert receipt.result["capability_checks"][
        "terminal_multi_asset_bt009_outcome_retained"
    ] is False
