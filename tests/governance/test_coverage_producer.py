from copy import deepcopy
from hashlib import sha256

import pandas as pd
import pytest

from bt.governance.coverage_producer import produce_coverage
from bt.governance.data_requirements import fingerprint


def fixture(tmp_path):
    symbols = ["BTCUSDT", "SOLUSDT"]
    plan = {
        "schema_version": "adaptive-representation-plan-v1.0.0",
        "candidate_key": "synthetic-coverage-test",
        "instruments": symbols,
        "basket_members": [
            {
                "instrument": s,
                "role": "primary" if i == 0 else "predictor",
                "legacy_groups": [],
                "selection_rationale": "Synthetic test basket only",
            }
            for i, s in enumerate(symbols)
        ],
        "source_timeframe": "1m",
        "research_timeframe": "5m",
        "resampling_policy": "left_closed_left_labeled_complete_bars",
        "transformations": [
            {
                "output_field": "price",
                "operation": "identity",
                "input_fields": ["BTCUSDT__close"],
                "parameters": {},
                "fit_policy": "stateless",
                "rationale": "Synthetic availability test only",
            }
        ],
        "transformation_rationale": "Synthetic clock verification, not research evidence",
        "rejected_alternatives": ["No outcome-driven selection"],
        "selection_data_boundary": "metadata_predictors_only_no_targets",
        "outcome_data_consulted": False,
    }
    receipts = []
    for symbol in symbols:
        frame = pd.DataFrame(
            {
                "ts": pd.date_range("2025-01-01", periods=30, freq="min", tz="UTC"),
                "symbol": symbol,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.0,
                "volume": 100.0,
            }
        )
        path = tmp_path / f"{symbol}.parquet"
        frame.to_parquet(path)
        digest = sha256(path.read_bytes()).hexdigest()
        receipts.append(
            {
                "result": {
                    "instrument": symbol,
                    "admitted": True,
                    "checks": {"fixture": True},
                    "panel_sha256": digest,
                    "recovery_copy": {
                        "uri": path.as_uri(),
                        "content_digest": digest,
                        "integrity_verified": True,
                    },
                }
            }
        )
    return dict(
        plan=plan,
        receipts=receipts,
        window={"start": "2025-01-01T00:00:00Z", "end": "2025-01-01T00:30:00Z"},
        required_fields={s: ["close"] for s in symbols},
    )


def test_predictor_clock_does_not_claim_membership_or_authority(tmp_path):
    args = fixture(tmp_path)
    before = deepcopy(args)
    result = produce_coverage(**args)
    assert args == before
    assert result["eligible_decisions_count"] == 5
    assert result["intervals"] == [
        {"start": "2025-01-01T00:05:00+00:00", "end": "2025-01-01T00:30:00+00:00"}
    ]
    assert result["point_in_time_membership_verified"] is False
    assert result["capital_or_order_authority"] is False
    assert result["evidence_digest"] == fingerprint(
        {k: v for k, v in result.items() if k != "evidence_digest"}
    )


@pytest.mark.parametrize("chunk", [5, 7, 10, 11, 30])
def test_chunked_coverage_equals_single_read_including_digest(tmp_path, chunk):
    args = fixture(tmp_path)
    args["plan"]["transformations"][0] = {
        "output_field": "lagged",
        "operation": "log_return",
        "input_fields": ["BTCUSDT__close"],
        "parameters": {"periods": 2},
        "fit_policy": "stateless",
        "rationale": "Retain lag state across chunk boundaries without future fill",
    }
    assert produce_coverage(**args, chunk_source_rows=chunk) == produce_coverage(**args)


def test_research_memory_budget_checked_before_read(tmp_path, monkeypatch):
    args = fixture(tmp_path)
    monkeypatch.setattr(
        pd, "read_parquet", lambda *a, **kw: pytest.fail("must reject before reading")
    )
    with pytest.raises(ValueError, match="research-row"):
        produce_coverage(**args, maximum_research_rows=1)


def test_empty_and_partial_chunks_do_not_invalidate_older_usable_history(tmp_path):
    args = fixture(tmp_path)
    args["window"] = {"start": "2024-12-31T23:45:00Z", "end": "2025-01-01T00:32:00Z"}
    assert produce_coverage(**args, chunk_source_rows=10) == produce_coverage(**args)


def test_changed_bytes_rejected_before_materialization(tmp_path):
    args = fixture(tmp_path)
    (tmp_path / "BTCUSDT.parquet").write_bytes(b"changed")
    with pytest.raises(ValueError, match="bytes differ"):
        produce_coverage(**args)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1.0])
def test_invalid_source_price_removes_entire_bar_from_basket_clock(tmp_path, bad):
    args = fixture(tmp_path)
    path = tmp_path / "SOLUSDT.parquet"
    frame = pd.read_parquet(path)
    frame.loc[12, "close"] = bad
    frame.to_parquet(path)
    digest = sha256(path.read_bytes()).hexdigest()
    args["receipts"][1]["result"]["panel_sha256"] = digest
    args["receipts"][1]["result"]["recovery_copy"]["content_digest"] = digest
    result = produce_coverage(**args)
    assert result["eligible_decisions_count"] == 4
    assert len(result["intervals"]) == 2


def test_non_ohlcv_availability_cannot_be_assumed(tmp_path):
    args = fixture(tmp_path)
    args["required_fields"]["BTCUSDT"].append("funding_rate")
    with pytest.raises(ValueError, match="field-specific"):
        produce_coverage(**args)


def test_failed_admission_cannot_produce_clock(tmp_path):
    args = fixture(tmp_path)
    args["receipts"][0]["result"]["admitted"] = False
    with pytest.raises(ValueError, match="successful native admission"):
        produce_coverage(**args)


def test_resource_budget_checked_before_reading_panels(tmp_path):
    args = fixture(tmp_path)
    (tmp_path / "BTCUSDT.parquet").unlink()
    with pytest.raises(ValueError, match="resource budget exceeded"):
        produce_coverage(**args, maximum_source_rows=1)
