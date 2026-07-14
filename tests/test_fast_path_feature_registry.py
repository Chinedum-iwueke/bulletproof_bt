from __future__ import annotations

import pandas as pd

from bt.engine.fast_path.candidate_events import (
    ColumnarCandidateEventPlan,
    candidate_columns,
    candidate_event_reasons,
)
from bt.engine.fast_path.data_session import DataSession
from bt.engine.fast_path.family_kernels import adapter_for_strategy
from bt.engine.fast_path.feature_registry import GLOBAL_FEATURE_REGISTRY


def _write_panel(root, symbol: str) -> None:
    ts = pd.date_range(pd.Timestamp("2024-01-01T00:00:00Z"), periods=4, freq="1min")
    frame = pd.DataFrame(
        {
            "ts": ts,
            "exchange": "binance",
            "symbol": symbol,
            "open": [1.0, 2.0, 3.0, 4.0],
            "high": [1.1, 2.1, 3.1, 4.1],
            "low": [0.9, 1.9, 2.9, 3.9],
            "close": [1.0, 2.0, 3.0, 4.0],
            "volume": [10.0, 11.0, 12.0, 13.0],
            "funding_rate": [0.0, 0.0, 0.0, 0.0],
            "open_interest": [100.0, 101.0, 102.0, 103.0],
            "basis_close_vs_index": [0.0, 0.01, 0.02, 0.03],
            "htf_15m_ready": [False, True, False, False],
            "htf_15m_close": [None, 2.0, None, None],
            "generic_entry_candidate": [False, False, True, False],
        }
    )
    path = root / "canonical" / "binance" / symbol / "timeframe=1m" / "research_panel.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_feature_registry_declares_causal_contracts() -> None:
    spec = GLOBAL_FEATURE_REGISTRY.get("htf_context")

    assert spec.required_inputs == ("ts", "symbol", "open", "high", "low", "close", "volume")
    assert spec.causality.past_only is True
    assert spec.causality.mode == "strict_closed_htf"
    assert len(spec.feature_hash) == 64


def test_data_session_exposes_feature_bank_and_generic_candidate_plan(tmp_path) -> None:
    root = tmp_path / "research_data"
    _write_panel(root, "BTCUSDT")
    manifest = pd.DataFrame({"exchange": ["binance"], "native_symbol": ["BTCUSDT"], "available": [True]})
    manifest_path = root / "manifests" / "stable_universe.parquet"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_parquet(manifest_path, index=False)

    session = DataSession.from_config(
        {
            "data": {
                "dataset_kind": "research_panel",
                "root": str(root),
                "exchange": "binance",
                "universe": "stable",
                "stable_manifest": str(manifest_path),
                "timeframe": "1m",
                "extra_column_prefixes": ["htf_15m_", "generic_"],
            }
        }
    )
    snapshot = session.snapshot()
    arrays = snapshot.arrays_for_symbol("BTCUSDT")

    assert "htf_context" in snapshot.feature_bank.feature_names()
    assert snapshot.feature_bank.readiness_mask("BTCUSDT", "htf_context").tolist() == [False, True, False, False]
    assert arrays.candidate_ready.tolist() == [False, True, True, False]
    assert "generic_entry_candidate" in candidate_columns(snapshot.feature_columns)
    plan = ColumnarCandidateEventPlan.from_snapshot(snapshot)
    assert plan.symbol_ids.tolist() == [0, 0, 0, 0]
    assert plan.candidate_mask.tolist() == [False, True, True, False]


def test_generic_candidate_event_reasons_include_declared_patterns() -> None:
    reasons = candidate_event_reasons(
        {
            "generic_entry_candidate": True,
            "htf_15m_ready": True,
            "l7h1_15m_compiled_feature_ready": False,
        }
    )

    assert any("entry_candidate:generic_entry_candidate" == reason for reason in reasons)
    assert any("htf_context" in reason and "htf_15m_ready" in reason for reason in reasons)


def test_strategy_adapter_declares_feature_requests_and_truth_layer() -> None:
    adapter = adapter_for_strategy("l1_h11_quality_filtered_continuation")

    assert adapter is not None
    assert adapter.truth_layer == "classic_engine"
    assert adapter.candidate_scheduler == "generic_sparse_events"
    assert [req.feature_name for req in adapter.feature_requests] == ["engine_state", "htf_context"]
