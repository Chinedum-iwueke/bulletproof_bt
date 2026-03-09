from __future__ import annotations

from bt.hypotheses.contract import HypothesisContract


def test_yaml_load_and_schema_validation() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h2.yaml")
    assert contract.schema.metadata.hypothesis_id == "L1-H2"
    assert "session_vwap" in contract.required_indicators()
