from __future__ import annotations

from bt.logging.decision_trace import flatten_decision_trace
from bt.logging.trade_enrichment import enrich_trade_row


def test_trade_schema_prefix_enrichment_and_flattening() -> None:
    row = {
        "identity_run_id": "run_1",
        "entry_state_vol_pctile": 0.8,
        "execution_actual_entry_price": 100.0,
        "path_mfe_r": 3.2,
        "path_mae_r": 0.4,
        "r_net": 1.5,
        "r_gross": 1.8,
    }
    enriched = enrich_trade_row(row)
    assert "counterfactual_exit_efficiency_realized_over_mfe" in enriched
    assert enriched["label_reached_3r"] is True
    assert enriched["label_reached_5r"] is False
    assert enriched["label_profitable_after_costs"] is True
    assert enriched["label_exit_efficiency_bucket"] in {"excellent", "decent", "poor", "failed"}

    flat = flatten_decision_trace({"reason_code": "x", "setup_class": "setup", "gate_values": {"csi": 0.9}})
    assert flat["entry_decision_reason_code"] == "x"
    assert "entry_gate_values_json" in flat
