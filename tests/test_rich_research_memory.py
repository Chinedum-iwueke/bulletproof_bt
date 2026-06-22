from __future__ import annotations

import sqlite3

from orchestrator.research_memory.query_engine import similar_state
from orchestrator.research_memory.schema import ensure_research_memory_schema
from orchestrator.research_memory.trade_memory import insert_trades, normalize_trade


def test_research_memory_ingests_and_queries_rich_state_fields() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_research_memory_schema(conn)
    rec = normalize_trade(
        {
                "trade_id": "t1",
                "symbol": "BTCUSDT",
                "identity_ts_signal": "2025-01-01T00:00:00Z",
                "net_pnl": 50.0,
                "r_net": 0.5,
            "entry_state_csi_pctile": 0.82,
            "entry_state_funding_pctile": 0.91,
            "entry_state_oi_accel_pctile": 0.88,
            "entry_state_basis_pctile": 0.79,
            "entry_state_csi_source": "enriched",
            "entry_state_csi_components_json": "{\"funding_extreme_score\":0.82}",
        },
        context={"experiment_root": "exp", "run_id": "run1", "metrics_valid": True},
        row_index=0,
    )
    insert_trades(conn, [rec])

    row = conn.execute("SELECT funding_pctile, oi_accel_pctile, basis_pctile, csi_source FROM research_memory_trades").fetchone()
    assert row["funding_pctile"] == 0.91
    assert row["oi_accel_pctile"] == 0.88
    assert row["basis_pctile"] == 0.79
    assert row["csi_source"] == "enriched"

    result = similar_state(conn, {"csi_pctile": 0.8, "funding_pctile": 0.9, "oi_accel_pctile": 0.9, "basis_pctile": 0.8})
    assert result["n_matches"] == 1
