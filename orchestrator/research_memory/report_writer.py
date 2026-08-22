from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


def write_reports(conn: sqlite3.Connection, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = _summary(conn)
    positive = _df(conn, "SELECT * FROM research_memory_state_buckets WHERE ev_r_net > 0 ORDER BY ev_r_net DESC, n_trades DESC LIMIT 100")
    avoid = _df(conn, "SELECT * FROM research_memory_state_buckets WHERE ev_r_net < 0 OR avg_cost_drag_r >= 0.5 ORDER BY ev_r_net ASC, avg_cost_drag_r DESC LIMIT 100")
    profiles = _df(conn, """
        SELECT setup_class, COUNT(*) AS n_trades, AVG(r_net) AS ev_r_net, AVG(mfe_r) AS avg_mfe_r,
               AVG(exit_efficiency) AS avg_exit_efficiency, AVG(cost_drag_r) AS avg_cost_drag_r,
               SUM(CASE WHEN r_net >= 5 THEN 1 ELSE 0 END) AS tail_5r_count
        FROM research_memory_trades
        WHERE metrics_valid = 1 AND r_net IS NOT NULL
        GROUP BY setup_class
        ORDER BY ev_r_net DESC
    """)
    recs = _df(conn, "SELECT * FROM research_memory_recommendations ORDER BY evidence_score DESC, confidence DESC")

    positive.to_csv(output_dir / "top_positive_states.csv", index=False)
    avoid.to_csv(output_dir / "top_avoid_states.csv", index=False)
    profiles.to_csv(output_dir / "setup_profiles.csv", index=False)
    recs.to_csv(output_dir / "recommendations.csv", index=False)
    recommendation_records = recs.astype(object).where(pd.notna(recs), None).to_dict("records")
    (output_dir / "recommendations.json").write_text(
        json.dumps(recommendation_records, indent=2, default=_json_default),
        encoding="utf-8",
    )
    (output_dir / "research_memory_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "research_memory_report.md").write_text(_markdown(summary, positive, avoid, profiles, recs), encoding="utf-8")
    (output_dir / "live_query_examples.md").write_text(_examples(), encoding="utf-8")
    return summary


def _json_default(value: object) -> object:
    if hasattr(value, "item"):
        return value.item()  # type: ignore[union-attr]
    if hasattr(value, "isoformat"):
        return value.isoformat()  # type: ignore[union-attr]
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _summary(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM research_memory_trades) AS trades,
          (SELECT COUNT(*) FROM research_memory_trades WHERE metrics_valid = 0) AS invalid_trades,
          (SELECT COUNT(*) FROM research_memory_state_buckets) AS state_buckets,
          (SELECT COUNT(*) FROM research_memory_candidates) AS candidates,
          (SELECT COUNT(*) FROM research_memory_recommendations WHERE status = 'PROPOSED') AS proposed_recommendations
        """
    ).fetchone()
    return dict(row) if hasattr(row, "keys") else {
        "trades": row[0], "invalid_trades": row[1], "state_buckets": row[2], "candidates": row[3], "proposed_recommendations": row[4]
    }


def _markdown(summary: dict[str, Any], positive: pd.DataFrame, avoid: pd.DataFrame, profiles: pd.DataFrame, recs: pd.DataFrame) -> str:
    def lines_for(df: pd.DataFrame, cols: list[str], limit: int = 10) -> str:
        if df.empty:
            return "_No evidence yet._"
        out = []
        for row in df.head(limit).to_dict("records"):
            out.append("- " + ", ".join(f"{c}={row.get(c)}" for c in cols if c in row))
        return "\n".join(out)

    add_gates = recs[recs["recommendation_type"] == "ADD_GATE"] if not recs.empty else pd.DataFrame()
    sizing = recs[recs["recommendation_type"] == "CHANGE_SIZING"] if not recs.empty else pd.DataFrame()
    exits = recs[recs["recommendation_type"] == "REFINE_EXIT"] if not recs.empty else pd.DataFrame()
    tier3 = recs[recs["recommendation_type"] == "PROMOTE_TIER3"] if not recs.empty else pd.DataFrame()
    weak = profiles[profiles["ev_r_net"] < 0] if not profiles.empty else pd.DataFrame()
    strong = profiles[profiles["ev_r_net"] > 0] if not profiles.empty else pd.DataFrame()

    return f"""# Research Memory Report

## Executive Summary

- Trades in memory: {summary.get('trades', 0)}
- Under-instrumented or invalid trades recorded but excluded from recommendations: {summary.get('invalid_trades', 0)}
- State buckets: {summary.get('state_buckets', 0)}
- Alpha candidates/verdict records: {summary.get('candidates', 0)}
- Proposed recommendations requiring human approval: {summary.get('proposed_recommendations', 0)}

## What States Currently Show Edge

{lines_for(positive, ['state_key', 'bucket', 'setup_class', 'n_trades', 'ev_r_net', 'avg_cost_drag_r'])}

## What States Should Be Avoided

{lines_for(avoid, ['state_key', 'bucket', 'setup_class', 'n_trades', 'ev_r_net', 'avg_cost_drag_r'])}

## Best Setup Classes

{lines_for(strong, ['setup_class', 'n_trades', 'ev_r_net', 'avg_mfe_r', 'avg_exit_efficiency'])}

## Weak / Rejected Setup Classes

{lines_for(weak, ['setup_class', 'n_trades', 'ev_r_net', 'avg_mfe_r', 'tail_5r_count'])}

## Cost Fragility Map

{lines_for(profiles.sort_values('avg_cost_drag_r', ascending=False) if not profiles.empty else profiles, ['setup_class', 'n_trades', 'ev_r_net', 'avg_cost_drag_r'])}

## Exit Failure Map

{lines_for(exits, ['recommendation_type', 'setup_class', 'recommendation', 'evidence_score', 'confidence'])}

## Alpha Candidates Worth Tier3 Review

{lines_for(tier3, ['target_id', 'hypothesis_name', 'recommendation', 'evidence_score', 'confidence'])}

## Recommended Gates

{lines_for(add_gates, ['setup_class', 'recommendation', 'evidence_score', 'confidence'])}

## Recommended Sizing Adjustments

{lines_for(sizing, ['setup_class', 'recommendation', 'evidence_score', 'confidence'])}

## Recommended Exit Refinements

{lines_for(exits, ['setup_class', 'recommendation', 'evidence_score', 'confidence'])}

## Research Gaps

- Treat thin samples as research prompts, not approval signals.
- Re-run memory after new Tier2/Tier3 batches or daily scheduled research windows.
- Inspect invalid metrics runs before using their evidence.

## Next Tests To Queue Manually

{lines_for(recs[recs['recommendation_type'].isin(['TEST_NEXT', 'REFINE_EXIT', 'ADD_GATE', 'CHANGE_SIZING'])] if not recs.empty else recs, ['recommendation_type', 'setup_class', 'recommendation'])}
"""


def _examples() -> str:
    return """# Live Query Examples

Similar-state query:

```bash
python orchestrator/research_memory.py --db research_db/research.sqlite --query similar_state --state-json current_state.json
```

Example state snapshot:

```json
{
  "setup_class": "trend_pullback",
  "csi_pctile": 0.82,
  "vol_pctile": 0.76,
  "spread_pctile": 0.48,
  "tr_over_atr": 1.9
}
```

Research Memory is evidence-only. It does not deploy strategies, approve hypotheses, queue tests, or trade live.
"""


def _df(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)
