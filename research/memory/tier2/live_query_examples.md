# Live Query Examples

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
