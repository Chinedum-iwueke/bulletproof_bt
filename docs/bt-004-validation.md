# BT-004 Validation

**Date:** 2026-08-24
**Scope:** classic event-driven causality, accounting, deterministic state and replay

## Certified contract

BT-004 retains the classic event-driven engine as the only backtest execution
authority. The certification layer adds:

- canonical UTC market, session, funding, borrow, valuation, decision, order, fill
  and liquidation event envelopes;
- stable ordering by availability time, event time, event-kind priority, source,
  source sequence and event identity;
- decision-time visibility and explicit rejection of future information;
- cash/equity/PnL and free-margin accounting identity checks;
- digest-chained state transitions;
- dataset-, configuration-, version- and state-bound checkpoints that fail closed;
- symbol-stable equal-timestamp historical-feed emission.

The contract does not introduce another engine, fill model, portfolio implementation
or execution shortcut.

## Automated acceptance

The focused fixture covers every equal-time event permutation, a deliberately rejected
future-funding observation, valid and invalid accounting states, a fixed golden state
digest, dataset/configuration mismatch, checkpoint tampering, transition replay and
source-row permutation at one market timestamp.

Adjacent engine evidence covers global streaming order, closed higher-timeframe bars,
backward-only auxiliary joins, margin reservation, liquidation, intrabar behavior,
classic/parallel causality parity, timeframe modes, accounting reconciliation and
independent repeat-run artifact equality.

```text
Focused and adjacent BT-004 suites: 46 passed
Scoped Ruff: clean
Full pinned Python 3.11 repository suite: 1,231 passed; 27 explicit skips
```

## Boundaries

BT-004 certifies deterministic causal and accounting semantics. BT-005 owns calibrated
fees, fills, funding, borrow, impact and capacity models. BT-006 owns distributed
orchestration. BT-007 owns feature/label lineage, and BT-008 owns dual-memory
publication. No live or capital authority is introduced.
