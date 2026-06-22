# Research Truth Repair - 2026-06-21

## Status

The prior Tier 2 outputs, state findings, verdict, and research memory are invalidated and must not be used as evidence. The repair keeps strategy and execution semantics unchanged and tightens accounting and post-run truth contracts.

## Corrected Failures

- `risk_budget` now records requested capital-at-risk; `risk_amount` records actual filled stop risk after caps, rounding, and fill price.
- Net and gross R are therefore derived from actual deployed stop risk, while utilization reports how much of the requested budget was deployed.
- Entry notional cap buffering is applied once through the conservative reference price, not twice.
- Entry risk, sizing, cap, margin, and exposure metadata are frozen and cannot be overwritten by exit metadata.
- Experiment extraction preserves per-trade `net_pnl` separately from `run_net_pnl` and populates stable trade, parameter-set, and signal-time identities.
- Research memory marks rows invalid when required PnL, R, identity, run, or signal-time truth is absent.
- Causal rolling percentiles use tie mid-ranks; constant funding is neutral (`0.5`) rather than extreme (`1.0`).
- Truth validation now checks required source artifacts, actual stop-risk reconciliation, risk budgets, R reconciliation, caps, margin, causal source timestamps, and extracted dataset population.
- Retained reference runs preserve `equity.csv` by default.
- Queue items become `DONE` only after state discovery, interpretation/verdict, research memory, and final terminal-card refresh all succeed.
- Final terminal cards discover phase-scoped verdicts and use canonical extracted execution-drag columns.

## Verification

- 78 focused accounting, extraction, state, validation, cleanup, daemon, terminal-card, and configuration tests passed.
- 15 engine accounting, deterministic regression, margin, liquidation, smoke, and rich-state logging tests passed.
- Python compilation and `git diff --check` passed.
- A repository-wide pytest invocation was stopped after its wrapper hung without a live pytest process; no failure output was emitted.

## Reset Policy

Generated Tier 2 outputs and derived intelligence are removed. Hypothesis YAML files and approved queue payloads are preserved. All approved backtests are reset to `PENDING` with their priority order intact.

## Restart Verification

- Dedicated tmux session: `research-daemon`
- First job: `l1_h11a`
- Grid mode: stable research panel fast path
- Workers launched: 8
- Host memory at launch: 64 GiB available, zero swap used
- Queue state: `l1_h11a=LOCKED`, remaining 27 hypotheses `PENDING`
