# Research Tiers

Bulletproof_bt separates signal discovery from portfolio deployability.

## Tier 2A: Signal Episode

Tier 2A answers:

> When this signal appears, what happens afterward?

It is evidence for signal quality and ML/state learning. It is not evidence
that an account can deploy the strategy under portfolio constraints.

Tier 2A runs are labelled:

- `research_tier: tier2a`
- `research_mode: signal_episode`
- `evidence_type: signal_outcome`
- `portfolio_constraints_applied: false`
- `capital_path_valid: false`
- `deployability_evidence: false`
- `signal_episode_evidence: true`

Tier 2A keeps causal execution assumptions, costs, delay, stops, intrabar
rules, path labels, rich state snapshots, ML extraction, state discovery,
terminal cards, verdict packets, and research-memory ingestion.

The current implementation uses the classic engine compatibility bridge:
decision logging is sparse and portfolio-level admission bottlenecks are
relaxed, while required run artifacts remain compatible with the existing
post-run pipeline. A future independent episode executor can replace this
bridge behind the same artifact labels.

Example:

```bash
python orchestrator/queue_hypothesis.py \
  --hypothesis research/hypotheses/example.yaml \
  --name example_signal_episode \
  --phase tier2a
```

## Tier 2B: Portfolio Simulation

Tier 2B is the current production portfolio-style Tier 2 backtest. It answers:

> Can this strategy survive as a constrained account simulation?

Tier 2B runs are labelled:

- `research_tier: tier2b`
- `research_mode: portfolio_backtest`
- `evidence_type: portfolio_outcome`
- `portfolio_constraints_applied: true`
- `capital_path_valid: true`
- `deployability_evidence: true`
- `signal_episode_evidence: false`

`tier2` remains accepted as a backward-compatible alias for `tier2b`.

Example:

```bash
python orchestrator/queue_hypothesis.py \
  --hypothesis research/hypotheses/example.yaml \
  --name example_portfolio \
  --phase tier2b
```

## Memory And ML

Both tiers flow into extraction and research memory. Consumers must filter by
`research_mode` or `evidence_type`:

- Use `signal_episode` / `signal_outcome` to learn signal-state relationships.
- Use `portfolio_backtest` / `portfolio_outcome` to judge deployability,
  drawdown, margin safety, and account-path behavior.

Never mix Tier 2A and Tier 2B as if they mean the same thing.
