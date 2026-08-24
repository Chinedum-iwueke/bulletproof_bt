# Governed Research Bridge

BT-009 composes existing Bulletproof contracts; it does not create a second
engine, registry, optimizer, or memory authority. Founder prose and pasted
hypothesis cards first become a non-executable, digest-bound proposal. Only an
explicitly approved proposal may move through the monotone bridge lifecycle.

The compiler resolves a registered hypothesis and strategy identity, an exact
finite parameter grid, a typed research tier, an immutable dataset snapshot,
and the repository commit. `Tier2A`, `Tier2B`, and `Tier3` are distinct. Legacy
`Tier2` is rejected unless its compatibility resolution is supplied and shown.
Unknown parameters, unregistered values, hidden optimization language,
unavailable auxiliary fields, and grids above the approved budget fail closed.

The first golden contract is CSI-Gated Displacement Trend. It reuses
`research/hypotheses/l7_h1_csi_gated_displacement_trend.yaml` and
`bt.strategy.l7_h1_csi_gated_displacement_trend`; the bridge must not regenerate
either. Its requested `d0`, `theta`, `k_stop`, and `k_trail` grid contains
exactly 16 variants.

The durable stage order is:

1. founder approval;
2. prospective registry binding;
3. native classic-engine execution;
4. truth validation;
5. atomic bundle finalization;
6. independent statistical and adversarial review;
7. Hermes laboratory publication;
8. Bulletproof memory confirmation;
9. completion.

Every transition requires an immutable receipt and cannot be skipped. All
states retain the no-capital, no-live-order, no-self-approval, and
no-production-promotion boundary.

Compile a submission with:

```bash
python scripts/compile_governed_research.py \
  --submission submission.json \
  --repository-commit "$(git rev-parse HEAD)" \
  --output proposal.json
```

Compilation does not run a backtest. Native execution remains behind approval,
registry, dataset, representation, market-model, search-ledger, truth, review,
and publication gates.
