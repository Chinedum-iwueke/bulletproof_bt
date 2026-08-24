# Classic Engine Certification Contract

The classic event-driven engine is authoritative. Certification is a fail-closed layer
around its existing market feed, strategy, risk, execution, portfolio and artifact
surfaces; it is not an alternate simulator.

## Two clocks

Every certified event declares `event_time` and `available_at` in UTC. `event_time`
describes when the underlying event occurred. `available_at` is the earliest instant at
which a decision may observe it. A decision at `t` may bind only evidence with
`available_at <= t`. Publication latency may therefore place an older event after a
newer market event without rewriting either clock.

## Stable ordering

Events sort by availability time, event time, canonical kind priority, source, source
sequence and event identity. Duplicate identities fail. Historical bars sharing one
timestamp emit in lexical symbol order, removing source-row-order dependence. Dataset
validation remains responsible for rejecting duplicate symbol/timestamp rows.

## Accounting

Every certified portfolio snapshot must satisfy:

```text
equity = cash + realized_pnl + unrealized_pnl
free_margin = equity - used_margin
used_margin >= 0
```

All values must be finite. Negative free margin remains representable because it is an
engine liquidation signal, not an accounting-identity violation.

## State and replay

Each transition digest binds the prior digest, complete canonical event and resulting
state digest. A checkpoint binds the contract version, immutable dataset digest,
configuration digest, last event ordering key and state digest. Resume rejects an
unsupported contract version, changed dataset/configuration or altered state. Evidence
is retained when a checkpoint is rejected; incompatible state is never silently loaded.
