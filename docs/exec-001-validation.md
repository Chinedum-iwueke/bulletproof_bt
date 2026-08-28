# EXEC-001 validation

EXEC-001 adds an append-only canonical market/order event journal to Bulletproof.
Every event binds source and stream identity, event and receive clocks, source
sequence, payload digest, optional correction lineage and a canonical event digest.

Replay is point-in-time by receive clock and deterministically ordered. Exact
duplicates are idempotent; conflicting identity reuse, invalid clocks and unknown
corrections fail closed. Gaps and late arrivals are visible evidence. Corrections
supersede projections without mutating journal history.

Hermes may register the schema and immutable producer receipt. It does not produce
events, infer ordering or reconstruct execution state. Neither repository receives
allocation, capital, promotion or order authority from this evidence contract.
