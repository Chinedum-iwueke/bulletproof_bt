# BT-006 validation

BT-006 adds prospective search registration and exactly-once accounting above the existing hypothesis contracts and parallel grid runner. It does not replace grid materialization, execution, resume checks or Hermes' institutional Trial Registry.

## Contract

- A search plan binds hypothesis, dataset snapshot/digest, repository/code, market-model bundle, declared parameter values, exact constraint-filtered variants, tiers, seeds, resources, budget, stopping rule and family before evaluation.
- The plan records raw Cartesian size, included and excluded variants, and total registered trials.
- Trial identities are deterministic from plan, parameters, tier and seed. Expanding a grid creates a new plan and new trial identities.
- Outcome-dependent early stopping and non-finite, duplicate, empty or over-budget grids fail closed.
- Registered manifests bind plan, family, trial, seed and initial attempt. The runner snapshots and validates the plan before launch.
- A SQLite ledger charges each trial once, retains the same trial identity across bounded attempts, prevents terminal replay and cancels only unleased work.
- Atomic run bundles bind search plan, family, trial and attempt alongside dataset, code, specification, environment and market-model lineage.

## Evidence

The retained compiler fixture uses an existing hypothesis contract and registers 24 schema-valid trial identities. Focused tests cover deterministic ordering, filtered variants, invalid and hidden-unbounded grids, duplicate values, expansion, manifest drift, idempotent registration, retry exhaustion, terminal replay, evidence digest validation, cancellation and serialized-plan tampering.

The ledger is a Bulletproof execution projection. Hermes remains the institutional owner of approvals and trial lifecycle; BT-008/009 will reconcile dual-system publication and orchestration.

Rollback rejects new registered execution while retaining plans, trials, attempts and results. Consumed search budget is never erased or reassigned.
