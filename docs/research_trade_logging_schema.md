# Research Trade Logging Schema (Phase 9)

Every trade row should preserve six layers: (1) structural state, (2) decision rationale, (3) execution reality, (4) realized path, (5) counterfactual outcomes, and (6) ML/meta labels.

Required prefixes:
- `identity_*`
- `entry_state_*`
- `entry_gate_*`
- `entry_decision_*`
- `execution_*`
- `risk_*`
- `path_*`
- `exit_*`
- `counterfactual_*`
- `label_*`

Strategies can attach `decision_trace` via signal metadata. The engine/trade writer flattens this into `entry_decision_*` and `entry_gate_*` columns. Missing fields remain `None` (additive, backward compatible).
