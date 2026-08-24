# BT-009 Validation

The governed bridge compiler and monotone lifecycle ledger are covered by the
golden CSI fixture and fail-closed tests for ambiguous tiers, unknown
parameters, unregistered values, duplicate values, hidden optimization,
Cartesian-budget overflow, missing auxiliary data, approval mutation, authority
expansion, reordered stages, immutable receipts, and idempotent replay.

BT-009 reuses the classic engine, BT-002 snapshots, BT-003 bundles, BT-005
market-model registry, BT-006 search ledger, BT-007 representation contracts,
BT-008 publication saga, and existing CSI strategy implementation. It does not
restore or authorize the deprecated fast path.
