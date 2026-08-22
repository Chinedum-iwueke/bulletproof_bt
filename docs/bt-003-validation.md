# BT-003 Validation

**Date:** 2026-08-22

Acceptance fixtures cover:

- atomic finalization and content-addressed replay;
- equal semantic digests across independent run directories;
- idempotent duplicate finalization and one stored receipt;
- interruption with no partial bundle and a retained failed attempt;
- missing, corrupt, binary and incompatible-schema artifacts;
- bundle-manifest and artifact-byte corruption;
- absolute protected paths and likely secret material;
- legacy manifest path normalization with source-byte provenance;
- digest-bound Hermes run payload and publication receipt;
- compatibility with existing required-artifact, artifact-manifest, run-manifest,
  BASE-002 identity and BT-002 snapshot tests.

The fixture is bounded and synthetic. It contains no market-data rows, credentials,
absolute protected paths or live-order surface.

The complete deterministic matrix collected 1,251 tests: 1,224 passed and 27
pre-existing explicit skips. Scoped Ruff passed for every BT-003 source and test file.
