# ML-001 Reproducible Model and Calibration Registry

ML-001 adds a predictive-model artifact and lifecycle boundary without granting alpha, order, sizing, promotion, or capital authority.

## Contract

- Immutable SHA-256 bindings cover the scientific problem, point-in-time dataset, BT-007 representation, temporal split, and source revision.
- The first certified estimator is a transparent deterministic logistic baseline with frozen training-set normalization.
- Calibration uses a distinct held-out partition and retains raw and calibrated log-loss and Brier scores.
- The artifact records seed, fit configuration, environment, coefficients, support range, uncertainty semantics, abstention and shift policy.
- The content-addressed artifact store refuses conflicting bytes.
- The SQLite registry records immutable versions and append-only lifecycle events. Independently attributed activation supersedes the prior active version; rollback reactivates a retained version.
- Inference receipts bind model, inputs, calibration method, probability, uncertainty, applicability, abstention, observation time and authority.
- Public JSON schemas define both the immutable model bundle and calibrated inference receipt envelopes.
- Schema mismatch, naive time, non-finite input, model corruption and distribution shift fail closed or abstain.

## Validation

```bash
pytest -q tests/test_ml_model_registry.py
python scripts/ml001_pilot.py --output /tmp/ml001-pilot
```

The pilot is an intentionally small deterministic contract fixture, not an alpha claim or production model. It performs two identical fits, independent held-out calibration/evaluation, registration, challenger activation, rollback, supported inference and shifted-input abstention without touching production resources.

The retained pilot at `docs/evidence/ml-001/` binds source commit `8dbfc4eaf9c2243ed5e035df68a4ae1dff05413d`. The original and reproduced model digest is `5e3b8c80a494aaedb33dd61c8484fb59ed1f89254dec52724ebae82193e175a3`; registry snapshot digest `1d738ee40118551ccfbeb3edabfea609b09a4319958fa55a3ee5772d78c3d605` and report digest `7e3f250ffb54d5d6e194ad07cee4a065b2bb614ac8ce52f708b99f398bb5cd69` record successful calibration, inference, shift abstention, version activation and rollback.

## Rollback

Stop new registration, retain all bundles/events, and reactivate a previously evaluated content digest through an independently attributed rollback event. No registry state can authorize a strategy, allocation, or order.
