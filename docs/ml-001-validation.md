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

## Rollback

Stop new registration, retain all bundles/events, and reactivate a previously evaluated content digest through an independently attributed rollback event. No registry state can authorize a strategy, allocation, or order.
