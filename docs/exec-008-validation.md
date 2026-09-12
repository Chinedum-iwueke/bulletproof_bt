# EXEC-008 validation

EXEC-008 keeps adapter certification inside Bulletproof and makes the evidence
environment-specific, expiring, and fail-closed.

## Delivered

- Explicit Binance perpetual and Bybit perpetual capability matrices.
- Exact EXEC-003, EXEC-004, and EXEC-007 receipt dependencies on one dataset digest.
- Twelve required drills covering authentication, clocks, precision, order lifecycle,
  partial fills, venue rejection, reconnect, restart reconciliation, duplicate
  suppression, and emergency kill.
- Distinct deterministic-conformance and venue-observed evidence classes.
- An immutable EXEC-008 producer receipt and an admission validator.
- Binance private-stream reconnect with fresh listen-key acquisition; Bybit and
  Binance reconnect/re-authentication are both exercised by deterministic drills.
- Micro-live authorization now requires the exact current live-environment receipt;
  a claimed status and digest are insufficient.

## Qualification boundary

The no-network pilot proves adapter contract conformance and proves that both demo
admission gates remain closed. It does not certify either venue demo environment,
authorize an order, or imply profitability. DEMO-001 must collect current
venue-observed evidence before demo execution is admitted. A separate live-environment
certification and later capital approval remain mandatory for micro-live.

## Verification

Run:

```bash
PYTHONPATH=src python -m pytest -q \
  tests/test_exec008_adapter_certification.py \
  tests/exec/test_c8_connector_certification.py \
  tests/exec/test_live_authorization.py

PYTHONPATH=src python scripts/exec008_pilot.py \
  --output /var/lib/invariance-swarm/exec008/native-report.json
```

The pilot succeeds only when both exact receipts replay, every deterministic drill
passes, and current venue evidence remains required.
