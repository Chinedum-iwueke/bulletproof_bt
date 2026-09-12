# RISK-003 validation

RISK-003 adds the authoritative Bulletproof producer for causal dynamic risk budgets.
It binds exact RISK-001, RISK-002, ML-004 and PORT-004 receipts, consumes only
point-in-time state available by the declared cutoff, and emits an immutable budget
trajectory with volatility, regime, confidence, uncertainty, hysteresis, delayed
recovery, expiry and executable-capacity controls.

Defensive reductions are immediate. Increases require a declared recovery streak and
are step-limited. Missing, expired, unknown or zero-capacity evidence selects the
static conservative fallback. The result has no allocation, capital, order or
promotion authority.

Validation commands:

```bash
python -m pytest -q tests/test_risk003_dynamic_budget.py
ruff check src/bt/institutional/risk_budget.py tests/test_risk003_dynamic_budget.py scripts/risk003_pilot.py
python scripts/risk003_pilot.py --source-commit "$(git rev-parse HEAD)" --output /tmp/risk003-native-report.json
```
