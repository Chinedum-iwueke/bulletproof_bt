# Bybit Live Hardening Contract (Phase 6)

## Scope
Phase 6 adds first-pass **Bybit mainnet/live** execution support with conservative startup guards, canary gating, and internal freeze controls.

## Demo vs Live
- `broker.environment: demo` keeps demo endpoints and demo auth env vars only.
- `broker.environment: live` requires live endpoints and non-demo auth env vars.
- Endpoint/auth mismatches are rejected during config resolution.

## Startup safety gate (live)
Before live mutation is enabled, runtime now performs:
1. adapter start + private stream readiness check (if required),
2. startup reconciliation against open orders, positions, fills, and balances,
3. canary policy loading/validation.

If startup fails, runtime is blocked/frozen and can be configured to either:
- remain read-only (`live_controls.read_only_startup: true`), or
- fail startup immediately.

## Canary controls
Canary controls are config-driven (`live_controls` + `canary`) and enforced pre-submit:
- symbol allowlist,
- max symbols,
- max open positions,
- max open orders,
- max order qty,
- max notional per order,
- max submitted orders per session.

Violations freeze new orders.

## Kill/freeze behavior
`KillSwitch` tracks frozen state and reason timestamp. Freeze can be triggered by:
- startup gate failure,
- reconciliation freeze action,
- canary guard violations,
- transport failure escalation.

Run status exposes freeze/read-only/startup gate fields.

## Restart/recovery in live
Live mode keeps existing checkpoint/recovery wiring and now requires startup safety gate before live mutation is enabled. If safe continuation cannot be established, runtime remains frozen/read-only or fails startup per config.

## Current limitations after Phase 6
- No external alerting pipeline in this phase.
- No exactly-once guarantee beyond current lifecycle/idempotency model.
- Rate-limit handling is conservative but still basic (header snapshot + freeze hooks; no global token bucket yet).
- Reduce-only exemptions while frozen are policy-surfaced but not a full separate reduce-only order path.
