# Portfolio and Risk Contract

## What this contract covers
This contract defines portfolio/risk controls, stop-resolution modes, and client-safe vs strict behavior.

Implementation: `src/bt/portfolio/portfolio.py`, `src/bt/risk/risk_engine.py`, `src/bt/risk/stop_resolver.py`, `src/bt/core/config_resolver.py`.

## V1 support
- Risk normalization and validation for canonical `risk.*` keys.
- Stop resolution modes:
  - `safe`
  - `strict`
  - legacy alias `allow_legacy_proxy` (normalized to safe+allow)
- Config packs shipped:
  - `configs/examples/safe_client.yaml`
  - `configs/examples/strict_research.yaml`

## Inputs and guarantees
- Risk keys validated with bounds (for example `min_stop_distance_pct`, `max_notional_pct_equity`, `maintenance_free_margin_pct`).
- `strict` mode forbids `allow_legacy_proxy=true`.
- Stop contract reporting is deterministic for identical inputs.

## Sizing Modes

Bulletproof_bt now supports two explicit sizing intents. They answer different
questions and must not be conflated.

### Risk-at-stop sizing

```yaml
sizing:
  mode: risk_at_stop
  r_per_trade: 0.005
```

Equivalent legacy/internal forms:

```yaml
risk:
  mode: equity_pct   # or r_fixed
  r_per_trade: 0.005
```

This means: "size the position so the initial stop would lose 0.5% of
equity." Position notional is derived from stop distance:

```text
target_risk = equity * r_per_trade
qty = target_risk / stop_distance
notional = qty * entry_price
```

This is the R-normalized/professional model because different stop distances
can still produce comparable loss impact.

### Fixed-notional-percent sizing

```yaml
sizing:
  mode: fixed_notional_pct_equity
  notional_pct_equity: 0.05
```

Equivalent internal form:

```yaml
risk:
  mode: fixed_notional_pct_equity
  notional_pct_equity: 0.05
```

This means: "open a position with about 5% of equity as notional exposure."
The stop still exits the trade and creates R metrics, but the actual loss at
stop varies with stop distance:

```text
notional = equity * notional_pct_equity
qty = notional / entry_price
actual_stop_risk = qty * stop_distance
```

Use this mode when the research question is exposure-first, such as small
account fixed-allocation tests.

## Cap Policy And Truthful Clipping

Notional, gross exposure, and margin caps are separate from the sizing target.
When a cap prevents full risk-at-stop sizing, use an explicit policy:

```yaml
sizing:
  mode: risk_at_stop
  r_per_trade: 0.005
  cap_policy: allow_clip_with_truth
  min_risk_utilization_pct: 0.10
  report_under_risked_trades: true
```

`allow_clip_with_truth` keeps potentially useful trades, but logs the truth:

- `risk_budget`: requested risk dollars
- `risk_amount`: actual filled stop risk
- `risk_utilization_pct`: `risk_amount / risk_budget`
- `under_risked_trade`: true when actual risk is below requested risk
- `cap_applied` / `gross_cap_applied`: whether caps resized the order

If the clipped trade would use less than `min_risk_utilization_pct` of the
requested risk, it is rejected as dust.

For strict measurement, use:

```yaml
sizing:
  mode: risk_at_stop
  r_per_trade: 0.005
  cap_policy: reject_if_clipped
```

That rejects capped trades instead of taking smaller positions.

## Instrument-aware sizing (T3)

Risk-at-stop sizing remains R-normalized (`risk_amount` and stop-distance
based), with deterministic instrument-aware conversion:

- **Crypto / no instrument block**
  - Keeps existing sizing behavior (no new rounding changes by default).
- **Equity**
  - Quantity is shares.
  - Shares are rounded down to whole integers.
- **Forex**
  - Quantity is lots.
  - Uses `instrument.contract_size` and `risk.fx.lot_step`.
  - Rounds down to lot step deterministically.

### New optional risk keys
- `risk.fx.lot_step` (required when `instrument.type=forex`)
- `risk.fx.pip_value_override` (optional)
- `risk.margin.leverage` (optional; if set must be `> 0`)

### Validation guardrails
- `instrument.type=forex` requires:
  - `instrument.contract_size`
  - `risk.fx.lot_step`
- Invalid/missing sizing keys raise actionable `ValueError`/`ConfigError` with key paths.

## Rejections and failure modes
- Invalid risk key types/ranges raise config errors.
- Strict mode + missing/unresolvable stop rejects entry intents.
- Margin and notional guardrail violations are rejected.
- Instrument-aware sizing can reject too-small orders (for example lot-step rounding to zero).

## Artifacts and metadata
- `decisions.jsonl` includes risk metadata fields such as:
  - `risk_amount`, `stop_distance`, `stop_source`
  - `qty_rounding_unit`, `instrument_type`, `sizing_notional`, `sizing_margin_required`
- `run_status.json` includes stop-resolution summary fields.

## Versioning
- Contract version: v1.
- Stop contract report includes `version: 1` today.
- Other risk schema versioning is not explicitly tagged; docs + tests are source of truth.
