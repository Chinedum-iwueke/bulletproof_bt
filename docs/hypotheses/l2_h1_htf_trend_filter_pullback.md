# L2-H1 HTF Trend Filter Upgrades LTF Pullbacks

## Claim

LTF pullback entries have higher net EV and lower drawdowns when conditioned on
fully closed 1h trend direction.

## Market Structure Rationale

Crypto trades continuously, so dominant higher-timeframe flow often persists
across calendar sessions instead of resetting around exchange opens. A local
pullback into EMA20 can be either continuation timing or mean-reversion noise;
the hypothesis is that closed 1h EMA50/EMA200 direction separates those cases.

## Data Inputs

- Base execution data: canonical 1m research panels.
- LTF signal bars: strict 1m or 5m bars.
- HTF trend bars: strict closed 1h bars only.
- Required fields: OHLCV.
- Optional logging-only fields: mark, index, funding, OI, basis, premium.

Funding/OI/mark/index/basis are not signal gates in v1. They are logged as
entry-state context for downstream structural buckets, verdicts, and research
memory.

## Signal Rules

HTF trend:

```text
dir_htf = sign(EMA_50(1h close) - EMA_200(1h close))
```

LTF pullback:

- Long candidate starts when LTF close crosses below EMA20.
- Long entry occurs when LTF close returns above EMA20 within `K` LTF bars.
- Short candidate starts when LTF close crosses above EMA20.
- Short entry occurs when LTF close returns below EMA20 within `K` LTF bars.

Filtered variants require:

- long only when `dir_htf = +1`
- short only when `dir_htf = -1`

Baseline variants set `use_htf_filter=false` to measure EV uplift and drawdown
change against no-filter pullbacks inside the same grid.

## Entry And Exit

Entries are submitted at LTF bar close for next-bar engine execution.

Exits:

- Frozen ATR stop: `k_atr * ATR_14` on the LTF signal bar.
- Time stop after `T_hold` signal bars.
- No take-profit in v1.

The strategy emits explicit `stop_price` and `entry_stop_price`; unresolved stops
are expected to reject entries under the engine stop contract.

## Risk

The production risk contract is risk-at-stop:

```yaml
sizing:
  mode: risk_at_stop
  r_per_trade: 0.005
  cap_policy: allow_clip_with_truth
  min_risk_utilization_pct: 0.10
  report_under_risked_trades: true
```

The engine remains the authority for quantity, notional caps, margin, costs,
fills, R metrics, and accounting.

## Parameter Grid

- `timeframe`: `1m`, `5m`
- `K`: `3`, `5`
- `k_atr`: `2.0`, `2.5`
- `use_htf_filter`: `true`, `false`
- `T_hold`: `48`

Total Tier2 grid rows: 16.

## Artifacts Logged

Trade metadata includes:

- `dir_htf`
- `htf_ready`
- `htf_ema_fast`
- `htf_ema_slow`
- `htf_source_ts`
- `ltf_ema20`
- `ltf_close`
- `pullback_bars`
- `K`, `k_atr`, `T_hold`
- `use_htf_filter`
- `entry_state_*` rich context fields when present
- decision trace conditions/gates
- explicit stop/risk fields for engine truth validation

## Evaluation

Primary comparisons:

- filtered EV versus no-filter EV
- drawdown duration reduction
- trade count reduction
- win rate and tail-R change
- cost drag after fees/spread/slippage

## Falsification

Scrap or refine if the HTF filter reduces trades without improving net EV,
drawdown duration, or tail behavior. Also reject if the edge is only broad beta
exposure during bull regimes.

## Fast Path

This first implementation is `classic_only` for signal semantics. It can still
benefit from the shared HTF precomputed context path when available, but no
family-specific compiled execution kernel is enabled until parity tests prove
identical decisions, fills, trades, equity, and metrics.
