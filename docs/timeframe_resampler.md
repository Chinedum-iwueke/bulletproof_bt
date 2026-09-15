# TimeframeResampler: current behavior map

This note describes the current repository behavior as implemented today.

## Arbitrary whole-minute research durations

The native parser now accepts positive integer durations with `m`, `h`, or `d`
units, such as `7m`, `12m`, `2h` and `2d`. `1m` remains valid for base-feed
compatibility. Seconds, fractional durations, calendar months and weeks are not
accepted. Input is still a strictly increasing, minute-aligned UTC 1m feed per
symbol; duplicates and out-of-order bars fail before updating bucket state.

All fixed durations use UTC floor alignment anchored to the Unix epoch. Odd
durations such as 7m continue across midnight; there is no daily bucket reset.
This preserves prior supported-interval boundary behavior. Strict completeness,
rollover-only emission and no interpolation remain unchanged. Emitted metadata
records bucket origin, timestamp convention, base cadence, strictness and
availability policy. Equivalent spellings such as 120m and 2h are valid but
remain distinct configuration labels and must not be double-counted as novelty.

The research-spec validators and shared timeframe-boundary helper reuse this
parser. Strategy-specific qualified cadence restrictions are not bypassed.
This change supports OHLCV aggregation, not automatic funding/OI/mark/index
aggregation or authority to use unadmitted data. Freeze timeframe alternatives
before evaluation and count them toward the finite research search budget.

## Where resampling is wired

- Resampling implementation lives in `src/bt/data/resample.py` (`TimeframeResampler`).
- Strategy context injection uses `HTFContextStrategyAdapter` in `src/bt/strategy/htf_context.py`, which calls `resampler.update(bar)` for each incoming symbol bar at each engine timestamp and places only *newly closed* HTF bars in `ctx["htf"]`.
- Construction path is `_build_engine` in `src/bt/api.py`:
  - it reads `config["htf_resampler"]` when present,
  - instantiates `TimeframeResampler(timeframes=..., strict=...)`,
  - wraps strategy with `HTFContextStrategyAdapter`.
- Legacy config aliasing into `htf_resampler` is normalized in `src/bt/core/config_resolver.py` via `htf_timeframes` / `htf_strict`.

## Input assumptions

- Input is assumed to be **1-minute UTC bars only** (`base_freq="1min"` required).
- Non-UTC or tz-naive bar timestamps raise assertions.
- No support for non-1m base feed in current implementation.

## Bucket boundaries and timestamp semantics

- Bucket start is computed with UTC floor semantics (`ts.floor(...)`).
  - Minute-based timeframes use `floor("{N}min")`.
  - `1h` uses `floor("1h")`.
  - `1d` uses `floor("1d")`.
- Emitted `HTFBar.ts` is the **bucket start timestamp** (not bucket close).

## Emission timing, strict mode, and completeness

- Resampler is streaming/event-driven: each `update(bar)` can emit zero or more HTF bars.
- A bucket is finalized only when a bar from a *new* bucket arrives (`bucket_start` changes).
- Completeness checks:
  - `expected_bars` = timeframe length in minutes.
  - `n_bars` = number of 1m bars observed in the bucket.
  - any intra-bucket timestamp jump > 1 minute marks bucket incomplete.
  - final completeness is `not is_incomplete and n_bars == expected_bars`.
- In strict mode (`strict=True`), incomplete buckets are **suppressed** (not emitted).
- There is no forced final flush at end-of-data by the resampler itself; only closed buckets observed at a boundary transition can emit.

## Gap policy / no interpolation

- Missing input minutes are not fabricated.
- Gaps mark bucket incomplete (strict mode then drops that bucket).
- No interpolation/backfill/synthetic bars are created.

## Engine ordering and no-lookahead interaction

- Engine loop pulls one timestamp slice from feed and calls strategy exactly once per timestamp.
- HTF adapter runs inside `strategy.on_bars(...)` and processes only currently available base bars.
- Because HTF bars emit only on bucket rollover (arrival of the first bar of next bucket), strategies never see an HTF bucket before it is closed.
- `ctx["htf"]` includes only newly emitted bars for the current engine timestamp, not future buckets.

## Potential lookahead pitfalls if misused

- Using *current, in-progress* bucket values outside this adapter (not done here) could introduce lookahead.
- Treating `HTFBar.ts` as close-time instead of open-time can cause alignment mistakes.
- Disabling strict mode in future implementations must still avoid exposing unclosed future data.

## New config knob: `data.timeframe`

- `data.timeframe` now provides a single run-level target timeframe override for HTF resampling.
- Supported values: positive integer `m`, `h`, or `d` durations (including `1m`).
- Behavior:
  - when set, it overrides `htf_resampler.timeframes` with a single-element list `[data.timeframe]` at engine build time,
  - if `htf_resampler` was absent, it creates one with strict mode enabled,
  - if unset, behavior is unchanged from previous wiring.
- It controls HTF context resample target used by the existing adapter path.
- It does **not** modify engine loop timing/clock semantics in `src/bt/core/engine.py`.

### Example override

```yaml
data:
  timeframe: "15m"
```
