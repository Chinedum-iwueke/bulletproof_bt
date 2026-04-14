# L1-H11 Quality-Filtered Continuation (Parent Family)

## Parent claim
Continuation trades improve net expectancy when entries are restricted to structurally clean setups with: directional context, sufficient impulse, bounded pullback geometry, deterministic reclaim entry, and explicit protection discipline.

## Canonical runtime semantics
- Base data frequency: `1m`.
- Signal logic: completed higher-timeframe bars only.
- Exit/protection monitoring: `1m` clock.
- No pyramiding.
- Canonical R truth: `engine_canonical_R` only.

## Core family sequence
1. EMA20/EMA50 directional continuation context.
2. Minimum impulse requirement in ATR units.
3. Pullback depth constrained by `[pull_entry_atr_low, pull_entry_atr_high]`.
4. Reclaim trigger: close reclaims EMA20 in trend direction.
5. Frozen initial stop established at entry.
6. Variant-specific protection behavior for post-entry survivability.

## Mandatory stop semantics
- ATR sourced from signal timeframe.
- Initial stop is explicit and frozen at entry.
- Any later stop movement is protective-only and does not redefine initial risk/R.

## Shared diagnostics fields
H11 logs setup type, timeframe semantics, trend/indicator state at entry, impulse/pullback geometry, entry-position metric, stop/protection controls, cost breakdown, and canonical trade outcomes (`mfe_r`, `mae_r`, `r_multiple_gross`, `r_multiple_net`).
