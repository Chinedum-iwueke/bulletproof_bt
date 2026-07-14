# Bulletproof_bt Engine Glossary

This glossary defines the core configuration variables, runtime concepts, and
output artifact fields used by Bulletproof_bt. It is written for strategy
authors, SaaS users, operators, and reviewers who need to understand what a
backtest setting or output field means before trusting a result.

## Core Backtest Concepts

- **Bar**: One timestamped OHLCV observation for one symbol. In canonical crypto research runs, base bars are 1 minute.
- **Decision timestamp**: The timestamp at which the strategy sees closed data and emits signals. Features must be known at or before this timestamp.
- **Execution timestamp**: The timestamp at which an order is filled after execution delay, spread, slippage, and intrabar rules.
- **No lookahead**: No feature, signal, funding value, OI value, membership choice, stop, or exit may use information with a timestamp later than the decision timestamp.
- **No interpolation**: Missing bars are not fabricated. Missing data means the strategy has less information or cannot decide.
- **HTF bar**: Higher-timeframe bar, such as 5m, 15m, or 1h, built from lower-timeframe bars.
- **Strict HTF completeness**: A higher-timeframe bar is available only after all required lower-timeframe bars have closed.
- **Stable universe**: Fixed configured major-symbol universe.
- **Volatile universe**: Timestamped dynamic universe reconstructed historically. A symbol is tradeable only during active membership windows known at that timestamp.
- **Research panel**: Canonical per-symbol parquet file containing OHLCV plus optional mark, index, funding, OI, basis, premium, and liquidation features.

## Configuration Variables

- **initial_cash**: Starting account equity for the backtest.
- **execution_engine**: Engine mode. `classic` uses the Python event engine. `auto` may use safe compiled feature paths when parity-tested. `fast_path` requires explicitly supported kernels.
- **data.mode**: Loading mode. `streaming` avoids loading all data at once where supported.
- **data.date_range.start / end**: Inclusive research window used for the run.
- **execution.intrabar_mode**: How ambiguous intrabar stop/target paths are resolved. `worst_case` assumes adverse fills first where ordering is unknowable.
- **execution.spread_mode**: Spread model. `fixed_bps` applies a fixed basis-point spread.
- **execution.delay_bars**: Number of base bars between signal and market order fill.
- **sizing.mode**: Optional user-facing sizing block. When present, it is normalized into `risk.mode` and related `risk.*` keys. This lets configs say `sizing.mode: fixed_notional_pct_equity` or `sizing.mode: risk_at_stop` without changing the engine's internal risk contract.
- **risk.mode**: Position sizing mode. `risk_at_stop`, `equity_pct`, and `r_fixed` mean the engine targets a configured dollar loss at the initial stop. `fixed_notional_pct_equity` means the engine targets a configured entry notional/exposure instead.
- **risk_at_stop sizing**: Professional R-normalized sizing. The engine computes `target risk dollars = equity * r_per_trade`, then sizes quantity so the initial stop would lose that amount before caps and guardrails.
- **fixed_notional_pct_equity sizing**: Exposure-first sizing. The engine computes `entry notional = equity * notional_pct_equity`; the stop still exits the trade, but actual stop-loss dollars vary with stop distance.
- **risk.r_per_trade**: Fraction of account equity intended to be lost if the initial stop is hit. Example: `0.005` means 0.5% of equity at stop, before caps and guardrails. It does not mean position notional.
- **risk.notional_pct_equity**: Fraction of account equity used as entry notional in `fixed_notional_pct_equity` mode. Example: `0.05` on a $100 account opens about a $5 position before caps and rounding.
- **risk.cap_policy**: What to do when notional, gross exposure, or margin caps prevent full target sizing. `allow_clip_with_truth` takes a smaller trade and logs actual risk. `reject_if_clipped` skips the trade when it cannot be sized as requested.
- **risk.min_risk_utilization_pct**: Minimum acceptable `actual stop risk / requested risk budget`. Example: `0.10` means a $500 target-risk trade must still risk at least $50 after caps or it is rejected as too small.
- **risk.report_under_risked_trades**: Whether capped trades should be explicitly labeled when actual stop risk is below requested risk. Default true.
- **risk.max_positions**: Maximum simultaneous open positions.
- **risk.max_leverage**: Maximum account leverage used for margin math. `1.0` means no leverage.
- **risk.max_notional_pct_equity**: Maximum entry notional as a fraction of equity. Example: `0.005` means a $100,000 account may open about $500 of notional exposure per entry.
- **risk.entry_notional_cap_buffer_pct**: Causal sizing buffer used so delayed/worst-case fills do not exceed the true notional cap. Example: `0.20` sizes against 80-83% of the cap depending on current bar range and buffer.
- **risk.max_gross_notional_pct_equity**: Maximum total open gross notional as a fraction of equity. Example: `0.025` means total open exposure cannot exceed 2.5% of equity.
- **risk.stop_resolution**: Stop contract enforcement. `strict` rejects entries without an explicit resolvable stop. `safe` may use only explicitly allowed safe fallbacks.
- **risk.allow_legacy_proxy**: Whether old proxy stop logic is allowed. Should be `false` for production research.
- **risk.min_stop_distance_pct**: Minimum stop distance as a fraction of price. Prevents pathological tiny stops from creating unrealistic position sizes.
- **risk.maintenance_free_margin_pct**: Equity reserve that must remain free after margin, fee, slippage, and adverse move buffers.
- **risk.may_liquidate**: If true, impossible margin states are explicitly liquidated and logged rather than silently ignored.
- **state_features.enabled**: Enables causal state snapshot enrichment.
- **state_features.profile**: `full` includes derivatives state features when present; `minimal` keeps a lean OHLCV state profile.

## Risk And Position Fields

- **equity_used**: Account equity snapshot used when approving the trade.
- **risk_budget**: Requested risk dollars for risk-at-stop sizing. In fixed-notional mode this equals the actual stop risk because there is no separate target risk budget.
- **requested_risk_amount**: Original risk-at-stop target before cap/margin clipping. Null in fixed-notional mode.
- **risk_amount**: Actual stop risk reconstructed from filled entry quantity and frozen stop distance. This is the R denominator used by trade metrics.
- **risk_utilization_pct**: `risk_amount / risk_budget`. A value of `1.0` means the trade used the requested risk. Values below `1.0` mean caps, rounding, or margin reduced actual risk.
- **under_risked_trade**: True when actual stop risk is below requested risk. This is expected when `cap_policy: allow_clip_with_truth` clips a position.
- **stop_price**: Initial stop price used for R and risk sizing.
- **stop_distance**: Absolute price distance between entry reference and stop.
- **entry_stop_distance**: Frozen stop distance stored at entry and used to reconstruct R later.
- **qty / entry_qty / exit_qty**: Filled quantity. For shorts, side carries direction; quantity is usually positive in trade rows.
- **entry_price / exit_price**: Actual modeled fill prices after execution assumptions.
- **sizing_notional**: Raw notional implied by stop-distance sizing before notional caps. Large values here reveal tight-stop pressure.
- **requested_notional**: Target entry notional in fixed-notional sizing mode.
- **notional_est**: Order notional estimate at approval time. Prefer actual filled notional for final exposure audits.
- **actual filled notional**: `abs(entry_qty * entry_price)`. This is the truth exposure used by validation.
- **max_notional**: True maximum allowed entry notional for that trade after account and gross caps.
- **effective_max_notional**: Lower causal sizing cap after applying pre-fill buffer.
- **cap_applied**: Whether notional or gross exposure cap reduced the requested size.
- **cap_reason**: Reason for cap, such as `max_notional_pct_equity` or `max_gross_notional_pct_equity`.
- **max_gross_notional**: Maximum total open gross notional allowed by config.
- **current_gross_notional**: Existing gross open exposure before approving the new order.
- **remaining_gross_notional**: Remaining gross budget available to the new order.
- **gross_cap_applied**: Whether the gross exposure cap resized the order.
- **margin_required**: Initial margin locked for the position under configured leverage.
- **margin_fee_buffer**: Fee reserve included in margin safety checks.
- **margin_slippage_buffer**: Slippage reserve included in margin safety checks.
- **margin_adverse_move_buffer**: Additional adverse move reserve based on current bar range and buffer tier.
- **free_margin_post**: Equity remaining after required margin, buffers, and maintenance reserve.
- **maintenance_required**: Maintenance margin estimate.
- **forced_liquidation**: True if the engine had to liquidate due to margin rules. Production research truth gates fail when this occurs unless explicitly testing liquidation behavior.

## Performance And R Fields

- **pnl**: Gross trade profit/loss before costs.
- **fees_paid / fees**: Fees charged by the execution model.
- **slippage**: Slippage cost charged by the execution model.
- **pnl_net**: Net trade profit/loss after costs.
- **r_multiple_gross / realized_r_gross**: Gross profit/loss divided by initial risk.
- **r_multiple_net / realized_r_net / r_net**: Net profit/loss divided by initial risk.
- **ev_r_net**: Mean net R across trades. This should equal the average of trade-level net R.
- **win_rate**: Fraction of trades with positive net R.
- **mfe_r**: Maximum favorable excursion in R.
- **mae_r**: Maximum adverse excursion in R.
- **cost_drag_r**: Cost impact expressed in R.
- **r_metrics_valid**: Whether R metrics were computed from a valid explicit risk contract.

## Rich State Fields

- **entry_state_ts**: Timestamp of the causal state snapshot attached to the trade.
- **entry_state_csi_source**: `ohlcv_proxy` or `enriched`.
- **entry_state_csi_raw / pctile / bucket**: Constraint Stress Index value, rolling percentile, and bucket.
- **entry_state_funding_raw**: Funding rate known at entry decision time.
- **entry_state_funding_source_ts**: Funding event timestamp used. Must be `<= entry_state_ts`.
- **entry_state_funding_pctile / z / regime**: Causal funding state.
- **entry_state_open_interest / oi_level**: OI snapshot known at decision time.
- **entry_state_oi_source_ts**: OI source timestamp used. Must be `<= entry_state_ts`.
- **entry_state_oi_change / oi_change_pct**: Recent OI change.
- **entry_state_oi_accel / oi_accel_pctile**: OI acceleration and causal percentile.
- **entry_state_mark_price / mark_close**: Mark price known at decision timestamp.
- **entry_state_index_price / index_close**: Index price known at decision timestamp.
- **entry_state_basis_raw / basis_pct / basis_pctile**: Perp/index basis state.
- **entry_state_premium_raw / premium_pctile**: Mark-vs-index premium state.
- **entry_state_crowding_proxy**: Composite crowding pressure proxy.
- **entry_state_constraint_stress_proxy**: Composite market-structure stress proxy.

## Output Artifacts

- **config_used.yaml**: Exact merged config used for a run.
- **decisions.jsonl**: Streamed decision records. Contains approved and rejected signals with reasons.
- **fills.jsonl**: Streamed fill records. Contains execution prices, costs, and metadata.
- **trades.csv**: Source-of-truth closed trade table for performance, state discovery, and ML extraction.
- **equity.csv**: Equity curve over time.
- **performance.json**: Run-level metrics summary.
- **run_status.json**: Atomic status file for resume safety.
- **run_timing.json**: Runtime and stage timing.
- **run_summary.csv**: Experiment-level summary of runs after post-analysis.
- **runs_dataset.parquet**: Extracted run-level ML dataset.
- **trades_dataset.parquet**: Extracted trade-level ML dataset.
- **truth_validation_report.json/md**: Hard validation result. A failed truth report means downstream learning must not trust the experiment.
- **state findings**: Structural state analyses used to discover conditional EV regimes.
- **verdict bundle**: Deterministic and optional LLM-assisted decision memo.
- **research memory**: Long-term evidence store built only from validated artifacts.

## How To Interpret Common Settings

- Setting `risk.r_per_trade: 0.005` does not mean each trade uses 0.5% notional. It means the loss at the stop is intended to be 0.5% of equity.
- Setting `risk.max_notional_pct_equity: 0.005` does mean entry exposure itself is capped near 0.5% of equity.
- Setting `sizing.mode: fixed_notional_pct_equity` with `notional_pct_equity: 0.05` means "open about 5% of equity as position notional." It does not mean "lose 5% if stopped."
- Setting `sizing.mode: risk_at_stop` with `r_per_trade: 0.005` means "size the position so the stop loses about 0.5% of equity." The resulting position notional may be much larger or smaller depending on stop distance.
- With `cap_policy: allow_clip_with_truth`, capped trades are still taken when possible, but `risk_utilization_pct` and `under_risked_trade` reveal that the trade did not use the full requested risk.
- With `min_risk_utilization_pct: 0.10`, capped trades that would use less than 10% of requested risk are rejected as dust.
- If `sizing_notional` is much larger than `max_notional`, the stop is tight and the notional cap is protecting the account.
- If `cap_applied` is always true, the strategy is mostly notional-cap constrained and its raw stop sizing is not deployable at the requested risk.
- If `free_margin_post` is negative, the run is invalid for production research.
- If `entry_state_funding_source_ts > entry_state_ts`, the run has lookahead and must be rejected.
- If `r_metrics_valid` is false, trade R values are not suitable for EV analysis.
