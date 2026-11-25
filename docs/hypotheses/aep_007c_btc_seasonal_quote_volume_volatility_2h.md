# AEP-007C BTC seasonal quote-volume surprise / future volatility

This native, observation-only hypothesis implements accepted proposal `532cf1f5…aecb`. It asks whether a completed BTCUSDT 2h bar's `log1p(quote_volume)` surprise, standardized using only the preceding 90 or 180 observations in the same UTC two-hour bucket, predicts the realized volatility of the next two contiguous completed 2h returns.

The target is `sqrt(r[t+1]^2 + r[t+2]^2)`. It is not a directional trade return. Missing or partial bars are never filled. A gap invalidates the affected lookback or target. The two final target-tail bars can resolve earlier targets and cannot create decisions.

Four variants are exhaustive: q90/L90, q90/L180, q95/L90, and q95/L180. Training freezes strict linear-quantile thresholds, four continuous-control standardizers, UTC bucket coding, and separate 17-column primary/rival OLS fits. Held-out evaluation uses common timestamps and reports primary residual contrast `D_P`, raw-volume-rival contrast `D_R`, and paired `A=D_P-D_R`. The registered uncertainty procedure is 10,000 circular moving-block replicates, primary lengths 12/24, sensitivity lengths 24/48, centered one-sided p-values, and separate four-member Holm families.

Validity is distinct from evidence. Minimum eligible rows are 2500/850/850; both event definitions require 80/40/40 observations. Missing support, rank, continuity, liquidity, finite values, or seasonal history is `invalid`, not negative evidence. Scientifically eligible evidence may be positive or negative. Synthetic success remains uncertified.

The immutable YAML retains separate coverage-selected smoke (`57d9…5a97`) and scientific (`abce…6a97`) plans. Native evaluation also requires the repository materializer's digest-bound receipt proving that every retained aggregate came from 120 consecutive, non-null 1m constituents, plus a dispatcher/evaluator digest over the exact frame values so a same-shape mutation cannot reuse that witness. No lake run or calibration is authorized. The registered calibration has 104 outer replicates and 10,000 inner replicates; the accepted three-outer-replicate diagnostic cannot satisfy precision or qualify calibration. The accepted fixed minimum-norm Newton design uses 8,190 solve histories and 8,190 fresh verification histories per scenario, four damped updates (`1, 1/2, 1/4, 1/8`), 19 functionals per update, and one explicit returned-iterate evaluation. Its 65,936 complete histories exactly meet the accepted cap. The calibration API validates lifecycle receipts and terminal states only; it does not generate a DGP or confer scientific certification.

## Requirement-to-test map

| Requirement | Test |
|---|---|
| Native registration, no orders, tail separation, fresh-instance state | `test_native_strategy_tail_and_repeat_state` |
| Exact four variants and accepted bindings/plans | `test_contract_preserves_frozen_method_and_plans` |
| Runner-issued point-in-time eligibility bound to verified source bytes and immutable contract | `test_eligibility_is_issued_only_for_verified_contract_bound_source`, `test_execute_registered_persists_invalid_failed_and_repeat_isolation` |
| Future-RV target, prior-only seasonality, gap invalidation | `test_prepare_target_and_seasonal_state_are_causal` |
| UTC alignment, positive ordered OHLC, range consistency | `test_prepare_rejects_misaligned_and_invalid_ohlc_rows`, `test_native_strategy_tail_and_repeat_state` |
| Exact 120-minute completeness, value-bound dispatch, runner-issued eligibility, exact variant identity, interface-error propagation, mutation rejection, actual registered contract grid, strategy/evaluator dispatch and four persisted artifacts | `test_registered_synthetic_fixture_uses_eligibility_and_exact_variant_ids`, `test_registered_synthetic_fixture_propagates_interface_errors`, `test_native_contract_runner_executes_strategy_and_evaluator_repeatably`, `test_execute_registered_persists_invalid_failed_and_repeat_isolation` |
| Actual registered-runner OLS, frozen training-state reuse on held-out test, 10,000-replicate bootstrap, Holm, positive and negative evidence for all four variants | `test_held_out_evaluation_reuses_frozen_training_state`, `test_real_estimator_bootstrap_and_holm_terminal_paths` |
| Insufficient support is invalid, not negative evidence | `test_terminal_outcome_classes_are_distinct` |
| Mixed held-out family validity is explicitly invalid | `test_mixed_test_validity_is_invalid_not_exception` |
| Complete accepted synthetic DGP binding; structured calibration success/inconclusive/failure/cap paths, malformed typed-field and non-finite rejection, typed exactly-once scenario/variant IDs, nested collection validation, and three-replicate limitation | `test_contract_preserves_frozen_method_and_plans`, `test_synthetic_calibration_contract_terminal_states_and_caps`, `test_calibration_registered_collections_fail_explicitly`, `test_calibration_nested_receipt_collections_are_total_and_typed` |
| Representation, dispatch, membership, dataset-binding, timestamp, and runner receipt boundaries reject malformed nested JSON before hashing or indexing | `test_peer_receipt_boundaries_reject_malformed_nested_json_without_raising` |

## Validation handoff

- Current author-side focused result: `python -m pytest tests/test_btc_volume_volatility_accepted_method.py -q` completed with 28 passed in 289.78 seconds. This is a non-authoritative diagnostic, not a review or certification receipt.
- Current accepted evidence SHA-256 is `57d90ff8554b226ff8685928d2738b7690af4a931bfc548b3cc534c18dae99b4`; portable retained engineering document digest `39a27ae4529be412b5c222a555adf4feb9896e022ba40e233ffbec5379d2f65a` and retained patch SHA-256 `b54009f039f27eae07d2db22c160688e177645cfde39352517961fa753a63224` were verified before restoration onto source commit `d232ee753f967760ce3d753abff607ee2232ce62`.
- Independent same-code review and governed full-repository validation remain external receipts. This document does not claim either approval.
- The complete repository suite is reserved for the governed outer validator and is not run in this coding turn.

All fixtures are synthetic. A passing engineering test is neither a market-data backtest nor calibration or scientific qualification.
