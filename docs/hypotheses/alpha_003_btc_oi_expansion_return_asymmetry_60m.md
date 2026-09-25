# ALPHA-003: BTC OI-expansion return asymmetry

## Claim

Does point-in-time open-interest expansion accompanying a completed 15m BTCUSDT return predict direction-dependent BTCUSDT close-to-close return over the next 60m, after controlling for prior return, realized volatility, funding-cycle position, and liquidity?

## Frozen design

The mechanism distinguishes price displacement accompanied by new leveraged positioning from displacement caused by position closure. Both continuation and reversal are evaluated; no sign is assumed. The sole admitted input is the immutable Bybit BTCUSDT 1m research panel build `fbb81c42-953b-42fb-8fe1-89c75b45e1aa` over 2025-05-01 through 2026-05-01.

Only 15 contiguous 1m rows form a completed 15m bar. The frozen adaptive compiler produces, in order, `btc_15m_log_return`, `btc_past_60m_log_return`, `btc_past_6h_realized_volatility`, and `btc_15m_quote_volume`. Runtime use requires the exact plan digest, exact ordered field declaration, and a representation decision timestamp equal to the engine timestamp. Open interest is backward-asof joined only when `oi_source_ts <= decision_ts`. Missing or malformed values produce no decision.

The setup requires positive point-in-time 15m log OI change above its train-fitted percentile, absolute completed return above its train-fitted percentile, and completed 15m quote volume of at least USD 1,000,000. Direction is the frozen `continuation` or `reversal` grid value. Entry is submitted at the causal decision boundary for next-bar engine execution. Exit is a fixed 60m wall-clock time exit with a frozen 3% stop from the decision close.

## Evaluation and falsification

The eight-variant grid is two OI percentiles (0.80, 0.90), two absolute-return percentiles (0.50, 0.75), and two directions. Decision rows are split chronologically 60/20/20 with 60m purge/embargo. Thresholds fit on train, validation directional effect selects the preregistered variant, and the test partition is opened once. Matching is deterministic and without reuse on prior 60m return, past-6h realized volatility, UTC funding-cycle position, and log quote volume.

The hypothesis fails unless each return sign has at least 30 held-out matched treated observations, the held-out 95% effect interval is positive, the doubled-cost effect remains positive, and the paired actual-minus-one-extra-15m-lag OI placebo interval is positive on identical treated/control timestamps. Incomplete predictor history, non-contiguous targets, schema gaps, missing liquidity/OI, or future OI source times are retained as invalid observations. Positive, negative, failed, and invalid outcomes are retained. Logged metrics include the validation/test effects, confidence intervals, paired placebo contrast, doubled-cost result, support, and maximum drawdown.

## Authority and execution truth

Risk-at-stop sizing requests 0.5% equity risk, with a 25% per-position and gross-notional cap, 1x maximum leverage, no pyramiding or flips, and clipped-risk truth logging. Registered costs are 6 bps taker fee, 2 bps slippage, 1 bp spread, and one-bar delay; robustness doubles costs. The classic event-driven engine alone owns fills, costs, sizing, margin, accounting, stops, and exits. This package grants no capital, order, promotion, production, or self-approval authority. OHLCV-only input and unavailable rich fields produce no decision.
