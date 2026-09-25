# ETH liquidity displacement and BTC residual return

The claim is the exact admitted question: an extreme, point-in-time ETHUSDT 15m log return divided by that completed bar's USD quote volume may predict a nonzero BTCUSDT close-to-close residual return over the next contiguous 60 minutes.

The fixed Bybit basket is BTCUSDT (target) plus ETHUSDT (predictor). Strict UTC 1m rows form left-closed, left-labeled 15m bars only when all 15 constituents exist. Both quote-volume sums must be nonnegative and at least USD 1,000,000. No interpolation, forward fill, partial bar, dynamic basket, or proxy statistic is allowed.

At decision time, the strategy consumes the six ordered fields and exact representation-plan digest declared in the YAML. The ETH absolute displacement threshold uses 35,040 strictly prior completed bars. The BTC volatility control uses 96 BTC returns ending one completed bar before the signal; its source-end timestamp is checked at runtime. Missing provenance means no decision.

The evaluator fits the BTC 60m-return residual model on training data only, using an intercept, the contemporaneously completed BTC 15m return, and prior-only BTC realized volatility. It uses chronological 60/20/20 partitions with 60m purge/embargo and deterministic one-to-one matching without replacement on both controls. Only the highest positive validation variant opens the test partition once. Positive, negative, invalid, and failed outcomes are retained.

The four frozen variants cross 97.5%/99% ETH displacement tails with continuation/reversal orientation. Falsification requires at least 50 held-out extremes, 30 matched controls, a 95% interval excluding zero in the selected direction, and a positive doubled-cost effect. Registered costs are 6 bps taker fee, 2 bps slippage, 1 bp spread, one-bar delay, and 2x stress.

Classic-engine execution is authoritative. Entries use engine risk-at-stop sizing at 0.5% intended risk, a fixed 3% stop, 25% per-position and gross-notional caps, 1x maximum leverage, no pyramiding/flips, and an explicit 60m close-only exit. Decisions, fills, trades, equity, state, risk utilization, representation provenance, and scientific outcomes remain logged. OHLCV-only input or inactive membership produces no entry. This research has no capital, order, promotion, or self-approval authority.
