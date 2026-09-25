# ALPHA-003 cross-sectional liquidity-dispersion reversal

This research-only native contract preserves the admitted question exactly. At each causal 5m boundary it uses a frozen BTCUSDT/ETHUSDT/SOLUSDT basket, completed left-closed 1m inputs, current completed-5m log returns, and quote-volume percentile ranks estimated from the preceding 288 complete 5m bars only.

The evaluated outcome is either the future winner-minus-loser return (symmetric reversal) or the future winner leg (winner-only) through six contiguous complete future 5m bars. The evaluator subtracts the registered costs, matches liquidity-normal controls on dispersion and prior-only 30m basket volatility, calculates a 95% mean-effect interval, checks directional support and repeats the gate at doubled costs. Test-partition end bounds apply to the complete target path.

The classic strategy consumes the exact ordered adaptive fields, plan digest, and decision timestamp on every basket member. It intentionally emits no order signals: the classic engine cannot atomically accept two independently risk-checked legs, and partial submission would change the scientific question into an outright trade. Scientific observations—including invalid and failed gates—are retained by the native evaluator. The package grants no capital, order, promotion, or approval authority.
