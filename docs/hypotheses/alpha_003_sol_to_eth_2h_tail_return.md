# SOL-to-ETH delayed information diffusion

This observation-only Tier2B study implements the admitted question verbatim. At each completed 2h boundary it consumes the exact ordered adaptive outputs for SOL and ETH returns, ETH volatility shifted one complete 2h bar, and both completed-bar USD quote volumes. It constructs only the future contiguous 4h ETH close-to-close outcome after the causal decision record is fixed.

Tail thresholds are fitted from the train partition only after the joint USD 1,000,000 liquidity gate and the 4,000-row requirement. Controls are fitted on train only. Validation selects one of two frozen tail percentiles; the held-out test is opened once. Both validation and held-out evaluation require at least 30 qualifying tail events. The lag rival is scored on the identical current-tail timestamps. Four-hour purge and embargo boundaries prevent labels crossing partitions, and a deterministic circular block bootstrap handles overlapping outcomes.

The evaluator subtracts the registered 9 bps round-trip cost and requires the result to remain positive after a second 9 bps stress. It retains positive, negative, invalid, and failed outcomes. The native strategy emits validation observations only and has no capital, order, promotion, or self-approval authority.
