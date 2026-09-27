# ETH relative underperformance and liquidity reversal

This Tier2B research-only experiment answers the admitted question verbatim. At each completed 1h boundary it uses six contiguous, complete historical hours for the ETH relative-return and quote-volume state. The six-hour outcome begins at that boundary. Missing minutes suppress the affected hour; no interpolation is permitted.

Residual coefficients are fitted only on the purged 60% training partition. All four frozen variants are evaluated on validation with dependence-aware circular-block-bootstrap confidence intervals, support, rival-control, nondeclining-liquidity, and doubled-cost gates. The test partition remains closed unless a validation variant passes every gate; only the deterministic winner opens it, once.

The native classic-engine strategy consumes the exact ordered adaptive output fields and provenance for all three basket members. It emits only a validation observation and has no capital, order, promotion, or self-approval authority.
