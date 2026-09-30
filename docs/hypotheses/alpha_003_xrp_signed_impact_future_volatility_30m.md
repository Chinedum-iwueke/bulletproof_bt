# XRP signed impact and next-3h volatility

This Tier2B research-only hypothesis preserves the admitted question verbatim. At each completed 30-minute boundary, it consumes the five ordered, digest-bound adaptive outputs. Upper and lower tails are computed separately from the signed-impact distribution using only the preceding 1,440 completed observations. No directional order is implied or emitted.

The evaluator estimates positive- and negative-shock indicators jointly with absolute current return, prior realized volatility, and log completed quote volume. Variant selection uses only the chronological validation partition; the test partition is opened once. Six bars are purged and embargoed at partition boundaries, and Newey–West HAC covariance with lag five accounts for overlap among six-bar targets.

Decisions with malformed provenance, non-UTC or non-contiguous timestamps, inconsistent signed impact, negative volume, quote volume below USD 1,000,000, or an incomplete six-bar future target are invalid. Fewer than 40 held-out observations per sign fails support. Positive classification requires both conditional shock effects to be positive, a 95% asymmetry interval excluding zero, and a positive minimum effect after exactly twice the registered 9 bps cost hurdle. Positive, negative, invalid, and failed outcomes are retained.

The implementation has no capital, order, promotion, or self-approval authority.
