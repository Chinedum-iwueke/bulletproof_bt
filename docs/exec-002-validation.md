# EXEC-002 validation

EXEC-002 unifies existing Bulletproof order-book, trade, mark/index basis,
funding, open-interest and liquidation evidence under a typed microstructure state.
Every field is explicitly observed, inferred or unavailable, with source-event IDs,
limitations and uncertainty.

Crossed books fail closed. Missing levels and liquidation history remain unavailable,
not zero. Venue resets invalidate prior state. Stale funding and open interest are not
reported as current observations. Liquidation proxies are labelled inferred and never
masquerade as exchange observations.

Hermes registers and verifies the immutable model and receipt only. It does not derive
microstructure state, authorize execution or place orders.
