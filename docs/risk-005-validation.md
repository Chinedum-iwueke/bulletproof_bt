# RISK-005 validation

RISK-005 is implemented by Bulletproof as the authoritative quantitative producer. It
binds one order intent to one causally available, versioned state snapshot and exact
RISK-002/003/004 and EXEC-001/004 receipts. Decisions are deterministic `allow`,
`deny`, or narrowly scoped `reduce_only_exit` outcomes.

External demo/live submissions fail closed without a current, untampered receipt whose
order fields and state version exactly match. Cancellation and the independent emergency
freeze/close path do not depend on this service.

The qualification pilot uses deterministic fixtures and has no capital or order authority.
It does not claim that a real candidate is admitted or that any exchange order was sent.
