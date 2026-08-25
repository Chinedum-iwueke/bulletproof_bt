# LIVE-001 gated-canary validation

## Decision

The source-side capital gate is implemented, but LIVE-001 is not operationally
complete. No demo or live order was submitted by this change. Capital remains
blocked until the external prerequisites below produce genuine evidence and a
separate, expiring founder approval binds that evidence to one exact canary.

## Implemented boundary

- `live_broker` refuses to construct an exchange adapter or resolve venue
  credentials until it validates a root-owned, mode `0600` authorization bundle.
- The bundle digest binds one venue, symbol set, time window, loss envelope,
  order envelope, rollback action, non-allocating PORT-001 candidate, demo
  qualification, live-connector certification, operational readiness, and a
  kill/rollback rehearsal.
- Runtime limits may be tighter than the signed plan but never wider.
- Live canary controls enforce order quantity/notional/count, open order and
  position counts, gross notional, daily/session loss, session duration, and
  wall-clock market-data freshness.
- Binance now requires its private stream to be ready at startup, matching the
  fail-closed Bybit policy.
- Authorization receipts contain digests and scope only. Venue credentials are
  neither accepted nor emitted by this contract.

## Adversarial validation

The focused suite covers pending and expired approvals, wrong rollback,
unauthorized scale authority, widened runtime limits, disabled canary controls,
wrong file mode, stale/future market data, loss and gross-notional breaches, and
expired sessions. Existing startup reconciliation, kill, and C5/C7 safety tests
remain in the validation set.

## Required evidence before any capital test

1. Complete `DEMO-001` against the selected venue's demo/testnet account.
2. Certify the production connector, including private-stream behavior.
3. Pass forced outage, partial-fill, stale-data, key-rotation/revocation,
   reconciliation, restart, independent kill, and rollback drills.
4. Produce current M8 operational-readiness and PORT-001 non-allocation dossiers.
5. Install dedicated no-withdrawal, least-privilege credentials outside source
   control and outside the authorization bundle.
6. Obtain a separate founder/risk approval for the exact plan digest and short
   execution window.
7. Execute one serialized micro-live canary, flatten and reconcile it, then retain
   the signed live dossier. That dossier grants no scaling authority.

Until all seven steps are evidenced, the correct LIVE-001 state is
`blocked_before_capital`, not `complete`.
