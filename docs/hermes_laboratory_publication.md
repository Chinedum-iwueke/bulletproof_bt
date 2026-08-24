# Hermes Laboratory Publication

BT-008 joins a finalized Bulletproof run bundle to the existing Hermes trial
registry. It does not create a second experiment registry and it never treats
an LLM narrative as evidence.

The bridge validates the complete BT-003 bundle locally, publishes its
canonical run object, and submits the registered trial/result identities plus
repository, dataset, market-model, representation, bundle, and manifest
digests. Hermes atomically records canonical result, independent reviews,
decision, and replay dossier evidence. Negative, null, invalid, and
inconclusive results follow exactly the same path as positive results.

Graph and retrieval projection receipts are confirmed before the local
Bulletproof memory projection is written. A retry resumes from the durable
Hermes state. The SQLite `research_memory_publications` table is a derived,
idempotent consumer ledger; Hermes canonical evidence remains authoritative.
Conflicting bundle, request, or publication identities fail closed.

Use `scripts/publish_laboratory_bundle.py publish` to create or resume a
publication. If it returns `awaiting_projections`, rebuild the canonical Hermes
graph and retrieval projections, submit their exact current digests with
`confirm-projections`, then rerun `publish`. The second pass writes the local
memory receipt and completes the saga.
