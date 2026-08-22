# Atomic Run Bundle

BT-003 composes the existing Bulletproof artifact manifest, required-artifact gate,
truth validation, deterministic serializers, BASE-002 identity and BT-002 dataset
identity into one publishable run boundary. It does not replace engine artifacts.

## State machine

```text
run artifacts -> staging -> validated -> atomic rename -> finalized
                       \-> failed attempt receipt
```

Finalization copies canonical textual artifacts into a same-filesystem staging
directory, assigns every artifact a source-byte digest, publishable-byte digest,
semantic digest, media type and structural schema identity, then atomically renames
the directory to `bundles/<bundle_digest>`. A crash or validation error cannot expose
a partial final bundle. Failures remain under `failures/` with a redacted category and
message.

The bundle digest uses stable lineage plus semantic artifact digests. Supported
volatile identifiers such as run IDs, generated timestamps and output directories do
not change it. Exact published bytes remain individually digest-bound. Therefore two
semantically identical runs can resolve to one bundle while byte provenance is still
auditable.

Legacy `run_manifest.json` is the sole compatibility normalization: absolute runtime
and data paths become `run://current` and `dataset://registered-snapshot` in the
derived bundle, while the original byte digest remains recorded. Any other absolute
protected path, suspected secret, unsupported binary artifact, corrupt structured
file or unregistered structural schema fails closed.

## Finalize and publish

The lineage JSON must bind the repository commit, code digest, BT-002 snapshot UUID
and digest, prospective specification digest, environment digest and attempt number.

```bash
python scripts/finalize_run_bundle.py \
  --run-dir outputs/runs/<run> \
  --bundle-root outputs/run-bundles \
  --lineage /derived/lineage.json
```

Add `--publish-api-url` to register the finalized run through Hermes' existing
idempotent canonical evidence endpoint. Publication requires
`SWARM_ORCHESTRATOR_TOKEN`; the token is sent only as an authorization header and is
never stored in the bundle or receipt.

## Authority boundary

Finalization proves artifact integrity, lineage and publication identity. It does not
approve a hypothesis, certify profitability, promote a candidate, place orders or
grant capital authority. BT-009 owns governed orchestration into this boundary;
BT-008 owns broader laboratory publication.
