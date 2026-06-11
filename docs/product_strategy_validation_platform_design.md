# Product Strategy Validation Platform Design

Generated from office-hours review on 2026-05-15.

## Executive Summary

This document corrects the earlier greenfield assumption. There is already a real web app repo:

- `invariance_research`: the existing Next.js + TypeScript SaaS application.
- `bulletproof_bt`: the existing Python strategy research, diagnostics, backtesting, artifact, and orchestration engine.

The product should not be rebuilt from scratch. The existing Strategy Robustness Lab in `invariance_research` should be treated as the incomplete product surface, and `bulletproof_bt` should be treated as the engine and research substrate that powers it.

The May 2026 commercial wedge was narrow:

> Upload strategy evidence. Get a hostile, auditable validation report.

That wedge produced useful infrastructure, but early user testing exposed a demand ceiling: upload-only software cannot reliably infer enough strategy truth from random, under-specified artifacts. A serious trader's edge lives in the hypothesis, rule logic, data context, execution assumptions, parameter choices, experiment lineage, and failed variants. A CSV or broker export is usually an output artifact, not the research system.

The June 2026 implementation thesis is now:

> Move from upload-only validation to a full research pipeline product, while keeping the Strategy Robustness Lab as the audit, import, report, and snapshot subsystem.

Approach A has been demoted from product center to product infrastructure:

> Upload evidence, validate existing results, produce report snapshots, and route incomplete cases into deeper research.

Approach B is now the product center:

> A continuous research pipeline that turns raw trading intuition into testable hypotheses, queues experiments, runs them through `bulletproof_bt`, interprets results, remembers the lineage, and recommends the next falsification step.

The product should still grow from the existing app, not around it. The difference is that the app should now be reorganized around Research Programs, Hypotheses, Experiment Queues, Runs, Verdicts, and Research Memory instead of only uploaded analyses.

## June 2026 Demand Learning: Approach A Is Infrastructure, Not The Company

After rolling out Approach A and getting early users to test it, the key learning is blunt:

> There is no durable company in upload-only strategy validation.

Parts of Approach A are valuable:

- report snapshots
- exportable validation memos
- share-safe proof reports
- artifact intake
- prop feasibility from trade histories
- evidence and assumption ledgers
- diagnostic honesty
- admin, billing, storage, worker, and queue infrastructure

But upload-only validation is not enough because the product receives artifacts without the causal system that produced them. The user often cannot supply enough context for a serious decision:

- strategy rules may be absent
- signal logic may be informal
- execution assumptions may be missing
- market context may not be aligned
- parameter history may not exist
- failed variants may be unknown
- the user may upload a random broker export and expect institutional inference

The right product is the process that produced the artifact, not only the artifact review.

### New Product Thesis

Invariance Research should provide the research pipeline built inside `bulletproof_bt`, surfaced through the SaaS app and assisted by AI agents:

> Describe a market intuition. The system turns it into a falsifiable hypothesis, converts it into a strategy spec, queues experiments, runs them continuously, attacks the results, stores the lineage, and tells the user what to test next.

This makes the product a research operating system from day one, but with a narrow first workflow:

> Hypothesis-to-Experiment Workbench for systematic traders.

### New Magic Moment

The magic moment is no longer:

> I uploaded a CSV and got a report.

The magic moment is:

> I wrote a rough trading idea, the system asked for missing assumptions, converted it into a valid strategy spec, ran the first experiment overnight, showed why it failed, and queued the next five better tests.

### Positioning Shift

Old positioning:

> Upload strategy evidence. Get hostile validation.

New positioning:

> Turn every trading idea into a disciplined research program.

Sharper product line:

> From trading intuition to falsified strategy research, without losing the thread.

Do not claim that the system finds alpha every day. The honest promise is research throughput, falsification discipline, memory, and compounding experiment quality.

### Approach A Remains In Scope As A Subsystem

Approach A should remain because it already built important machinery:

- artifact ingestion becomes import mode
- Strategy Truth Room becomes audit/report mode
- report snapshots become durable research artifacts
- Share Room becomes recipient-safe publication
- Research Desk becomes escalation and expert review
- prop evaluation remains a trade-history feasibility workspace
- billing/admin/storage/worker infrastructure remains the SaaS foundation

But the default product should no longer be "upload and inspect." The default product should be "start or continue a research program."

## Office-Hours Position

The product should be judged by demand, not architecture.

The sharpest early customer is not "anyone who backtests." The sharpest early customer is someone who needs credible strategy evidence:

- a serious independent trader deciding whether to keep allocating time or capital
- a strategy seller or educator who needs a hostile third-party report
- a systematic trader who needs artifact-backed diagnostics
- an emerging manager or prop-style operator screening a strategy
- a crypto-native researcher whose edge depends on execution and regime context

The status quo is not just another dashboard. The status quo is:

- hand-curated spreadsheets
- screenshots of equity curves
- broker exports with missing assumptions
- loosely written strategy claims
- private notebooks that cannot be audited later
- manual PDF writeups that overstate confidence
- credibility rituals in private communities instead of reproducible validation

The immediate product should not ask users to adopt a whole research OS. It should ask them for an artifact they already have and return a report they would actually share.

## Bloomberg Terminal Lessons For This Product

The Bloomberg Terminal is world-class because it is not a single feature. It is a connected institutional work surface: market data, news, analytics, research, alerts, messaging, trading workflow, risk tools, Excel/API extraction, support, and shared professional context live behind one trusted interface. Bloomberg's own product catalog groups the Terminal with Research, Portfolio & Risk Analytics, Charts, Monitors & Alerts, Collaboration, News, Data Connectivity, Trading, Risk, and Compliance. That is the lesson to adapt, not the exact product.

Pricing is high because the value is not just "data access." Public 2026 budgeting estimates commonly place a standard annual Terminal subscription in roughly the $24,000-$32,000/user range, with hardware, premium data, API, analytics, support, and enterprise modules increasing total cost. Treat those numbers as market estimates, not Bloomberg list-price gospel. The durable point is that institutions pay when a product becomes a trusted daily operating system with data, workflow, network effects, auditability, and extraction into existing processes.

Source note: Bloomberg's public product pages describe the Terminal as an integrated data, news, research, analytics, collaboration, trading, portfolio analytics, risk, compliance, alerting, and data-connectivity environment. Bloomberg does not publish a simple public list price; the pricing numbers here are third-party 2026 budgeting estimates and should be used directionally.

For this product, the translation is:

> Bloomberg sells market command plus institutional workflow.
> Invariance Research should sell strategy truth command plus institutional validation workflow.

### Bloomberg Pattern Translation

| Bloomberg capability | Why users pay | Strategy Truth Room adaptation |
| --- | --- | --- |
| Trusted cross-asset data | one source of truth for decisions | evidence lake: uploaded trades, broker exports, OHLCV, benchmark, funding/OI/liquidations where available |
| Analytics functions | fast answers to hard questions | validation functions: execution drag, rare-trade dependence, regime dependency, Monte Carlo survival, ruin, parameter fragility |
| News and research context | explains market state and why results changed | research memory and market-state notes explaining when a strategy works, fails, or becomes unsupported |
| Terminal commands | expert speed and repeat workflows | command palette and saved questions: compare runs, show missing evidence, explain verdict, find similar failures |
| Excel/API extraction | fits institutional workflows | PDF/Markdown/JSON exports, future API, report snapshots, parquet/CSV evidence appendices for teams |
| Messaging/network | coordination and professional trust | Share Room, Research Desk handoff, reviewer addenda, report comments, client-safe validation packets |
| Alerts | users act when conditions change | evidence alerts: report superseded, share viewed, diagnostic unlocked, edge killed by costs, regime dependency found |
| Audit trail | defensible records for committees | immutable snapshots, hashes, manifests, assumption ledger, unsupported-claim ledger, access logs |
| Support/help | power users can understand the machine | "Why this verdict?", "What unlocks this?", "What changed?", "What should I upload next?" explanations |

### Office-Hours Demand Decision

Do not turn Approach A into a Bloomberg clone. That is how the wedge dies.

Approach A should absorb only the Terminal patterns that make a single uploaded strategy report more valuable this month:

- connected evidence objects, not isolated pages
- reusable validation functions, not generic charts
- report snapshot as the durable demand object
- command-palette speed for serious users
- export parity into existing workflows
- recipient-safe sharing with access state
- alerts around evidence changes and report trust
- help/explain affordances that tell the user why a diagnostic is limited

Everything else belongs in full ambition until demand justifies it.

### Approach A Additions From Bloomberg

The following additions are now in scope for the sellable Approach A product because they strengthen the upload-validation wedge without exposing the full research terminal:

1. **Validation Command Palette**
   - A keyboard-accessible app command surface for completed analyses.
   - Initial commands:
     - explain verdict
     - show missing evidence
     - compare to previous run
     - open report snapshot
     - create share link
     - request Research Desk review
     - show unsupported claims
     - show diagnostics blocked by artifact vs plan
   - This is the Bloomberg command-function pattern adapted to strategy validation. It is not a public terminal UI.

2. **Evidence Alert Center**
   - Alerts for events that change report trust:
     - snapshot generated
     - snapshot superseded
     - share viewed
     - share expired/revoked
     - export completed/failed
     - diagnostic newly unlocked by richer artifact
     - high-materiality assumption emitted
     - unsupported claim blocks report confidence
   - Alerts should be evidence-state events, not marketing notifications.

3. **Connected Case File**
   - Every analysis should expose a left-to-right evidence chain:
     - artifact files
     - accepted facts
     - assumptions
     - diagnostics
     - verdict
     - proof report
     - share room
     - Research Desk packet
   - Users should never wonder where a report statement came from.

4. **Explain Layer**
   - Each verdict and limitation should have a "why" path:
     - why this verdict?
     - why this diagnostic is limited?
     - what input unlocks it?
     - what changed since the previous snapshot?
     - what evidence would rescue the claim?
   - This makes honesty usable rather than merely cautious.

5. **Workflow Exports**
   - Keep PDF/Markdown/JSON exports.
   - Add future-ready export slots for CSV/parquet evidence appendices and report API.
   - Do not export raw trade files in public Share Room by default.

### Full-Ambition Bloomberg Adaptations

These are valuable, but they should not ship in first-launch Approach A:

- multi-run strategy workspace with terminal-style command functions
- tenant-scoped research memory search
- cross-run failure clustering
- cross-strategy regime intelligence
- watchlists for strategies, claims, regimes, and evidence states
- portfolio of strategy reports
- team chat/comments attached to report snapshots
- API access for institutional report ingestion
- data terminal for market-state panels, funding/OI/liquidations, benchmark libraries, and instrument master
- full research graph: claim to hypothesis to run to state bucket to verdict to next experiment
- enterprise compliance archive for validation work

### Internal Research Loop Additions

The `bulletproof_bt` research daemon and memory layer are already the internal intelligence substrate. Keep improving it, but do not make users adopt it before the upload-validation wedge works.

Useful internal additions:

- **Research Command Layer**: status, queue, latest verdict, compare experiments, explain failure, find similar state buckets, promote/scrap recommendation.
- **Terminal-Grade Intelligence Cards**: Hypothesis, Run Quality, Regime Dependency, Execution Drag, Failure Cause, Verdict, Similar Runs, Next Experiment.
- **Data Provenance Board**: panel version, exchange/source coverage, funding/OI/liquidations availability, gaps, and known bad intervals.
- **Failure Memory Index**: query prior strategies killed by costs, rare-trade dependence, parameter cliffs, adverse regimes, or insufficient evidence.
- **Research HELP Equivalent**: explain any daemon verdict, what artifact drove it, what data was missing, and what next experiment has the highest information value.
- **Moat Discipline**: every internal research card should eventually be convertible into a share-safe product object, but internal alpha notes must never leak into tenant/customer reports.

## Current Repo Reality

### `invariance_research` Exists And Is The Product App

Observed from repo docs and code:

- public authority site exists
- `/robustness-lab` marketing surface exists
- authenticated `/app` workspace exists
- `/app/new-analysis` upload flow exists
- `/api/uploads/inspect` exists
- `/api/analyses` exists
- queue-backed analysis jobs exist
- export jobs exist
- JSON, Markdown, and PDF export paths exist
- plan matrix exists: `explorer`, `professional`, `research_lab`, `advisory`
- Stripe checkout, portal, and webhook processing exist
- admin/ops console exists
- health checks exist
- persistence abstraction exists with SQLite now and Postgres-ready modules present
- local object storage abstraction exists
- benchmark manifest/data plumbing exists
- Python bridge exists through `scripts/run_bulletproof_engine.py`
- current engine seam calls `bt.run_analysis_from_parsed_artifact`

Key files in the web app:

- `src/app/robustness-lab/page.tsx`
- `src/app/app/new-analysis/page.tsx`
- `src/components/forms/new-analysis-intake.tsx`
- `src/app/api/uploads/inspect/route.ts`
- `src/app/api/analyses/route.ts`
- `src/lib/server/services/upload-intake-service.ts`
- `src/lib/server/services/analysis-service.ts`
- `src/lib/server/engine/bulletproof-runner.ts`
- `src/lib/server/adapters/bulletproof/map-engine-analysis-record.ts`
- `src/lib/contracts/analysis.ts`
- `src/lib/server/entitlements/plans.ts`
- `src/lib/server/persistence/postgres-schema.ts`
- `src/lib/server/workers/analysis-worker.ts`
- `src/lib/server/workers/export-worker.ts`

Implication:

The product is not pre-product. It is an incomplete product with real surfaces, contracts, workflows, billing, and ops. The next plan should be a hardening and expansion plan, not a first-build plan.

### `bulletproof_bt` Is The Engine And Research Substrate

Observed:

- deterministic event-driven backtest core
- public `run_backtest` and `run_grid` APIs
- `bt.run_analysis_from_parsed_artifact` seam for the web app
- SaaS service under `src/bt/saas/service.py`
- typed SaaS models under `src/bt/saas/models.py`
- artifact discipline with trades, manifests, benchmark artifacts, performance, robustness, and R metrics
- execution modeling with fees, slippage, spreads, profiles, intrabar assumptions, and cost attribution
- hypothesis contract system under `src/bt/hypotheses`
- research data layer for OHLCV, mark/index, funding, open interest, liquidations, and panels
- orchestration layer with SQLite research database, daemon, queues, experiment pipelines, verdict artifacts, and research memory
- research memory modules for trades, state buckets, candidates, recommendations, and reports
- internal FastAPI/Jinja dashboard that is useful for local research but should not become the public web app

Key files:

- `src/bt/saas/service.py`
- `src/bt/saas/models.py`
- `src/bt/api.py`
- `src/bt/core/engine.py`
- `src/bt/execution/*`
- `src/bt/metrics/*`
- `src/bt/benchmarks/*`
- `src/bt/hypotheses/*`
- `src/bt/research_data/*`
- `orchestrator/research_memory/*`
- `orchestrator/research_daemon.py`

Implication:

The Python repo should not become a web app. It should become the increasingly stable engine, artifact, validation, and research-memory substrate called by `invariance_research`.

## Product Thesis

Most trading validation tools help users produce a more attractive result. This product should help users find the point where their strategy stops deserving belief.

The brand should be comfortable saying:

> We cannot support that conclusion from the data provided.

That sentence is not a failure state. It is the product.

The user promise:

> Find out whether your trading edge is real before the market does.

The wedge:

> Upload strategy evidence. Get a hostile validation report.

The long-term ambition:

> A strategy research operating system where every market claim becomes a falsifiable hypothesis, every run becomes auditable evidence, and every failure improves future research.

## Demand Hypothesis

This plan is still a hypothesis until real users pay, share reports, or build workflow around it.

Strong demand evidence would look like:

- a trader pays for a report even when the verdict is negative
- a strategy seller shares the report with buyers
- an allocator asks for reports on multiple strategies
- a user uploads a better artifact to unlock a missing diagnostic
- a user asks for an advisory follow-up after a limitation is surfaced
- a team wants org-level retention and audit history
- a user returns after a rejected strategy with a revised hypothesis

Weak evidence:

- users say the product is interesting
- users like the public site
- users upload one toy CSV and leave
- users only want the product to confirm their existing belief
- users ask for live trading before they trust validation

The first release should be instrumented to learn which of these is true.

## Premises

1. `invariance_research` is the product app and should remain the primary user-facing repo.
2. `bulletproof_bt` is the engine and research substrate and should expose stable seams to the web app.
3. The existing Strategy Robustness Lab is real but incomplete.
4. Early users showed that upload-only validation is too weak to be the product center.
5. The fastest credible path is now to productize the `bulletproof_bt` research pipeline behind the existing SaaS shell.
6. Approach A infrastructure remains useful as import, audit, proof-report, Share Room, Research Desk, and billing/ops substrate.
7. Claim-first research features should now be introduced as the default workflow, not only as later extensions.
8. Research memory is the eventual moat, but tenant safety and auditability must come before cross-user intelligence.
9. Product honesty matters more than feature breadth.

## Strategic Approaches Considered

### Approach A: Harden The Existing Strategy Robustness Lab First

Summary:

Use the existing `invariance_research` app as the product surface. Deepen the upload-to-analysis-to-report path, tighten the Node/Python seam, improve diagnostic truthfulness, and convert the current Lab into a saleable validation product.

Effort: Medium

Risk: Medium

Reuses:

- `invariance_research` public site and `/robustness-lab`
- authenticated `/app` workspace
- upload inspection and eligibility system
- analysis queue and worker model
- `bt.run_analysis_from_parsed_artifact` bridge
- `map-engine-analysis-record.ts`
- report and export pipeline
- entitlements, billing, and admin
- `bulletproof_bt` SaaS service and artifact diagnostics

Pros:

- ships fastest because the app already exists
- reduces product risk before expanding architecture
- lets real users validate the wedge
- preserves existing billing, admin, auth, upload, and worker work
- creates a clean path for advisory/report revenue
- keeps the product concrete: upload evidence, get verdicts

Cons:

- current architecture carries transitional SQLite/local-storage assumptions
- current report/PDF output is not yet premium enough for high-trust sale
- current diagnostics depend heavily on artifact richness
- claim-first hypothesis flows are deferred
- research memory remains mostly internal for now

Recommendation:

Historical May 2026 recommendation: choose Approach A first because it was the best path from current repo reality to real product evidence. June 2026 user testing superseded this as the product center. Keep Approach A as import/audit/report infrastructure, not as the main company direction.

### Approach B: Claim-First Research OS

Summary:

Make the core product object a Research Program rather than an uploaded artifact. Users begin with a market intuition or claim, turn it into falsifiable hypotheses, generate strategy specs, queue experiments, run them through `bulletproof_bt`, interpret results, and track lineage.

Effort: Large

Risk: Medium-high

Reuses:

- `bulletproof_bt` hypothesis contracts
- orchestrator experiment pipeline
- research memory schema
- research data panels
- existing diagnostic/report pages after adaptation
- Strategy Research Terminal cards and command layer
- current SaaS auth, billing, workspace, workers, storage, exports, and admin ops

Pros:

- strongest long-term differentiation
- aligns with the deepest research culture in `bulletproof_bt`
- creates a durable data model for lineage, failures, and follow-on research
- better supports systematic traders and research teams
- solves the real demand problem found by early Approach A users: users need a process, not only a post-hoc artifact review
- creates a compounding loop where every run improves memory and the next experiment

Cons:

- more complex than upload-only validation
- requires new product objects in the web app: claims, hypotheses, experiments, validations, forks
- needs careful UX to avoid feeling like homework
- requires guarded AI assistance so generated specs do not violate engine contracts
- requires stronger tenant isolation and job orchestration

Recommendation:

Lead with a narrow version of this. The first Approach B product is not the full institutional research OS. It is the Hypothesis-to-Experiment Workbench: raw English intuition to approved strategy spec to queued experiments to interpreted verdict to memory-backed next tests.

### Approach C: Advisory / Research Desk First

Summary:

Use the app as intake for paid human or expert-assisted validation. The internal team reviews artifacts, runs deeper engine workflows, annotates findings, and delivers polished reports.

Effort: Medium

Risk: Medium

Reuses:

- existing contact and research-desk funnel
- admin console
- report/export pipeline
- failed-job and rerun controls
- `bulletproof_bt` orchestrator and internal dashboard concepts

Pros:

- can generate revenue before full automation
- reveals what serious users actually ask for
- creates high-quality report templates and review rubrics
- gives a path for incomplete diagnostics to become consultative upsell

Cons:

- does not scale without operations discipline
- can distract from product automation
- requires clear boundaries around financial advice
- report quality must be high immediately

Recommendation:

Use this as a companion to Approach A, not a replacement. The Lab should produce automated first-pass validation, then route serious cases to Research Desk.

## Recommended Path

Choose Approach B as the product center, using Approach A as infrastructure.

The product should advance in this order:

1. Preserve the existing Lab as audit/import/report mode.
2. Add Research Programs as the main workspace object.
3. Add plain-English idea intake and clarification.
4. Convert ideas into versioned hypothesis specs.
5. Generate engine-safe strategy specs with human approval gates.
6. Queue and run experiments continuously through `bulletproof_bt`.
7. Interpret results into verdicts, failure causes, and next experiments.
8. Persist tenant-scoped research memory.
9. Use report snapshots, exports, Share Room, and Research Desk as the proof and escalation layer.
10. Expand toward the full claim-first Research OS after the pipeline loop is reliable.

## CEO Review Scope Additions

Generated by `/plan-ceo-review` in selective-expansion mode on 2026-05-15.

Historical note: this section was written when Approach A remained the baseline. The additions are still useful because they strengthen the Lab as an import/audit/report subsystem, but the June 2026 product center is Approach B: Research Pipeline First.

### 1. Evidence Sufficiency Ledger

The product needs a first-class Evidence Sufficiency Ledger. Diagnostic availability alone is not enough; the report should track what claims the evidence can and cannot support.

The ledger should store, for each user-facing conclusion or implied claim:

- claim ID
- plain-language claim
- evidence status: `supported`, `limited`, `unsupported`, or `contradicted`
- required evidence
- evidence actually received
- source diagnostic or source report section
- missing fields or missing artifacts
- limitation text suitable for UI/report use
- deterministic reason code

It should appear in:

- upload inspection: what this artifact can support
- overview: strongest supported and unsupported conclusions
- report appendix: full evidence map
- share room: report-safe evidence state without exposing raw uploads

The ledger is the product's trust spine. It makes the statement "we cannot support that conclusion from the data provided" operational instead of rhetorical.

### 2. Single Source Of Evidence Truth

The Evidence Sufficiency Ledger must not be recomputed separately by upload UI, overview UI, report rendering, share rooms, and LLM insight generation.

Implementation rule:

```text
artifact eligibility
  + engine capability profile
  + diagnostic outputs
  + report/share policy
  -> Evidence Sufficiency Ledger service
  -> persisted ledger snapshot
  -> entitlement projection overlays locked/unlocked state
  -> UI/report/share/LLM projections
```

The ledger is canonical for evidence status. Entitlements are deliberately projected after ledger derivation so the product never confuses "the evidence does not support this conclusion" with "your plan does not include this diagnostic."

Requirements:

- one shared product contract for evidence status
- one derivation service for the ledger
- implement ledger derivation outside the engine adapter mapper, for example in `src/lib/server/evidence/evidence-ledger-service.ts`
- keep `map-engine-analysis-record.ts` as a mapper that consumes ledger output/projections rather than deriving evidence policy
- deterministic ledger snapshots saved with the analysis/report
- upload inspection, diagnostic access, overview, reports, share rooms, and LLM inputs consume ledger projections rather than recomputing rules
- plan locks are overlays on top of ledger status, not alternate evidence states
- exports and share rooms use the same saved ledger snapshot
- LLM-generated insights may consume the ledger but cannot override it
- ledger derivation must be fixture-tested and snapshot-tested

### 3. Cross-Repo Rollout Contract

Approach A spans two repos. The plan must explicitly define release order and compatibility rules.

Required rollout sequence:

1. Add or update shared engine seam fixtures.
2. Update `bulletproof_bt` seam behavior and payload version.
3. Verify `bulletproof_bt` emits the fixture shape.
4. Update `invariance_research` adapter tests to consume the fixture shape.
5. Update web adapter mappings.
6. Update UI/report/share projections.
7. Release behind feature flags.

Compatibility rules:

- engine payload changes are additive unless the seam version is bumped
- unknown diagnostics fail closed into `unsupported`, not omitted
- fixture tests block merge when adapter behavior drifts
- every engine result carries a versioned `EngineEnvelopeV1`
- `EngineEnvelopeV1` includes `engine_name`, `engine_version`, `seam_name`, `seam_version`, `adapter_version`, `parser_version`, `capability_profile_version`, and `diagnostic_contract_version`
- `bulletproof_bt` owns the emitted engine/seam fields; `invariance_research` owns adapter/parser/report projection fields
- one owner is assigned for the compatibility matrix

The failure mode this prevents:

```text
bulletproof_bt changes EngineAnalysisResult
  -> invariance_research adapter expects old shape
  -> worker persists degraded or mis-mapped diagnostics
  -> report displays an evidence state that should have failed closed
```

### 4. Failure And Rescue Matrix

Every failure class needs a user-visible rescue path and an admin-visible debug path.

| Failure class | User sees | Admin sees | Rescue |
|---|---|---|---|
| Parser failure | File rejected with exact issue and accepted template link | Parser version, file envelope metadata, validation issue codes | Download/fill matching validation packet template |
| Eligibility limited | Available/limited/unavailable matrix plus missing evidence | Diagnostic evidence state and missing fields | Upload richer artifact or continue with limitations |
| Queue failure | Delayed or retrying status with product-safe explanation | Job lease, attempts, worker heartbeat, backoff state | Automatic retry, admin retry, or support handoff |
| Engine failure | Product-safe failure reason and retry/support option | Bridge exit code, timeout class, stderr pointer, engine version | Retry if safe, otherwise Research Desk/support handoff |
| Adapter failure | Report blocked because engine result could not be trusted | Schema mismatch details and fixture failure | Fail closed, add fixture, update adapter mapping |
| Export failure | Export retryable/failed status | Export job trace and renderer error class | Retry/regenerate export |
| Share-room failure | Report unavailable, expired, revoked, or superseded | Access audit event and share state | Regenerate share link or request access |
| Entitlement failure | Locked state clearly separated from unsupported evidence | Plan policy reason and diagnostic entitlement | Upgrade path or richer artifact guidance |

No failure should silently degrade into a confident report.

### 5. Report Share Room

The report should become a measurable demand object, not only a downloaded PDF.

Scope:

- shareable report view backed by `report_snapshot_id`
- dedicated `/share/[token]` route/API, not reused owner analysis detail routes
- `SharedReportViewModel` projection with allowlisted fields only
- raw uploads hidden by default and no artifact download route in share context
- expiration and revocation controls
- visible report version and generated-at timestamp
- stale/superseded warning when a newer report snapshot exists
- evidence ledger summary
- limitation appendix
- report engagement events that do not log report content
- Research Desk request CTA
- redaction tests for filenames, file paths, user IDs, account IDs, internal job IDs, bridge logs, raw upload metadata, and admin-only addenda

Not in scope:

- comments
- collaboration
- public profiles
- marketplace features
- raw artifact sharing by default

### 6. Report Sharing Threat Model

A shareable report creates a new trust boundary. It needs privacy rules from the first implementation.

Requirements:

- raw uploads are never exposed through shared reports by default
- share links are scoped, expiring, revocable, and audit-logged
- report views use least-privilege `SharedReportViewModel` payloads separate from owner analysis detail payloads
- share routes never call owner analysis detail APIs or artifact download APIs
- report appendix redacts sensitive file paths, raw filenames where needed, user IDs, account IDs, internal job IDs, and bridge logs
- share-room access events are tracked without logging report content
- admin/research-desk addenda are marked as human-reviewed and versioned
- expired, revoked, or superseded share links fail closed or show an explicit stale warning, never a silent stale report

### 7. Analysis, Report, And Share State Machine

The plan needs explicit states so retries, regeneration, and sharing cannot produce stale trust artifacts.

Analysis states:

```text
uploaded -> queued -> processing -> completed
       \-> failed
completed -> queued      # retry/rerun creates new processing attempt
completed -> superseded  # newer accepted result replaces it as current
```

Report states:

```text
not_generated -> queued -> rendering -> ready
                            \-> failed
ready -> expired
ready -> superseded
ready -> queued            # regenerate creates a new report snapshot
```

Share states:

```text
inactive -> active -> expired
                 \-> revoked
                 \-> superseded
```

Rules:

- all analysis/report/share state mutations go through explicit transition guards, not ad hoc repository updates
- implement either one scoped `state-transitions.ts` module or three small modules: `analysis-state-machine.ts`, `report-state-machine.ts`, and `share-state-machine.ts`
- transition guards define legal from/to states, required side effects, idempotency keys, and stale/superseded behavior
- retrying or rerunning an analysis after a report exists must either supersede the old report or leave it visible with a clear stale warning
- regenerating a report creates a new immutable report snapshot
- share rooms point to report snapshots, not mutable analysis objects
- overview/report pages must define how they select the current snapshot
- double-clicks on export/share must be idempotent or visibly deduplicated
- browser refresh/back-button flows must not create duplicate jobs or stale share links
- transition guards are fixture-tested for valid transitions, invalid transitions, duplicate requests, retry/rerun, report regeneration, and share revocation

### 8. Validation Packet Templates

The Lab should provide validation packet templates that make artifact sufficiency concrete.

Templates:

- trade CSV template
- structured bundle template
- research bundle template
- intentionally incomplete example that demonstrates unsupported diagnostics
- example that unlocks parameter stability
- example that unlocks benchmark/regime context

Each template should include:

- expected files and columns
- diagnostics it can support
- diagnostics it cannot support
- common mistakes
- sample accepted upload
- matching parser/validator fixture

This turns "your artifact is insufficient" into "here is exactly how to make it sufficient."

### 9. Wedge Acceptance Test Matrix

The first implementation should pass end-to-end tests that prove the wedge, not just units that prove functions.

| Scenario | Expected result |
|---|---|
| Trade CSV only | Overview, distribution, and Monte Carlo available; execution limited; stability/regime unavailable |
| Structured bundle full | Strongest upload eligibility, evidence-gated diagnostics, report ready, share room can be created |
| Incomplete bundle | Ledger marks unsupported claims and points to missing files |
| Plan-locked diagnostic | Locked is distinct from unavailable |
| Engine skipped diagnostic | Ledger preserves skipped reason and report cannot overclaim |
| Adapter unknown diagnostic | Fail closed; fixture test fails until mapped |
| Report regeneration after retry | Old share is superseded or visibly stale |
| Expired share link | Fails closed with no report payload |
| Raw upload privacy | Shared report cannot access raw artifact URL |
| Validation template upload | Each template unlocks the documented diagnostics |
| LLM insight contradiction | Deterministic ledger wins over generated prose |

Cross-repo fixture requirement:

- `bulletproof_bt` emits canonical fixture payloads
- `invariance_research` maps those same fixtures into product contracts

### 10. Wedge Learning Event Model

Analytics are part of the demand test. Events should answer whether the hostile validation report is working.

Minimum events:

- `upload_started`
- `upload_rejected`
- `eligibility_viewed`
- `missing_evidence_cta_clicked`
- `analysis_started`
- `analysis_completed`
- `ledger_viewed`
- `report_generated`
- `report_exported`
- `share_room_created`
- `share_room_opened`
- `share_room_expired`
- `research_desk_requested`
- `upgrade_clicked_from_locked_diagnostic`
- `template_downloaded`
- `template_uploaded_successfully`

Event payload rules:

- include account/plan bucket, not raw identity in analytics exports
- include artifact class
- include diagnostic availability counts
- include ledger counts by evidence status
- include report version where relevant
- never include raw strategy names, uploaded filenames, report content, or bridge logs

### 11. Feature Flag And Rollout Plan

Ship the wedge in slices so a half-built ledger or share room does not reach users as a confusing trust artifact.

Feature flags:

- `evidence_ledger_internal`: ledger computed and visible only in admin/debug
- `evidence_ledger_user`: ledger visible in upload, overview, and report
- `validation_templates`: packet templates visible in docs/intake
- `report_share_room_internal`: share rooms creatable by admins/test accounts only
- `report_share_room_public`: share rooms available to eligible users
- `research_desk_handoff`: CTA creates internal queue items

Rollout order:

1. Cross-repo fixtures and seam versioning.
2. Internal ledger computation.
3. User-visible eligibility and overview ledger.
4. Report appendix ledger.
5. Validation templates.
6. Report share room internal.
7. Report share room public.
8. Research Desk queue/handoff.

### 12. Memory Promotion Gate

Do not start tenant research memory until the Lab wedge shows demand.

Promotion criteria:

- at least 10 real artifact bundles reviewed
- at least 3 users submit a second or richer artifact after seeing limitations
- at least 3 reports are shared externally or used in a Research Desk request
- evidence ledger contract and tests are stable
- report snapshot/version model is live
- report sharing privacy model is implemented
- there is a specific user workflow that asks "where have I seen this before?"

When promoted, tenant memory starts with:

- per-account strategy history
- repeated unsupported claims
- recurring missing evidence
- hostile regimes per user/account
- report/version lineage

Not included at first:

- cross-user recommendations
- anonymized benchmark learning
- strategy generation
- automatic deployment advice

## Target Product Shape

### First Screen Promise

The public `/robustness-lab` page should sell one concrete action:

> Upload strategy evidence and receive a validation report that tells you what is supported, what is fragile, and what cannot be concluded.

Avoid leading with "platform" language. Lead with a job the user already understands.

### Authenticated Workflow

The current `/app/new-analysis` workflow should become:

1. Upload artifact.
2. Inspect artifact.
3. Explain what is available, limited, and unavailable.
4. Let the user name the strategy and choose benchmark/runtime assumptions.
5. Run analysis.
6. Show progress with meaningful stages.
7. Land on verdict-first overview.
8. Allow diagnostic deep dives.
9. Generate a shareable validation report.
10. Offer deeper Research Desk review when unsupported conclusions matter.

### Core Objects

Current objects to keep:

- account
- user
- artifact
- analysis
- analysis job
- export
- export job
- entitlement
- usage snapshot
- benchmark config

Objects to add later:

- strategy
- project
- market claim
- hypothesis
- validation plan
- experiment group
- research finding
- limitation
- user annotation
- reviewer annotation
- research memory entry

Do not add all of these in the first pass. Add `strategy`, `project`, and `market_claim` only when the current Lab can already produce a credible validation report.

## Engineering Rollout Slices For Approach A

Generated by `/plan-eng-review` on 2026-05-15 after the complexity gate selected phased scope.

Historical Approach A note: the CEO scope remains valid for the Lab subsystem, but it is no longer the product center. These slices remain useful wherever the Research Pipeline needs import/audit/report capabilities.

### Slice 1: Evidence And Seam Foundation

Goal: make the existing Lab's evidence status deterministic and cross-repo-safe.

Owns:

- shared engine seam fixtures in both repos
- `EngineEnvelopeV1` and compatibility matrix
- canonical `EvidenceLedgerService` in a dedicated evidence module, not inside the bulletproof adapter mapper
- shared evidence ledger contract schemas and fixtures
- upload inspection projections from the ledger
- diagnostic access projections with entitlement overlays
- fail-closed adapter behavior for unknown diagnostics
- fixture and snapshot tests for trade CSV, limited bundle, full bundle, and malformed payload

Must not include:

- share rooms
- public report links
- Research Desk workflow changes
- tenant memory
- LLM-driven claim rewriting

### Slice 2: Report Snapshot Foundation

Goal: make reports immutable, reproducible trust artifacts.

Owns:

- `report_snapshots` persistence
- report payload generation from analysis + ledger snapshot
- owner exports rendering from report snapshots
- report regeneration idempotency
- stale/superseded warnings
- transition guard tests for analysis/report state

Must not include:

- externally shared report rooms
- comments or collaboration
- marketplace/profile behavior

### Slice 3: Share Room Trust Boundary

Goal: make the report a safe, measurable demand object.

Owns:

- dedicated share token model
- `/share/[token]` route/API
- `SharedReportViewModel` allowlisted projection
- expiry, revocation, superseded behavior
- share access audit events without report-content logging
- indexes and cleanup rules for share token lookup, access events, expired shares, revoked shares, and superseded reports
- redaction tests for all sensitive fields

### Slice 4: Research Desk And Learning Loop

Goal: route high-value gaps to human review and product learning.

Owns:

- Research Desk request from limitations
- reviewer addenda tied to report snapshots
- wedge learning events
- validation packet template engagement
- memory promotion gates only after repeated evidence supports promotion

### Slice 5: Full Ambition Expansion

Goal: graduate from Lab-first product to claim-first research operating system.

Owns:

- explicit market claim capture
- strategy entity lifecycle
- tenant-scoped research memory
- experiment planning and promotion workflow
- cross-strategy research recall

## Approach A Detailed Implementation Plan

### Phase A0: Repo Contract Map

Goal:

Create a written contract between `invariance_research` and `bulletproof_bt` so both repos can evolve without accidental breakage.

Actions in `bulletproof_bt`:

- document `run_analysis_from_parsed_artifact`
- document `ParsedArtifactInput`
- document `AnalysisRunConfig`
- document `EngineAnalysisResult`
- version the SaaS seam payload
- add golden JSON fixtures for trade CSV, structured bundle, parameter sweep, and incomplete artifact
- add a fixture that intentionally lacks OHLCV/benchmark/context so unsupported diagnostics are locked

Actions in `invariance_research`:

- document expected engine response envelope
- add fixture tests around `map-engine-analysis-record.ts`
- add contract tests for `UploadInspectionResponse`, `CreateAnalysisRequest`, `AnalysisRecord`, and export payloads
- add a compatibility matrix mapping app diagnostic pages to engine diagnostic names

Acceptance criteria:

- a developer can change `bulletproof_bt` diagnostics and know which web tests must pass
- unsupported diagnostics are represented as first-class outputs, not missing data
- every engine payload carries version, engine name, adapter version, and capability profile

### Phase A1: Make Upload Eligibility The Trust Moment

Goal:

Turn upload inspection into a product asset, not a preflight form.

Current app already has:

- file type checks
- 10 MB limit
- `.csv` and `.zip`
- parser/validator pipeline
- eligibility summary
- plan upload policy checks

Needed changes:

- introduce the canonical `EvidenceLedgerService` before expanding report/share surfaces
- show a clearer "what we can conclude" and "what we cannot conclude" panel from ledger projections
- separate data insufficiency from plan locks by deriving evidence first and applying entitlement overlays second
- show missing fields that would unlock each diagnostic
- show parser confidence and assumptions
- show detected time range, symbols, trade count, costs, benchmark presence, OHLCV presence, parameter sweep presence
- preserve rejected uploads as diagnostic events only if privacy policy allows
- add sample downloadable artifact templates

Diagnostic eligibility language:

- "Available" means the diagnostic is supported by the artifact and engine.
- "Limited" means the product can compute a bounded proxy but cannot support a strong conclusion.
- "Unavailable" means the data does not support the diagnostic.
- "Locked" means the plan does not include the diagnostic even though the artifact may support it.

Acceptance criteria:

- a user knows why a diagnostic is unavailable before paying
- a user knows exactly what to upload next
- a plan restriction is never confused with missing evidence
- the app never silently infers execution realism from trade CSV alone

### Phase A2: Harden The Engine Seam

Goal:

Make the Node-to-Python bridge boring and auditable.

Current app already calls:

```text
analysis worker
  -> buildAnalysisEngineDispatchPayload
  -> runBulletproofAnalysisFromParsedArtifact
  -> runBulletproofEngine
  -> scripts/run_bulletproof_engine.py
  -> bt.run_analysis_from_parsed_artifact
  -> map-engine-analysis-record.ts
```

Needed changes:

- introduce a versioned `EngineEnvelopeV1` before ledger/report/share expansion
- add seam version negotiation
- add `seam_version`, `adapter_version`, `parser_version`, `capability_profile_version`, and `diagnostic_contract_version` to persisted engine context
- add engine timeout and failure classification
- persist bridge stdout/stderr safely for admins without exposing user strategy content
- require engine result schema validation before persistence
- persist engine context alongside analysis result
- distinguish engine failure, parser failure, queue failure, entitlement failure, and report failure
- add fixture-based regression tests for every mapped diagnostic
- add a compatibility test that imports `bt` and probes the seam in CI

Acceptance criteria:

- failed analyses show product-safe user errors
- admins can debug failures without touching raw strategy content unnecessarily
- changes to `bt.saas.models` fail fast in the web app
- engine payloads cannot accidentally bypass adapter normalization

### Phase A3: Verdict-First Results Overview

Goal:

The results page should read like a hostile research memo, not a dashboard.

Current app already has diagnostic pages:

- overview
- distribution
- monte carlo
- execution
- regimes
- ruin
- stability
- report

Needed top-level overview:

- headline verdict: robust, conditional, fragile, unsupported, or failed validation
- one-sentence reason
- three reasons to trust
- three reasons to doubt
- most important unsupported conclusion
- next experiment to run
- export/report CTA
- Research Desk CTA when a limitation blocks a high-value decision

Verdict rules:

- "Robust" requires multi-diagnostic support, not a high score alone.
- "Conditional" means the strategy is promising under stated assumptions but missing key validation.
- "Fragile" means one or more diagnostics show material dependence on costs, regime, parameter, or outlier concentration.
- "Unsupported" means the artifact cannot prove the user's likely claim.
- "Failed validation" means the evidence actively contradicts the claim.

Acceptance criteria:

- charts support the verdict instead of replacing it
- a user can explain the result to someone else in under one minute
- unsupported diagnostics are visible on the overview, not hidden in tabs

### Phase A4: Make The Report Worth Sharing

Goal:

The report must feel like an artifact someone would send to a buyer, allocator, partner, or internal committee.

Current app already has:

- report page
- deterministic JSON, Markdown, PDF exports
- export queue
- download endpoint

Engineering review decision: immutable report snapshots come before share rooms. Today exports are generated from mutable analysis records. Approach A must first create a snapshot foundation so retries, reruns, regenerations, and shared links cannot silently point at stale or changed evidence.

Snapshot foundation requirements:

- add `report_snapshots` before adding share rooms
- store immutable `report_payload_json`, `source_analysis_id`, `source_hash`, `ledger_snapshot_id`, `report_version`, `generated_at`, and `superseded_by_report_id`
- render owner exports and shared reports from a report snapshot, not directly from a mutable `AnalysisRecord`
- make regeneration create a new snapshot instead of mutating the previous report
- make share rooms point to `report_snapshot_id` only
- show stale/superseded warnings when a newer accepted analysis or report exists
- add idempotency keys for report generation/export/share creation

Required sequence:

```text
analysis completed
  -> evidence ledger snapshot created
  -> report snapshot generated from analysis + ledger snapshot
  -> owner export renders from report snapshot
  -> share room points to report snapshot
  -> rerun/regenerate creates new immutable snapshot
```

Needed report sections:

- executive verdict
- strategy and artifact identity
- evidence received
- diagnostic availability matrix
- performance summary
- cost/execution assumptions
- trade distribution and concentration
- benchmark context
- Monte Carlo and ruin assumptions
- parameter stability if available
- regime sensitivity if available
- limitations and unsupported claims
- reproducibility appendix
- engine, adapter, parser, dataset, and report versions
- content hashes for inputs and derived artifacts

PDF improvement path:

- keep deterministic renderer first
- add stronger layout only after content contract stabilizes
- no investment advice language
- no unsupported confidence language

Acceptance criteria:

- every chart or metric in the report has a source diagnostic
- every conclusion points to evidence or limitation
- a report generated today can be reproduced later from stored artifacts and versions
- reports can be shared without leaking raw uploaded files by default

### Phase A5: Research Desk Handoff

Goal:

Convert incomplete automated validation into a revenue and learning path.

Current app already has:

- public `/research-desk`
- contact funnel
- admin console
- account and analysis records

Needed changes:

- add "Request deeper validation" CTA from report and limited diagnostics
- capture which limitation triggered the request
- create admin queue item tied to analysis ID
- allow internal reviewer notes
- allow reviewer-approved report addendum
- track requested services: execution audit, data QA, benchmark suite, claim formalization, strategy rewrite as hypothesis, full advisory validation

Acceptance criteria:

- a serious user never hits a dead end
- every Research Desk request is linked to the exact artifact, analysis, limitations, and report
- manual review teaches the product what to automate next

### Phase A6: Production Persistence And Storage

Goal:

Remove assumptions that block real deployment.

Current app has:

- SQLite persistence
- Postgres schema/repository work in progress
- local object storage abstraction
- benchmark provider abstraction
- health checks

Needed path:

- make Postgres the production default
- keep SQLite for local dev only
- move uploads, exports, reports, and benchmark manifest/data to S3/R2-compatible object storage
- enforce object keys by account, artifact, analysis, and export ID
- add retention policy controls
- add deletion/tombstone paths
- add backup/restore runbook
- add rate limits to upload, analysis create, auth, waitlist, and contact endpoints
- add worker concurrency controls by plan and system load

Acceptance criteria:

- production does not depend on workstation paths
- workers can run outside the web process
- storage survives redeploys
- user deletion and retention policies are documented
- admin health shows DB, storage, engine, queue, Stripe, email, benchmark, and worker state

## Strategy Truth Room Sellable Approach A Definition

Approach A should not ship as "a dashboard for backtests." It should ship as a falsification room for strategy evidence.

Product name:

> Strategy Truth Room

Core promise:

> Upload evidence. Get hostile validation.

Best positioning:

> Institutional-grade strategy due diligence for traders who cannot afford to fool themselves.

Primary actions:

- Validate My Strategy
- Audit This Backtest
- Generate Proof Report
- Research Desk Review

The product should optimize for falsification:

- what assumptions produced this result?
- what happens when fills get worse?
- what happens when fees change?
- what happens outside the cherry-picked regime?
- would this strategy survive the rules of the prop firm evaluation the user is trying to pass?
- how much of the edge comes from rare trades?
- how quickly does the thesis die under perturbation?
- what evidence is missing?
- what does this result not prove?

### Office Hours Demand Reality

The user is not buying another way to admire an equity curve. The user is buying defensible doubt.

Current status quo:

- spreadsheet summaries
- screenshots from backtest tools
- broker exports without context
- strategy seller PDFs
- forum claims
- self-built notebooks
- basic trade journaling analytics
- informal code or data reviews

The wedge is strongest when the user already has evidence and a claim they need to defend or kill. Do not start by asking the user to build a strategy inside the app. Start by letting them upload the proof they already trust, then show where that proof fails.

First customer profiles:

- serious independent systematic trader validating a live or near-live system
- prop firm challenge participant trying not to breach daily-loss, total-drawdown, or profit-target rules
- strategy seller or educator who needs a buyer-ready validation memo
- allocator, prop evaluator, or partner screening a claimed edge
- crypto, FX, index, or equity researcher stress-testing regime-dependent results
- backtest platform power user who needs a hostile external audit

Demand test:

The product has pull when users export or share conditional and negative reports, not only flattering reports.

### CEO Review Scope Decision

Scope mode:

> Selective expansion, with Lab-first hardening as the baseline.

Keep Approach A focused on artifact-first validation. Cherry-pick only the expansions that make the first product commercially credible:

- stronger artifact schema and bundle contract
- falsification-first analyst workbench
- explicit unsupported-claims inventory
- prop evaluation readiness as a rules-based account-survival diagnostic
- proof report snapshots and share controls
- Research Desk upgrade path
- tiering that turns missing evidence and advanced diagnostics into natural upgrades

Do not include in launch Approach A:

- natural-language strategy generation
- web-native strategy builder
- signal marketplace
- broker execution
- live trading
- cross-user intelligence
- full research OS memory
- portfolio allocator

Those are full-ambition products. Approach A wins by making uploaded evidence trustworthy or visibly untrustworthy.

### Sellable Readiness Bar

Approach A is sellable only when every completed analysis produces:

- a clear verdict classification
- an evidence coverage map
- an unsupported-claims inventory
- an assumptions ledger
- at least one concrete falsification result
- a rules-based prop evaluation readiness result when the user provides evaluation constraints or chooses the fallback profile
- a diagnostic availability matrix
- a "next evidence to upload" path
- an exportable proof report
- a Research Desk path when automation cannot answer the user's decision

The verdict taxonomy:

- Structurally credible: the available evidence supports the claim across the required diagnostics.
- Promising but under-supported: the result may be real, but missing artifacts block stronger conclusions.
- Likely overfit: performance appears too dependent on narrow parameters, outliers, or selection.
- Execution fantasy: the edge degrades materially under realistic costs, fills, latency, or liquidity assumptions.
- Data-insufficient: the artifact cannot support the claim the user likely wants to make.
- Regime-dependent: edge exists mainly in a narrow market state.
- Untradeable after costs: expected edge is consumed by plausible costs, slippage, or sizing constraints.

The overview must always explain why the verdict exists. A score without a hostile reason is not enough.

### Analyst Workbench Information Architecture

The authenticated product should feel like an analyst workbench, not a pile of chart cards. Every page should answer a falsification question, show the evidence behind the answer, and end with a user decision.

Shared page anatomy:

- verdict strip: the page-level conclusion in one sentence
- evidence state: Available, Limited, Unavailable, or Locked
- what was tested
- what the result says
- what assumption matters most
- what evidence is missing
- next action: upload richer evidence, run stronger test, export report, or request Research Desk review

The sidebar should always include all workspaces, even when gated or artifact-limited. Missing diagnostics are product education, not invisible features.

Required workspaces for sellable Approach A:

- Evidence Intake
- Truth Room Overview
- Assumption Ledger
- Execution Reality
- Distribution And Edge Concentration
- Monte Carlo Survival
- Ruin And Capital Survival
- Prop Evaluation Readiness
- Regime Dependence
- Parameter Stability
- Proof Report
- Share Room
- Research Desk Review
- Analysis Library

### Evidence Intake

Falsification question:

> What can this artifact prove, and what can it not prove?

Must show:

- detected artifact type
- parser confidence
- time range
- asset universe
- symbol coverage
- trade count or observation count
- fields detected
- costs detected
- OHLCV/context detected
- benchmark detected
- parameter sweep detected
- config detected
- unsupported diagnostics and exact missing inputs
- plan locks separated from evidence limitations

Primary user decision:

- continue with limited validation
- upload a richer bundle
- use a template
- request Research Desk help

Premium behavior:

Even free users should see what stronger artifacts would unlock. They should not see enough output to replace the paid diagnostic.

### Truth Room Overview

Falsification question:

> Is this result credible enough to keep investigating?

Must show:

- headline verdict
- credibility score
- evidence coverage score
- diagnostic availability matrix
- three reasons to trust the result
- three reasons to doubt the result
- strongest positive evidence
- strongest negative evidence
- most important unsupported claim
- next kill test
- export/report CTA
- Research Desk CTA if a limitation blocks a high-value decision

Primary user decision:

- accept the verdict as sufficient
- upload richer evidence
- inspect a specific failure mode
- generate a proof report

Design note:

The overview should not be a metric grid. It should read like the first page of a hostile research memo with supporting instruments below it.

### Assumption Ledger

Falsification question:

> What assumptions produced this result?

This is a missing workspace and should become a first-class product object. It can also appear as a persistent panel across other workspaces.

Must show:

- declared assumptions from config, report, or user input
- inferred assumptions from uploaded data
- default assumptions the engine applied
- cost assumptions
- execution assumptions
- sizing assumptions
- benchmark assumptions
- timezone and currency assumptions
- missing assumptions
- assumptions that materially affect verdict
- assumptions contradicted by artifacts

Primary user decision:

- accept assumptions for report
- revise assumptions
- upload supporting evidence
- request human review where assumptions are ambiguous

Report rule:

No proof report should bury assumptions in an appendix only. The executive report must state the assumptions that make the verdict true.

### Execution Reality

Falsification question:

> Does the edge survive worse fills, fees, spreads, and slippage?

Must show:

- baseline expectancy versus stressed expectancy
- fee sensitivity
- slippage sensitivity
- spread sensitivity where data permits
- commission model detected or assumed
- breakeven cost threshold
- execution fantasy warnings
- cost-adjusted drawdown and profitability
- unsupported execution claims
- broker/fill evidence quality

Primary user decision:

- trust the execution assumptions
- upload broker fills
- revise cost model
- classify the strategy as execution-fragile

Evidence behavior:

Trade CSV alone can support limited cost sensitivity, but not a strong execution realism verdict. Broker fills or explicit cost assumptions are required for strong execution conclusions.

### Distribution And Edge Concentration

Falsification question:

> Is the edge broad, or does it come from rare trades and outliers?

Must show:

- return distribution
- win/loss distribution
- top trade contribution
- top 5 and top 10 trade contribution
- payoff asymmetry
- streak profile
- sample size warnings
- tail dependence
- median trade versus mean trade
- concentration score
- rare-trade reliance verdict

Primary user decision:

- trust the edge as repeatable
- treat the result as outlier-dependent
- collect more trades
- segment the strategy before drawing conclusions

Design note:

This page should make "one whale trade made the backtest" impossible to miss.

### Monte Carlo Survival

Falsification question:

> Does the thesis survive path perturbation?

Must show:

- equity fan chart
- ending equity distribution
- drawdown envelope
- survival probability
- probability of hitting user-selected drawdown levels
- losing streak distribution
- time-to-recovery distribution where possible
- path dependence warning
- assumptions behind resampling

Primary user decision:

- accept path risk
- reduce size
- gather more trades
- reject the strategy as path-fragile

Evidence behavior:

Monte Carlo from trade logs is useful but bounded. It does not prove market stationarity. The page must say when it is resampling the past rather than simulating a richer future distribution.

### Ruin And Capital Survival

Falsification question:

> Can the account survive this edge under realistic sizing?

Must show:

- capital survival score
- ruin probability under baseline assumptions
- ruin probability under stressed costs
- drawdown breach probabilities
- risk-per-trade sensitivity
- max adverse run
- account size and sizing assumption
- recommended risk guardrails as non-advisory diagnostics
- limits of the ruin model

Primary user decision:

- reduce size
- reject the strategy
- upload sizing/config evidence
- request deeper review

Language rule:

The app must not give investment advice. It can say "this sizing assumption creates a high probability of breaching a 30% drawdown threshold under this model."

### Prop Evaluation Readiness

Falsification question:

> Would this strategy pass the user's funded-account evaluation rules without breaching the contract?

This workspace should be named for the user's job, not for the internal model. Recommended product label:

> Prop Evaluation Readiness

The diagnostic should be rules-based and non-advisory. It does not say "take this challenge" or "trade this size." It says whether the uploaded strategy path, under stated assumptions, would have breached the evaluation rules and what would need to change for the result to become feasible.

Must show:

- selected rule profile: fallback profile, user-entered runtime rules, saved firm profile, or post-run edited rules
- starting account size
- profit target to pass
- maximum total drawdown
- total drawdown basis: static starting balance, trailing balance, trailing equity, or end-of-day trailing high-water mark
- maximum daily loss
- daily-loss basis: intraday equity, closed balance, or end-of-day balance
- rule reset timezone
- minimum and maximum trading days when provided
- consistency rule when provided, such as max single-day profit contribution
- lot, leverage, max position, or exposure limits when provided
- news/weekend/holding restrictions as declared constraints when provided
- first breach date, breach type, and margin to breach
- probability of breach under Monte Carlo path perturbation where trade-level data supports it
- probability of reaching the profit target before breaching drawdown rules
- rule-by-rule pass/fail/unknown table
- suggested non-advisory improvement levers:
  - reduce risk per trade
  - cap daily loss before the firm limit
  - avoid concentration in one day or one outlier trade
  - reduce trade frequency during weak regimes
  - require richer intraday equity or broker fills for stronger daily-loss conclusions

Runtime and post-run behavior:

- During analysis setup, users may optionally enter evaluation rules for the prop firm or challenge they are testing against.
- If they do not enter rules, the run uses a clearly labeled fallback evaluation profile for preview purposes only.
- After a run completes, the Prop Evaluation Readiness tab must show which rules were used and whether they were fallback or user-provided.
- The user can edit the evaluation rules after the run and recompute the readiness analysis against the saved trade/equity path without rerunning unrelated diagnostics.
- Each recomputation creates a versioned rule snapshot and readiness result so the report can say exactly which contract assumptions produced the conclusion.
- If the uploaded artifact lacks intraday equity, timestamps, sizing, or closed/open PnL detail, daily-loss conclusions degrade to limited or unknown rather than pretending precision.

Primary user decision:

- reduce risk or sizing assumptions
- upload richer broker/equity data
- edit rules to match the real prop firm contract
- reject the strategy for that evaluation
- request Research Desk review for ambiguous rules or missing execution evidence

Tier strategy:

- Explorer should see a locked or watermarked preview using the fallback profile so the value is obvious.
- Individual should get one active custom evaluation profile per analysis and post-run recomputation.
- Pro should get multiple saved firm profiles, rule-profile comparison, report inclusion, and share-safe readiness summaries.
- Team should get shared firm-rule templates, admin-managed profiles, and bulk comparison across strategies.
- Research Desk can review ambiguous rule interpretation, broker evidence, and challenge-specific risk controls.

Report rule:

Prop Evaluation Readiness should appear in the proof report only with the exact rule snapshot used. Reports must avoid implying affiliation with, endorsement by, or guaranteed acceptance from any prop firm.

### Regime Dependence

Falsification question:

> What happens outside the cherry-picked regime?

Must show:

- regime availability state
- regime definitions used
- best and worst regime
- performance by volatility/trend/liquidity state where supported
- regime heatmap
- regime dispersion score
- missing OHLCV/context warning
- comparison between full sample and favorable regimes
- unsupported regime claims

Primary user decision:

- upload OHLCV/context data
- restrict the claim to specific regimes
- treat the result as regime-dependent
- request Research Desk benchmark/context construction

Evidence behavior:

This tab must always exist. If OHLCV or context is missing, it should show the locked evidence state and explain exactly what bundle unlocks the analysis.

### Parameter Stability

Falsification question:

> How quickly does the thesis die under parameter perturbation?

Must show:

- parameter sweep availability state
- parameter surface
- robustness plateau
- cliff zones
- best parameter versus neighboring parameters
- sensitivity by objective metric
- overfit warning
- missing sweep requirements
- required format for parameter grid uploads

Primary user decision:

- accept robustness
- classify as overfit-prone
- upload a parameter sweep
- request Research Desk sweep design

Evidence behavior:

This page requires a parameter sweep or structured engine run bundle. It must not fake stability from a single backtest run.

### Proof Report

Falsification question:

> Can this evidence be shared as defensible proof without overclaiming?

Must show:

- executive verdict
- strategy and artifact identity
- evidence received
- evidence coverage matrix
- assumptions ledger summary
- unsupported claims
- diagnostic summaries
- key charts only where they support the verdict
- limitations
- reproducibility appendix
- engine, parser, adapter, report, and artifact schema versions
- content hashes
- export controls
- share controls

Primary user decision:

- export private PDF
- create controlled share link
- regenerate after richer evidence
- request Research Desk addendum

Report standard:

The report should feel buyer-ready, allocator-ready, and committee-readable. It should not read like a marketing brochure. Negative and conditional reports must still feel valuable.

### Share Room

Falsification question:

> Can this verdict be shared without leaking strategy IP or overstating evidence?

Must show:

- current report snapshot
- share status
- expiry
- access log summary
- fields included and excluded
- raw artifact privacy status
- revocation control
- superseded report warning
- recipient-safe report preview

Primary user decision:

- create link
- revoke link
- regenerate report before sharing
- upgrade for private diligence room

Threat model:

Shared reports must render from immutable report snapshots and allowlisted projections. Raw uploads, internal engine payloads, account data, private notes, and Research Desk reviewer drafts must never be available in the public share context.

### Research Desk Review

Falsification question:

> What requires human or agent-assisted review beyond automated validation?

Must show:

- limitation that triggered the handoff
- recommended review type
- evidence packet preview
- estimated scope
- optional addendum path
- reviewer-safe artifact access rules
- prior report snapshot

Primary user decision:

- request execution audit
- request data QA
- request benchmark/context construction
- request parameter sweep design
- request buyer/allocator memo upgrade

Research Desk should be the pressure-release valve for serious users. A paid user should never reach "the product cannot answer this" without a next step.

### Analysis Library

Falsification question:

> How has this strategy's evidence changed over time?

Must show:

- analyses grouped by strategy or artifact family
- verdict history
- evidence coverage history
- report snapshots
- exports
- richer-upload prompts
- stale or superseded reports
- comparison-ready runs

Primary user decision:

- rerun with richer evidence
- compare two analyses
- regenerate report
- move into later full-ambition strategy workspace

This library is the bridge to Stage 2 strategy workspaces. It should be useful before full research OS memory exists.

## Artifact Schema And Diagnostic Unlock Contract

Approach A needs a public artifact contract. Users should understand which evidence unlocks which conclusions before they upload.

Artifact states:

- Available: the artifact supports the diagnostic.
- Limited: the product can compute a bounded proxy but cannot make a strong conclusion.
- Unavailable: the artifact cannot support the diagnostic.
- Locked: the user's plan does not include the diagnostic even though the artifact may support it.

Evidence must be evaluated before entitlement. A diagnostic can be artifact-supported and plan-locked, but a plan must never turn unsupported evidence into a supported conclusion.

### `trade_log_csv_v1`

Purpose:

- validate realized or simulated trade outcomes
- compute distribution, concentration, Monte Carlo, drawdown, and limited execution sensitivity

Required fields:

- `entry_time`
- `exit_time` or close timestamp
- `symbol` or instrument
- `side`
- `entry_price`
- `exit_price`
- `quantity`, `size`, or normalized exposure
- `pnl`, `return`, or enough fields to compute it

Recommended fields:

- `trade_id`
- `fees`
- `slippage`
- `strategy_tag`
- `timeframe`
- `entry_reason`
- `exit_reason`
- `account_currency`
- `venue`

Unlocks:

- Overview: Full
- Assumption Ledger: Limited
- Distribution: Full
- Monte Carlo: Full
- Ruin: Limited to Full depending on sizing fields
- Prop Evaluation Readiness: Limited to Full depending on account size, timestamps, and sizing fields
- Execution: Limited unless costs and fills are included
- Regime: Limited unless OHLCV/context is included
- Parameter Stability: No
- Proof Report: Full with limitations

### `equity_curve_v1`

Purpose:

- validate path behavior when trade-level data is unavailable

Required fields:

- `timestamp`
- `equity`, `nav`, or cumulative return

Recommended fields:

- deposits/withdrawals
- benchmark value
- account currency
- strategy tag

Unlocks:

- Overview: Limited
- Monte Carlo: Limited path analysis
- Ruin: Limited drawdown breach analysis
- Prop Evaluation Readiness: Limited path/drawdown analysis when account sizing and rule profile are supplied
- Distribution: No trade-level concentration
- Execution: No
- Regime: Limited only with aligned OHLCV/context
- Parameter Stability: No
- Proof Report: Limited

### `broker_export_v1`

Purpose:

- audit execution realism from fills, orders, commissions, and venue data

Required fields:

- fill timestamp
- symbol
- side
- fill price
- quantity
- commission or fee where available

Recommended fields:

- order type
- order timestamp
- fill venue
- spread
- liquidity flag
- account currency
- order id
- trade id mapping

Unlocks:

- Execution: Full when matched to strategy trades
- Assumption Ledger: Full for execution assumptions
- Distribution: Limited unless complete trade lifecycle exists
- Prop Evaluation Readiness: Fuller daily-loss evidence when fills, timestamps, commissions, and account currency are present
- Proof Report: Full execution appendix

### `backtest_report_v1`

Purpose:

- audit claims and stated metrics from existing reports, PDFs, HTML exports, or JSON outputs

Accepted formats:

- PDF
- HTML
- JSON
- CSV summary
- Markdown

Extracted objects:

- declared performance metrics
- stated assumptions
- strategy claims
- cost claims
- benchmark claims
- data period
- asset universe
- tool/source metadata

Unlocks:

- Assumption Ledger: Limited to Full depending on parse quality
- Unsupported Claims Inventory: Full
- Overview: Limited unless raw trades/equity are included
- Proof Report: Limited claim-audit report

Rule:

A report alone can be audited for unsupported claims. It cannot validate the underlying strategy without raw evidence.

### `ohlcv_context_v1`

Purpose:

- support regime dependence, benchmark context, and market-state conditioning

Required fields:

- timestamp
- open
- high
- low
- close
- volume where available
- symbol
- timeframe

Recommended fields:

- adjusted close
- session/calendar metadata
- asset class
- exchange
- liquidity proxy

Unlocks:

- Regime: Conditional when aligned to trades or equity curve
- Benchmark Context: Limited to Full
- Execution: Limited spread/liquidity proxies where available
- Proof Report: regime appendix

### `parameter_sweep_v1`

Purpose:

- validate whether results survive parameter perturbation

Required fields:

- run id
- parameter names
- parameter values
- objective metric
- sample period

Recommended fields:

- train/test split
- costs used
- drawdown
- Sharpe or risk-adjusted metric
- trade count
- seed
- config hash

Unlocks:

- Parameter Stability: Conditional when the sweep is real, mapped, and sufficiently populated
- Overfit Warnings: Full
- Proof Report: parameter appendix

Rule:

Single-run configs do not unlock parameter stability. They only populate the assumption ledger.

### `strategy_config_v1`

Purpose:

- capture assumptions, sizing, costs, filters, and risk model

Accepted formats:

- JSON
- YAML
- TOML
- INI
- plain text with structured extraction

Recommended fields:

- strategy name
- asset universe
- timeframe
- entry/exit rules
- filters
- sizing rules
- risk limits
- cost model
- data source
- benchmark
- excluded periods

Unlocks:

- Assumption Ledger: Full
- Unsupported Claims Inventory: Full when paired with declared claims
- Ruin: Full when sizing fields exist
- Prop Evaluation Readiness: Fuller rule simulation when sizing, risk limits, account size, and firm constraints exist
- Proof Report: assumptions appendix

### `prop_evaluation_rules_v1`

Purpose:

- define the funded-account evaluation rules the user wants to test the strategy against

Accepted formats:

- runtime form input
- saved profile
- JSON/YAML bundle file
- Research Desk-entered profile

Recommended fields:

- firm or challenge label
- account size
- profit target
- maximum total drawdown
- total drawdown basis: static, trailing balance, trailing equity, or end-of-day trailing
- maximum daily loss
- daily-loss basis: intraday equity, closed balance, or end-of-day balance
- rule reset timezone
- minimum trading days
- maximum evaluation days
- consistency rule
- max lot, max exposure, or leverage cap
- weekend, news, or holding restrictions
- payout or phase label where relevant

Unlocks:

- Prop Evaluation Readiness: Full when paired with trade or equity path data
- Assumption Ledger: Full for evaluation assumptions
- Proof Report: prop-readiness appendix with exact rule snapshot

Rule:

The product can include fallback rules only as a clearly labeled preview. User-entered or saved rules are required before the report can claim readiness against a specific prop firm evaluation.

### `benchmark_series_v1`

Purpose:

- compare results against a relevant market or strategy benchmark

Required fields:

- timestamp
- benchmark value or return
- benchmark identifier

Unlocks:

- Overview: benchmark-relative context
- Regime: benchmark-aware context where aligned
- Proof Report: benchmark appendix

### Full Validation Bundle

A full bundle is the gold-standard upload for Approach A.

Required top-level file:

```json
{
  "schema_version": "strategy_truth_room_bundle_v1",
  "bundle_id": "uuid",
  "strategy_identity": {
    "name": "string",
    "asset_universe": ["string"],
    "base_currency": "USD",
    "timezone": "UTC"
  },
  "declared_claims": [
    {
      "claim": "string",
      "claimed_metric": "string",
      "invalidation_condition": "string"
    }
  ],
  "source_tool": "string",
  "export_timestamp": "iso-8601",
  "privacy_flags": {
    "allow_report_share": false,
    "allow_research_desk_access": false
  },
  "files": [
    {
      "path": "trades.csv",
      "artifact_type": "trade_log_csv_v1",
      "sha256": "string"
    }
  ]
}
```

Recommended bundle files:

- `manifest.json`
- `trades.csv`
- `equity_curve.csv`
- `broker_fills.csv`
- `ohlcv/*.csv`
- `parameter_sweep.csv`
- `strategy_config.json`
- `benchmark.csv`
- `source_report.pdf`, `source_report.html`, or `source_report.json`

Full bundle unlocks:

- Truth Room Overview: Full
- Assumption Ledger: Full
- Execution Reality: Full
- Distribution And Edge Concentration: Full
- Monte Carlo Survival: Full
- Ruin And Capital Survival: Full
- Prop Evaluation Readiness: Full when rules are provided
- Regime Dependence: Conditional. Full upload confidence requires aligned OHLCV/context, explicit symbol coverage, timestamp alignment, and auditable regime definitions; portfolio-level multi-asset attribution should route to Research Desk when any of those are ambiguous.
- Parameter Stability: Conditional. A single parameter file is context only; true stability requires a multi-run parameter sweep with run-to-parameter mapping, per-run outcomes, and neighborhood topology. Otherwise route to Research Desk.
- Proof Report: Full
- Share Room: Full subject to plan
- Research Desk Review: Full packet

Honesty rule: `research_complete` means strongest upload eligibility, not automatic proof. Upload automation must always offer Research Desk when evidence is insufficient for true parameter stability, multi-asset regime attribution, broker-level execution realism, strategy reconstruction from config/report, portfolio-level exposure analysis, or an independent validation memo.

### Diagnostic Unlock Matrix

| Workspace | Trade log | Equity curve | Broker export | Backtest report | OHLCV/context | Parameter sweep | Strategy config | Prop rules | Full bundle |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Evidence Intake | Full | Full | Full | Full | Full | Full | Full | Full | Full |
| Truth Room Overview | Full | Limited | Limited | Limited | Limited | Limited | Limited | Limited context | Full |
| Assumption Ledger | Limited | Limited | Full for execution | Limited to Full | Limited | Limited | Full | Full for evaluation rules | Full |
| Execution Reality | Limited | No | Full | Limited claims audit | Limited proxy | No | Limited to Full | Limited constraints only | Full |
| Distribution | Full | No | Limited | No | No | Limited by run metrics | No | No | Full |
| Monte Carlo | Full | Limited | Limited | No | No | No | Limited sizing context | Rule thresholds only | Full |
| Ruin | Limited to Full | Limited | Limited | No | No | No | Full if sizing exists | Rule thresholds only | Full |
| Prop Evaluation Readiness | Limited to Full with sizing and timestamps | Limited path analysis | Fuller daily-loss evidence | Limited rule extraction only | No | Scenario comparison only | Sizing/risk assumptions | Full rules | Full |
| Regime Dependence | Limited | Limited | No | No | Conditional when aligned | Limited by run metrics | Limited | No | Conditional; Research Desk for attribution |
| Parameter Stability | No | No | No | No | No | Conditional with real sweep | Limited assumptions only | No | Conditional; Research Desk if no sweep |
| Proof Report | Full with limitations | Limited | Full execution appendix | Limited claim audit | Regime appendix | Parameter appendix | Assumptions appendix | Prop-readiness appendix | Full |
| Share Room | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Full subject to plan |
| Research Desk Review | Full packet component | Packet component | Packet component | Packet component | Packet component | Packet component | Packet component | Rule interpretation packet | Full |

## Launch Subscription Model For Approach A

Pricing should reflect the value of avoiding false confidence, not the commodity value of charting. The first launch should be simple enough to explain and strong enough to support a premium report product.

Principles:

- Free should demonstrate the falsification experience, not give away a full proof report.
- Paid individual should be attractive to serious solo traders.
- Pro should unlock the strongest automated due diligence loop when artifacts support it, while preserving evidence-gated limits.
- Team should support commercial evaluation, education businesses, and small research groups.
- Research Desk should monetize high-stakes ambiguity without turning the automated product into consulting.

### Recommended Launch Tiers

| Tier | Price | Best for | Core limit |
| --- | ---: | --- | --- |
| Free | $0/month | curious traders testing one artifact | 3 analyses/month |
| Truth Room Individual | $39/month | serious individual traders | 25 analyses/month |
| Truth Room Pro | $99/month | strategy sellers, educators, advanced researchers | 100 analyses/month |
| Research Lab Team | $399/month | small teams, prop evaluators, research desks | 250 analyses/month, 5 seats |
| Research Desk Review | from $1,000/review | high-stakes diligence or ambiguous evidence | scoped manual/agent-assisted validation memo |

### Permission Matrix

| Capability | Explorer | Individual | Pro | Team | Research Desk add-on |
| --- | --- | --- | --- | --- | --- |
| Trade CSV upload | Yes | Yes | Yes | Yes | Yes |
| Equity curve upload | Yes | Yes | Yes | Yes | Yes |
| Broker export upload | Preview only | Yes | Yes | Yes | Yes |
| Backtest report upload | Preview only | Yes | Yes | Yes | Yes |
| Full validation bundle | No | Limited | Yes | Yes | Yes |
| Truth Room Overview | Limited | Full | Full | Full | Full |
| Assumption Ledger | Limited | Full | Full | Full | Reviewer-enhanced |
| Execution Reality | Preview | Full | Full | Full | Reviewer-enhanced |
| Distribution | Limited | Full | Full | Full | Full |
| Monte Carlo | Limited | Full | Full | Full | Full |
| Ruin | Preview | Full | Full | Full | Reviewer-enhanced |
| Prop Evaluation Readiness | Fallback preview | 1 custom profile per analysis | saved profiles + report inclusion | shared firm profiles + comparisons | Reviewer-enhanced |
| Regime Dependence | Locked preview | Locked or add-on | Evidence-gated with aligned OHLCV/context | Evidence-gated with aligned OHLCV/context | Reviewer-enhanced attribution |
| Parameter Stability | Locked preview | Locked or add-on | Sweep-required and evidence-gated | Sweep-required and evidence-gated | Reviewer-designed/reviewed sweep |
| Proof Report export | Watermarked preview | PDF/Markdown | PDF/Markdown/JSON | PDF/Markdown/JSON | Addendum included |
| Controlled share links | No | 5 active links | 25 active links | 75 active links | Scoped to engagement |
| Report snapshots | Latest only | 30-day history | 1-year history | 2-year history | Included in packet |
| Analysis library | Latest 5 | Full personal | Full | Team-wide | Included |
| Research Desk request | Waitlist/contact | Paid add-on | Discounted add-on | Priority add-on | Included |
| API/webhook access | No | No | Limited beta | Limited beta | No |
| Support | Community/docs | Email | Priority | Priority/team admin | Scoped review channel |

### Tier Notes

Explorer:

- must make the user feel the product is hostile and useful
- should show unsupported claims and missing evidence
- should show Prop Evaluation Readiness as a fallback-profile preview, not as a specific firm pass/fail claim
- should not allow polished proof export
- should show locked Regime and Parameter pages with upload requirements

Individual:

- should be enough for a serious trader validating personal systems
- includes export because the report is the product
- includes custom prop evaluation rules per analysis and post-run recomputation because this is a high-intent individual use case
- can gate Regime and Parameter Stability if pricing pressure requires it, but the pages must remain visible

Pro:

- should be the default recommended paid tier for strategy sellers, educators, and advanced researchers
- unlocks the strongest automated suite when artifacts support it; it must not turn unsupported parameter, regime, broker, or portfolio claims into supported conclusions
- includes saved prop firm profiles, profile comparison, and prop-readiness inclusion in proof reports
- includes share links because external trust is a major buying reason

Team:

- should support multiple analysts and repeat due diligence
- includes shared prop firm rule templates and team-level comparison across strategies
- requires admin history, seat control, and stronger retention controls before being aggressively sold

Research Desk:

- should be sold as deep validation review, not investment advice
- should produce reviewer-approved report addenda tied to immutable report snapshots
- should focus on execution audit, data QA, benchmark construction, claim formalization, prop-rule interpretation, and parameter/regime review
- should always be offered when upload evidence is insufficient for true parameter stability, multi-asset regime attribution, broker-level execution realism, strategy reconstruction from config/report, portfolio-level exposure analysis, or an independent validation memo

### Commercial Packaging Rules

Locked pages should not disappear. They should show:

- what the diagnostic would answer
- what artifact is missing
- what the user's current plan includes
- what upgrade or upload unlocks it
- why the product refuses to make a stronger claim

The upgrade path should be tied to trust, not artificial scarcity:

- "Your artifact does not support parameter stability" is an evidence limitation.
- "Your plan does not include parameter stability exports" is an entitlement limitation.
- The UI must never confuse the two.

### Implementation Implications

The current product is not sellable until the core workbench surfaces are rebuilt around falsification rather than chart presentation.

Highest-priority implementation gaps:

- create the Assumption Ledger workspace
- make Regime and Parameter Stability visible in the sidebar even when gated or evidence-limited
- add Prop Evaluation Readiness as a visible workspace with fallback rules, custom runtime rules, post-run recomputation, and exact rule snapshots
- replace old card language with verdict-led evidence instruments
- add unsupported-claims inventory across Overview and Report
- make export/report generation a primary CTA on completed analyses
- make Share Room render from immutable report snapshots
- add upload templates for each artifact class
- add bundle manifest parsing
- separate evidence limitations from subscription locks in every workspace
- add Research Desk CTA from limitations, not only from marketing pages

The first product is ready to sell only when a user can upload a messy artifact, understand what it proves, see what it fails to prove, export a defensible memo, and know the next evidence needed to strengthen the verdict.

## First-Client Readiness Roadmap

This roadmap is the implementation bridge from the current two-repo state to a sellable Approach A product: Strategy Truth Room as an artifact-first validation product. The goal is not to build the full research operating system yet. The goal is to create a world-class first product that can take a real client's strategy evidence, falsify it honestly, produce a defensible report, and expose the next evidence required.

The repos already contain important foundations:

- `bulletproof_bt` has the deterministic analysis seam, diagnostic envelopes, capability profiles, trade-based diagnostics, Monte Carlo, execution sensitivity, ruin, regime analysis, parameter stability, and report payloads.
- `invariance_research` has auth, local and Postgres persistence, upload inspection, bundle intake, evidence ledger projection, analysis workers, diagnostic workspaces, chart adapters, report snapshots, export jobs, share tokens, Research Desk records, admin operations, and public product pages.

The product is still not first-client ready because the foundations are not yet unified into a strict Strategy Truth Room contract. The current product can run useful analyses, but it does not yet consistently behave like a hostile validation room that tracks evidence, claims, assumptions, artifact sufficiency, report immutability, share safety, and paid-tier boundaries as first-class product objects.

### Current Product Audit

| Area | Current State | Sellable Gap | Primary Repo |
| --- | --- | --- | --- |
| Product framing | Public pages now describe validation, execution realism, report output, and Research Desk handoff. | App workbench still needs to fully embody "upload evidence, get hostile validation" on every completed-analysis surface. | `invariance_research` |
| Design system | Research-red visual system, artifact surfaces, evidence panels, metric instruments, public-page refinements, and report snapshot UI exist. | Some analysis pages still mix old card grammar with new evidence instruments; workbench IA is not yet cohesive page-to-page. | `invariance_research` |
| Artifact intake | CSV and ZIP uploads exist; generic trade CSV and generic bundle parser exist; upload inspection returns eligibility and an evidence ledger. | Needs canonical Strategy Truth Room bundle manifest, richer artifact families, templates, claim intake, stronger bundle validation, and file-level provenance. | Both |
| Evidence ledger | Upload and engine statuses are reconciled into a diagnostic evidence ledger. | Needs to become a persistent evidence object, not only a diagnostic availability projection. It must include artifact facts, diagnostic unlock reasons, claims, assumptions, contradictions, and report-safe summaries. | `invariance_research` |
| Assumption handling | Diagnostics emit assumptions, limitations, and recommendations; UI has context panels. | No normalized Assumption Ledger workspace yet. Assumptions are not source-linked, materiality-scored, contradicted, or tied to verdict movement. | Both |
| Unsupported claims | Some copy and limitations warn about missing evidence. | No engine-native or app-native claim inventory that says which claims are supported, unsupported, contradicted, or outside scope. | Both |
| Diagnostic coverage | Overview, Execution, Distribution, Monte Carlo, Ruin, Regime, Parameter Stability, and Report routes exist. | The routes need a shared analyst-workbench grammar and stronger falsification content. Gated/unavailable states must remain visible and useful. | `invariance_research` |
| Engine diagnostics | `bulletproof_bt` supports core Approach A diagnostics with degradation behavior. | Diagnostics need stronger artifact-aware realism: broker fills, richer cost/slippage schedules, regime-aware Monte Carlo, stronger parameter topology, asset-class capability statements, and proof-report payloads. | `bulletproof_bt` |
| Report export | Report page has export actions; export queue can render JSON/Markdown/PDF from snapshots. | Export must become a polished validation memo contract with evidence coverage, assumption ledger, unsupported claims, hashes, schema versions, redaction state, and share-room projection. | Both |
| Share Room | Report snapshots, share tokens, share routes, and access events exist. | Needs productized Share Room: privacy controls, revoked/expired states, recipient-safe memo view, access log, redaction policy, and threat-model enforcement. | `invariance_research` |
| Command and alert workflow | Report, share, export, and analysis actions exist as separate UI surfaces. | Needs a validation command palette, evidence alert center, explain layer, and connected case-file timeline so serious users can move through validation at terminal speed. | `invariance_research`, minor `bulletproof_bt` |
| Subscription model | Entitlements exist for `explorer`, `professional`, `research_lab`, and `advisory`; diagnostic locks exist. | Needs launch tiers aligned to the new Free/Individual/Pro/Team/Research Desk matrix, pricing, copy, Stripe mapping, and per-diagnostic unlock states. | `invariance_research` |
| Research Desk | Request records, admin page, addenda, and report-page CTA exist. | Needs full handoff packet, human/agent review workflow, addendum approval policy, pricing path, and first-client operating procedure. | `invariance_research` |
| Reliability | Workers, retries, export queue, admin ops, and Postgres schema initialization exist. | Needs end-to-end first-client readiness harness across local SQLite, production Postgres, worker startup, artifact storage, email, exports, and share links. | Both |

### Engine Audit: `bulletproof_bt`

The engine is strong enough to be the foundation of Approach A, but it is not yet the final Strategy Truth Room engine contract.

Strengths already present:

- deterministic SaaS seam: `StrategyRobustnessLabService.run_analysis_from_parsed_artifact`
- structured models for normalized trades, parsed artifacts, analysis run config, engine envelope, run context, diagnostic status, and capability profiles
- diagnostic names covering overview, distribution, Monte Carlo, stability, execution, regimes, ruin, and report
- trade-only degradation and capability-profile signaling
- execution stress, distribution diagnostics, Monte Carlo simulation, ruin diagnostics, OHLCV-gated regime analysis, parameter sweep stability, and report output
- tests covering parsed-artifact engine seam behavior, trade-only degradation, skipped diagnostics, OHLCV regime metrics, parameter stability shape, and richer trade artifacts

Engine gaps to close:

| Requirement | Current Engine State | Required Change | Phase |
| --- | --- | --- | --- |
| Canonical artifact families | Engine artifact kinds are still coarse: trade CSV, artifact bundle, parameter sweep. | Add typed Strategy Truth Room artifacts: trade log, equity curve, broker export, backtest report, strategy config, OHLCV context, benchmark series, parameter sweep, declared claims, and full validation bundle. | Phase 1 |
| Full bundle manifest | Engine can consume parsed artifact input but does not own the full Strategy Truth Room bundle manifest contract. | Add manifest validation or shared contract tests so app and engine agree on file roles, hashes, schema versions, required/optional files, and unlock rules. | Phase 1 |
| Asset-class validation | Normalized trades are broadly asset-class agnostic. | Add explicit asset-class capability profile: crypto, FX, equity, index, futures/CFD where supported; mark unsupported execution details when broker/venue fields are absent. | Phase 1 |
| Assumption Ledger | Assumptions exist as diagnostic strings. | Emit normalized assumptions with source, diagnostic, materiality, confidence, testability, verdict impact, rescue evidence, and share-safe wording. | Phase 2 |
| Unsupported claims | No declared-claim support model. | Add claim objects and evidence mapping: supported, partially supported, unsupported, contradicted, outside scope. | Phase 2 |
| Execution realism | Execution stress exists but is largely deterministic and scenario-based. | Add broker/fill-aware execution audit, fee model provenance, spread/slippage sensitivity schedules, venue/timeframe caveats, and "execution fantasy" verdict triggers. | Phase 3 |
| Cost sensitivity | Fee/slippage stress exists. | Make stress ladders configurable from artifact, broker profile, asset class, and user assumptions; emit break-even friction and cost-kill threshold. | Phase 3 |
| Distribution falsification | Distribution metrics exist. | Add rare-trade dependence, winner concentration, tail asymmetry, liquidity-sensitive outlier handling, and edge-source classification. | Phase 3 |
| Monte Carlo | IID bootstrap Monte Carlo exists. | Add block bootstrap, regime-conditioned resampling when context exists, serial-dependence warning, and simulation model disclosure. | Phase 3 |
| Ruin | Ruin can run with account/risk config. | Make account and sizing assumptions explicit in the Assumption Ledger, avoid advice-like deployment language, and emit capital survival verdicts as validation statements. | Phase 3 |
| Regime | OHLCV-gated technical regimes exist. | Add regime definition metadata, regime coverage sufficiency, thin-sample warnings, optional benchmark-relative regimes, and future extension hooks for macro/event/liquidity regimes. | Phase 3 |
| Parameter stability | Parameter sweep support exists. | Add plateau/cliff topology, sensitivity by parameter, robust-region scoring, optimization target disclosure, and unsupported-sweep reasons. | Phase 3 |
| Proof report payload | Report payload exists. | Emit a report contract with verdict taxonomy, evidence coverage, assumptions, limitations, unsupported claims, diagnostic confidence, artifact hashes, and Research Desk packet hooks. | Phase 5 |
| Verdict taxonomy | Engine verdicts are closer to robust/conditional/fragile/not-ready. | Map to Strategy Truth Room verdicts: structurally credible, promising but under-supported, likely overfit, execution-fantasy, data-insufficient, regime-dependent, untradeable after costs. | Phase 5 |

### App Audit: `invariance_research`

The web app has enough infrastructure to become the product surface, but the UX and data model must now be tightened around falsification.

Strengths already present:

- auth and account provisioning with local SQLite and production Postgres paths
- upload inspection API with CSV and ZIP handling
- generic trade CSV parser, generic bundle parser, semantic validation, and artifact storage
- evidence ledger projection that reconciles artifact eligibility and engine capability profile
- analysis queue, retry path, status polling, and worker runtime
- diagnostic routes for the required core workspaces
- chart adapters and dashboard components
- report page with export actions
- export queue and PDF/Markdown/JSON renderers
- report snapshots, share tokens, share access events, Research Desk requests, reviewer addenda, and admin pages
- entitlement policy and diagnostic lock models

App gaps to close:

| Requirement | Current App State | Required Change | Phase |
| --- | --- | --- | --- |
| Strategy Truth Room intake | New Analysis accepts CSV/ZIP and shows eligibility. | Rebuild as Evidence Intake: artifact classification, file manifest review, unlock matrix, missing evidence prompts, templates, and bundle repair guidance. | Phase 1 |
| Canonical contracts | App has `bundle_v1`, `trade_csv`, `trade_history_bundle`, `backtest_result_bundle`, `research_bundle`. | Align contracts with the artifact schemas in this design doc and the engine models; version all file-level schemas. | Phase 1 |
| Evidence ledger persistence | Upload response returns an evidence ledger projection. | Persist evidence ledger snapshots per artifact and per analysis; display them consistently across workspaces and exports. | Phase 2 |
| Assumption Ledger route | No dedicated route. | Add `/app/analyses/[id]/assumptions` and make it a first-class sidebar item. | Phase 2 |
| Claim inventory | No declared-claim workspace. | Add claim intake, claim extraction from reports/config where possible, unsupported-claim inventory, and report integration. | Phase 2 |
| Workbench IA | Workspaces exist but are uneven in hierarchy, copy, and evidence density. | Redesign all analysis pages into a coherent analyst workbench with shared page anatomy, state blocks, diagnostics, figures, attack panels, and next-evidence prompts. | Phase 4 |
| Sidebar visibility | Core routes exist. | Ensure all diagnostics are always visible; show evidence-limited and plan-locked states without hiding the page. | Phase 4 |
| Analysis Library | Exists as run list. | Upgrade to case library with artifact richness, verdict, report status, share status, Research Desk status, and next-evidence filter. | Phase 4 |
| Report artifact | Export exists but is still mostly renderer-driven. | Build proof report model and visual report surface around verdict, coverage, assumptions, unsupported claims, diagnostic confidence, limitations, appendices, and share policy. | Phase 5 |
| Share Room | Share routes and tokens exist. | Build recipient-facing Share Room with report summary, redaction boundary, token status, expiration, access events, and revoke controls. | Phase 5 |
| Command/explain layer | Actions exist as scattered buttons and page links. | Add validation command palette, evidence alert center, explain drawer, saved validation questions, and case-file event timeline. | Phase 5.5 |
| Prop Evaluation Readiness | No dedicated prop-evaluation workspace or rule profile model. | Add runtime rule capture, fallback profiles, post-run recomputation, versioned rule snapshots, breach/profit-target simulation, report appendix, and tier-aware profile limits. | Phase 5.6 |
| Subscription tiers | Entitlements exist but names and gates do not yet match launch packaging. | Update tier model, pricing copy, Stripe mapping, upgrade page, pricing page, billing page, and diagnostic lock messaging. | Phase 6 |
| Research Desk handoff | Requests and addenda exist. | Add request wizard, packet generation, admin triage states, reviewer checklist, client-facing addenda, and status timeline. | Phase 7 |
| Production readiness | Admin ops and schema auto-init exist. | Add migration discipline, startup checks, email deliverability, worker launch docs, storage checks, share security checks, and first-client smoke tests. | Phase 8 |

### Implementation Phases

Each phase below should be implemented as a Codex slice with tests and a short verification note. Phases are ordered to avoid building polished pages on unstable contracts.

### Phase 0: Freeze The Strategy Truth Room Contract

Repo ownership:

- `bulletproof_bt`: diagnostic output contract, engine fixture outputs, version constants
- `invariance_research`: app contract types, intake contracts, report/share contracts
- Both repos: cross-repo fixture pack and contract tests

Purpose:

Create a single contract spine so the app and engine stop drifting. This phase should not chase visual polish. It should define the artifacts, statuses, verdicts, evidence objects, and report payloads that every later phase uses.

Implementation tasks:

- define `strategy_truth_room_contract_version`
- define canonical diagnostic names and page slugs:
  - overview
  - execution
  - distribution
  - monte_carlo
  - ruin
  - prop_evaluation_readiness
  - regimes
  - stability
  - assumptions
  - report
  - share_room
  - research_desk
  - library
- define canonical artifact families:
  - `trade_log_v1`
  - `equity_curve_v1`
  - `broker_export_v1`
  - `backtest_report_v1`
  - `strategy_config_v1`
  - `prop_evaluation_rules_v1`
  - `ohlcv_context_v1`
  - `benchmark_series_v1`
  - `parameter_sweep_v1`
  - `declared_claims_v1`
  - `strategy_truth_room_bundle_v1`
- define canonical verdict taxonomy:
  - structurally credible
  - promising but under-supported
  - likely overfit
  - execution-fantasy
  - data-insufficient
  - regime-dependent
  - untradeable after costs
- define canonical evidence states:
  - supported
  - limited
  - unsupported
  - contradicted
  - unavailable
  - plan_locked
  - pending_review
- define immutable IDs and hashes required for artifacts, analyses, reports, snapshots, shares, and Research Desk packets
- add fixture files representing:
  - trade-only CSV
  - trade CSV with fees and R-multiples
  - full validation bundle
  - bundle missing OHLCV
  - bundle missing parameter sweep
  - broker export with fills
  - equity-curve-only artifact
  - prop evaluation rules profile
  - malformed bundle
- add cross-repo contract tests:
  - app fixture parses into engine-accepted payload
  - engine emits app-accepted diagnostic envelope
  - missing artifact inputs produce evidence-limited, not silent failure
  - unsupported diagnostic remains visible in app navigation
  - report payload includes evidence coverage and limitations

Definition of done:

- both repos expose matching contract version constants
- a fixture from `invariance_research` can be sent through the `bulletproof_bt` bridge and validated against app contracts
- existing tests continue to pass
- contract drift fails tests before reaching UI work

### Phase 1: Artifact Intake And Bundle Manifest

Repo ownership:

- `bulletproof_bt`: expand parsed artifact models, capability profiles, and artifact-aware diagnostic requirements
- `invariance_research`: upload UI, parser routing, manifest validation, storage metadata, artifact templates
- Both repos: fixture and manifest contract tests

Purpose:

Make the product artifact-first in a way a real client can understand. The user should know exactly what was accepted, what each file unlocked, what is missing, and what the system refuses to infer.

Implementation tasks in `bulletproof_bt`:

- expand artifact kind/type models to match the canonical artifact families
- accept optional declared claims, strategy config, broker fills, benchmark series, OHLCV context, equity curve, and parameter sweep metadata
- emit artifact-family-aware capability profiles
- emit file-level required inputs and optional enrichments per diagnostic
- emit asset-class capability statements:
  - crypto supported when symbol, timestamp, quantity, prices, fees, exchange are present
  - FX supported when pair, timestamp, quantity/lot, entry/exit, costs/spread assumptions are present
  - equities/indexes supported when symbol, timestamp, shares/contracts, prices, fees, and market calendar assumptions are present
  - futures/CFDs marked limited unless contract multiplier, tick value, margin, and session data are supplied
- ensure engine does not imply broker-grade execution realism when only trade-level rows are provided

Implementation tasks in `invariance_research`:

- replace generic upload copy with Evidence Intake language
- add artifact-type selector only as a fallback; prefer automatic classification
- add manifest review UI:
  - file list
  - schema version
  - checksum
  - recognized/ignored/rejected
  - diagnostic unlocks
  - missing evidence
  - parser warnings
- add downloadable templates:
  - trade log CSV
  - equity curve CSV
  - broker export mapping guide
  - OHLCV context CSV
  - parameter sweep CSV
  - declared claims JSON
  - full validation bundle ZIP example
- persist file-level artifact metadata and parser provenance
- distinguish artifact rejection, artifact-limited, and plan-limited states
- support equity-curve-only inspection as limited, not as an invalid strategy when possible
- show full-bundle readiness score before run creation

Definition of done:

- a user can upload a trade CSV and see exactly which diagnostics are available, limited, or unavailable
- a user can upload a full bundle and see the full-suite unlock matrix
- a malformed bundle produces repair instructions
- a bundle with extra files does not silently discard them without showing ignored/unsupported status
- engine and app agree on diagnostic unlock status

### Phase 2: Evidence Ledger, Assumption Ledger, And Claim Inventory

Repo ownership:

- `bulletproof_bt`: emit normalized assumptions, evidence facts, and claim-support objects
- `invariance_research`: persist/display ledgers, add Assumption Ledger workspace, add claim inventory UI

Purpose:

Turn the product from "analysis pages" into a truth room. The user should see what assumptions produced the result, what claims the artifact cannot support, and what evidence would change the verdict.

Implementation tasks in `bulletproof_bt`:

- emit `assumption_ledger` entries with:
  - assumption id
  - source: user, parser, engine default, inferred, missing
  - diagnostic
  - statement
  - materiality: low, medium, high, critical
  - confidence
  - falsification test
  - affected metrics
  - verdict impact
  - rescue evidence
  - share-safe wording
- emit `evidence_facts` with:
  - file/source id
  - field/value summary
  - diagnostic relevance
  - provenance
  - confidence
- support `declared_claims_v1`
- emit `claim_inventory` with:
  - claim id
  - claim text
  - source
  - support status
  - supporting diagnostics
  - contradicting diagnostics
  - missing evidence
  - report wording
- add tests for assumptions and claims in trade-only, full-bundle, and missing-context scenarios

Implementation tasks in `invariance_research`:

- persist evidence ledger snapshots on upload and analysis completion
- add `Assumption Ledger` sidebar item
- build Assumption Ledger workspace with:
  - critical assumptions
  - engine defaults
  - user-declared assumptions
  - inferred assumptions
  - contradictions
  - missing evidence
  - "what would rescue this" actions
- add unsupported claims panel to Overview and Report
- add declared-claims intake field/file support
- make assumptions and claims exportable in JSON/PDF
- make Research Desk request prefill with critical assumptions and unsupported claims

Definition of done:

- every completed run has an evidence ledger and assumption ledger
- every report has an unsupported-claims section, even if empty
- Overview answers "what assumptions produced this result?"
- Report answers "what this result does not prove"
- Research Desk packet can be generated from the ledger state

### Phase 3: Engine Diagnostic Hardening

Repo ownership:

- `bulletproof_bt`: primary owner
- `invariance_research`: update mappers, charts, copy, fixtures, and workbench states as engine output expands

Purpose:

Make the diagnostics hostile enough that a serious trader, seller, educator, or allocator would trust the output as due diligence rather than a prettified backtest summary.

Implementation tasks in `bulletproof_bt`:

- Execution:
  - add broker/fill-aware audit when broker export is supplied
  - classify missing fees, suspicious zero-cost fills, impossible fill timestamps, duplicate fills, partial fills, and venue/timezone ambiguity
  - compute break-even cost and break-even slippage
  - emit cost-kill threshold
  - emit execution-fantasy verdict triggers
- Distribution:
  - compute rare-trade dependence
  - compute top-N trade contribution
  - compute winner concentration and loser concentration
  - flag strategies where edge comes from too few trades
  - separate gross vs net distribution when fees exist
- Monte Carlo:
  - add IID bootstrap disclosure
  - add block bootstrap mode
  - add regime-conditioned bootstrap when OHLCV/regimes exist
  - emit path survival, drawdown envelope, time-under-water, and model limitations
- Ruin:
  - require explicit capital and risk assumptions for full confidence
  - separate validation language from investment advice
  - emit sizing fragility and survivability status
- Regime:
  - emit regime definition metadata
  - warn on thin samples per regime
  - emit regime dominance and adverse-regime decay
  - add benchmark-relative regime hooks
- Parameter Stability:
  - parse parameter sweep files into parameter surfaces
  - compute robust region, cliff risk, plateau width, local optimum dependence, and optimization target risk
  - emit stability confidence only when sweep coverage is sufficient
- Report:
  - emit diagnostic confidence per section
  - emit proof-report payload with assumptions, unsupported claims, evidence coverage, and artifact provenance

Implementation tasks in `invariance_research`:

- update bridge types and mappers for new diagnostic fields
- add charts/instruments for:
  - cost-kill threshold
  - rare-trade dependence
  - Monte Carlo survival envelope
  - regime dominance
  - parameter plateau/cliff map
  - assumption materiality
- add regression tests so missing engine fields degrade cleanly

Definition of done:

- engine can falsify a strategy under worse fills, higher fees, rare-trade removal, adverse regimes, Monte Carlo path stress, ruin assumptions, and parameter perturbation
- app displays every expanded diagnostic without raw JSON leakage
- each diagnostic produces a verdict-driving statement, not only charts

### Phase 4: Analyst Workbench IA And Page Redesign

Repo ownership:

- `invariance_research`: primary owner
- `bulletproof_bt`: minor support for output naming and payload consistency

Purpose:

Make the dashboard the core sellable product. The user should feel they are inside a rigorous analyst workbench, not a generic metrics dashboard.

Shared page anatomy:

- verdict strip:
  - current diagnostic posture
  - evidence state
  - artifact dependency
  - plan state if locked
- attack question:
  - one direct falsification question the page answers
- evidence instruments:
  - metrics, charts, scenarios, and tables presented as instruments
- assumption and limitation rail:
  - source-linked assumptions
  - material limitations
  - unsupported claims
- next evidence:
  - what to upload or request next
- report impact:
  - how this page changes the final memo

Page-specific implementation:

- Overview:
  - truth-room verdict
  - credibility score
  - evidence coverage
  - strongest support
  - strongest doubt
  - unsupported claims
  - next experiment
- Execution:
  - execution realism audit
  - cost/slippage sensitivity
  - broker/fill anomalies
  - cost-kill threshold
  - execution-fantasy triggers
- Distribution:
  - payoff anatomy
  - rare-trade dependence
  - outlier removal sensitivity
  - winner concentration
  - gross vs net behavior
- Monte Carlo:
  - simulation model disclosure
  - survival envelope
  - drawdown burden
  - path dependence
  - regime-aware mode when available
- Ruin:
  - capital/risk assumptions
  - ruin probability
  - sizing fragility
  - survival under reduced/increased risk
  - non-advisory language
- Regime:
  - regime definition
  - coverage sufficiency
  - favorable/adverse regime split
  - regime concentration
  - missing context state
- Parameter Stability:
  - sweep coverage
  - robust plateau
  - cliff zones
  - local optimum dependence
  - missing sweep state
- Assumption Ledger:
  - critical assumptions
  - engine defaults
  - inferred assumptions
  - user-declared assumptions
  - contradictions
  - rescue evidence
- Report:
  - proof memo preview
  - evidence coverage
  - limitations
  - unsupported claims
  - assumptions
  - export/share CTA
- Share Room:
  - recipient-safe report view
  - redaction notice
  - access state
  - snapshot identity
- Research Desk:
  - request scope
  - packet contents
  - status timeline
  - reviewer addenda
- Library:
  - case list
  - artifact richness
  - verdict
  - evidence gaps
  - report/share status
  - Research Desk status

Definition of done:

- all required pages exist and are reachable from navigation
- locked or evidence-limited pages are visible and useful
- no completed-analysis page uses generic "card dump" structure as its dominant pattern
- all pages answer a falsification question
- all pages show what evidence is missing

### Phase 5: Proof Report, Export, And Share Room

Repo ownership:

- `bulletproof_bt`: proof-report payload and verdict inputs
- `invariance_research`: report UI, snapshot service, export renderer, Share Room, redaction/access controls

Purpose:

Make the report the demand object. A user should pay because the memo is defensible, shareable, and precise about its limits.

Implementation tasks in `bulletproof_bt`:

- emit proof-report sections:
  - executive verdict
  - artifact identity
  - evidence coverage
  - assumptions
  - unsupported claims
  - diagnostic confidence
  - falsification results
  - limitations
  - next evidence
  - Research Desk recommended scope
- emit report-safe language that avoids investment advice
- map engine verdicts to Strategy Truth Room taxonomy

Implementation tasks in `invariance_research`:

- version report snapshots with:
  - analysis id
  - artifact ids and hashes
  - report schema version
  - generated timestamp
  - redaction policy
  - included diagnostics
  - excluded diagnostics and reasons
- rebuild PDF export around proof-report structure
- add Markdown and JSON export parity
- add report preview state before export
- add snapshot regeneration and superseded-state behavior
- build Share Room:
  - token creation
  - expiration
  - revoke
  - recipient view
  - report download policy
  - access-event log
  - private fields redaction
- implement Report Sharing Threat Model from the design doc:
  - no raw trade files in public share by default
  - no PII in share payload
  - no owner account ids exposed
  - snapshot cannot mutate after share creation
  - revoked tokens fail closed
  - expired tokens fail closed

Definition of done:

- a completed run can generate a polished PDF
- the PDF contains evidence coverage, assumptions, unsupported claims, limitations, and next evidence
- a share link renders a recipient-safe report
- revoked/expired shares cannot be accessed
- report snapshots are immutable enough for client trust

### Phase 5.5: Validation Command Layer, Explainability, And Evidence Alerts

Repo ownership:

- `invariance_research`: primary owner for command palette, alert center, explain UI, case-file timeline, saved validation questions, and report/workbench actions
- `bulletproof_bt`: stable reason codes, diagnostic explanation payloads, next-evidence labels, and report-safe wording where the app cannot infer them safely

Purpose:

Add the Bloomberg-inspired workflow layer that makes the first product feel like a serious validation terminal without exposing the full research operating system. Users should be able to ask the product direct questions, jump to the right evidence object, understand why the verdict moved, and see which report events affect trust.

This phase is intentionally after Phase 5 because commands, alerts, and explanations need stable report snapshots, share events, exports, assumptions, limitations, and unsupported claims. It is before Phase 6 because this command layer also becomes the cleanest place to explain plan locks versus evidence locks.

Implementation tasks in `invariance_research`:

- add a keyboard-accessible validation command palette available on every analysis workspace page
- define an explicit command registry with permission-aware and artifact-aware actions:
  - explain verdict
  - show missing evidence
  - open Assumption Ledger
  - open unsupported claims
  - open report snapshot
  - export PDF
  - export Markdown
  - export JSON
  - create share link
  - revoke share link
  - compare previous run when a strategy lineage exists
  - request Research Desk review
  - show diagnostics blocked by artifact
  - show diagnostics blocked by subscription
  - open Prop Evaluation Readiness
  - edit prop evaluation rules
  - recompute prop evaluation readiness
- add saved validation questions as first-class shortcuts:
  - what assumptions produced this result?
  - what happens if fills get worse?
  - what happens if fees change?
  - where does this strategy fail by regime?
  - how much edge comes from rare trades?
  - would this strategy breach my prop evaluation rules?
  - what evidence is missing?
  - what does this report not prove?
- add an Evidence Alert Center backed by persisted evidence events:
  - snapshot generated
  - snapshot superseded
  - export completed
  - export failed
  - share created
  - share viewed
  - share expired
  - share revoked
  - diagnostic unlocked by richer artifact
  - diagnostic unavailable because evidence is insufficient
  - diagnostic unavailable because plan is insufficient
  - high-materiality assumption emitted
  - unsupported claim blocks confidence
  - Research Desk packet created
- add a per-analysis connected case-file timeline:
  - upload accepted
  - artifact classified
  - evidence ledger snapshot created
  - analysis queued
  - diagnostics completed or skipped
  - verdict generated
  - report snapshot generated
  - export/share events
  - Research Desk request and addendum events
- add an explain drawer or panel that answers:
  - why this verdict?
  - why this diagnostic is limited?
  - what input would unlock the missing diagnostic?
  - what changed since the previous snapshot?
  - what evidence would rescue a weak or unsupported claim?
- make every command produce a product-safe empty state when the action is blocked by missing evidence, missing plan rights, missing lineage, or missing snapshot
- make the Report page, Overview page, Library, Share Room owner controls, and Assumption Ledger link into the same command/event model instead of each inventing separate action logic
- add tests for command availability, permission gating, evidence gating, alert persistence, event ordering, and redaction-safe explanation payloads

Implementation tasks in `bulletproof_bt`:

- emit stable explanation reason codes for verdicts, skipped diagnostics, warnings, material assumptions, and unsupported claims where missing
- emit `next_evidence` labels that the app can display without rewriting engine meaning
- ensure diagnostic outputs distinguish:
  - artifact limitation
  - model limitation
  - execution assumption limitation
  - sample-size limitation
  - unavailable context data
  - unsupported asset-class detail
- keep every reason code deterministic and schema-versioned
- do not change backtest, execution, no-lookahead, fill, cost, or simulation semantics in this phase

Definition of done:

- a user can open the command palette from any completed-analysis workspace page
- command results route to the correct workspace, drawer, export, share, or Research Desk action
- every command is artifact-aware, plan-aware, and redaction-aware
- the alert center shows report, share, export, diagnostic, assumption, and claim events
- the case-file timeline makes the full evidence chain navigable from artifact to report
- the explain layer can answer verdict, limitation, unlock, changed-snapshot, and rescue-evidence questions without exposing raw private files
- no internal research daemon memory, alpha notes, or cross-tenant patterns leak into customer reports

### Phase 5.6: Prop Evaluation Readiness

Repo ownership:

- `bulletproof_bt`: prop evaluation rule schema, feasibility engine, breach simulation, Monte Carlo breach/profit-target estimates, explanation reason codes, and deterministic fixtures
- `invariance_research`: runtime rule capture, post-run rule editing, Prop Evaluation Readiness workspace, saved rule profiles, entitlement gates, report/share projections, and recomputation workflow

Purpose:

Add a commercially sharp diagnostic for funded-account and prop-firm evaluation users: can this strategy pass the user's actual challenge rules without breaching daily-loss, total-drawdown, consistency, or time constraints?

This belongs after Phase 5.5 because it reuses stable snapshots, command actions, explain panels, alerts, and case-file events. It should land before Phase 6 subscription alignment because it materially improves the value of Individual, Pro, Team, and Research Desk tiers.

Product rules:

- Use the product name **Prop Evaluation Readiness**.
- Treat all outputs as validation diagnostics, not trading or financial advice.
- Never say the user is guaranteed to pass a prop firm challenge.
- Never imply affiliation with or endorsement by any prop firm unless a real partnership exists.
- Always show the exact rule snapshot used for the conclusion.
- Clearly label fallback rules as preview assumptions.
- Specific firm readiness requires user-entered, saved, or reviewer-confirmed rules.
- Missing intraday equity, open PnL, timestamps, sizing, or broker fill data must degrade daily-loss conclusions to limited or unknown.

Implementation tasks in `bulletproof_bt`:

- add `PropEvaluationRulesV1` model with:
  - profile id and label
  - account size
  - profit target
  - maximum total drawdown
  - total drawdown basis: static, trailing balance, trailing equity, end-of-day trailing
  - maximum daily loss
  - daily-loss basis: intraday equity, closed balance, end-of-day balance
  - reset timezone
  - minimum trading days
  - maximum evaluation days
  - consistency rule
  - max lot, exposure, leverage, or position limits where provided
  - weekend, news, or holding restrictions as declared constraints
- add `PropEvaluationReadinessResultV1` with:
  - result id
  - rule snapshot hash
  - data sufficiency status
  - pass/fail/limited/unknown verdict
  - first breach event
  - max daily loss observed versus allowed
  - max total drawdown observed versus allowed
  - profit target progress and target-hit date if reached
  - rule-by-rule status table
  - breach margin and safety buffer
  - estimated breach probability under eligible Monte Carlo paths
  - estimated target-before-breach probability where supported
  - improvement levers as non-advisory diagnostics
  - explanation reason codes
- implement deterministic feasibility simulation from trade log and/or equity curve artifacts
- support account path reconstruction from:
  - trade PnL
  - trade R-multiples with risk/account assumptions
  - equity curve
  - broker/export fills when available
- implement daily reset and timezone handling with tests
- implement total-drawdown modes with tests for static and trailing drawdown rules
- implement consistency-rule checks such as single-day profit concentration where configured
- emit clear limitations for restrictions the engine cannot verify from uploaded data, such as news trading or weekend holding
- add fixture payloads for:
  - strategy passes fallback profile
  - strategy hits profit target but breaches daily loss
  - strategy breaches trailing drawdown
  - strategy fails consistency rule
  - artifact lacks intraday path and daily-loss result is limited
  - user-entered custom rules recompute against the same run
- expose the readiness result through the SaaS analysis seam without changing unrelated diagnostic semantics

Implementation tasks in `invariance_research`:

- add runtime optional rule capture to the analysis setup flow:
  - fallback profile selected by default
  - manual custom rules
  - saved profile selection for eligible tiers
  - clear "preview only" label when fallback rules are used
- add persistence for:
  - prop evaluation rule profiles
  - per-analysis rule snapshots
  - readiness result snapshots
  - recomputation jobs/events
- add `/app/analyses/[id]/prop-evaluation` workspace with:
  - readiness verdict strip
  - rule profile summary
  - rule-by-rule pass/fail/limited table
  - first breach timeline
  - profit-target progress
  - daily-loss and total-drawdown buffers
  - target-before-breach estimate where supported
  - missing-evidence prompts
  - non-advisory improvement levers
  - edit-rules and recompute controls
- add post-run rule editing:
  - user can replace fallback rules with actual firm rules
  - user can edit previously entered rules
  - recomputation uses saved analysis artifacts and does not rerun unrelated diagnostics
  - each recomputation creates a new rule snapshot and result snapshot
  - previous results remain visible or auditable with timestamps and rule hashes
- add commands and alerts:
  - open Prop Evaluation Readiness
  - edit prop rules
  - recompute prop readiness
  - prop readiness recomputed
  - prop readiness changed after rule edit
  - fallback rules replaced by user rules
- add proof-report integration:
  - include readiness appendix only when plan and artifact rights allow it
  - include exact rule snapshot and limitations
  - include fallback-profile warning if applicable
  - include share-safe prop readiness summary for eligible share links
- add entitlement rules:
  - Explorer: fallback preview only, no specific firm claim, no export appendix
  - Individual: one custom profile per analysis, post-run recomputation, report summary
  - Pro: saved profiles, profile comparison, report appendix, share-safe summary
  - Team: shared firm templates, team profile library, cross-strategy comparison
  - Research Desk: reviewer-confirmed rule interpretation and addendum
- add tests for:
  - runtime rule submission
  - fallback-rule labeling
  - post-run recomputation
  - snapshot immutability
  - entitlement gates
  - report/share redaction
  - missing-evidence degraded states
  - command and alert wiring

Definition of done:

- a user can run an analysis with fallback rules or custom prop evaluation rules
- a completed analysis exposes a Prop Evaluation Readiness workspace
- a user can edit the rules after the run and recompute readiness without rerunning unrelated diagnostics
- every readiness result is tied to a versioned rule snapshot
- reports and share rooms include prop readiness only with correct tier, redaction, and limitation handling
- missing data produces limited/unknown states rather than fake precision
- the feature has deterministic cross-repo fixtures for pass, breach, limited, and recompute scenarios

### Phase 6: Launch Subscription And Entitlement Model

Repo ownership:

- `invariance_research`: primary owner
- `bulletproof_bt`: no major work unless diagnostics require plan-aware output labels

Purpose:

Make pricing match value without corrupting evidence truth. Evidence limitations and subscription limitations must remain separate.

Implementation tasks:

- update account plan ids or add mapping layer for launch packaging:
  - Free
  - Individual
  - Pro
  - Team
  - Research Desk
- align pricing page, upgrade page, billing page, Stripe checkout, webhook mapping, account state, and diagnostic locks
- update entitlement matrix:
  - uploads per month
  - file size
  - bundle support
  - evidence-gated diagnostic access
  - export access
  - share links
  - retention
  - Research Desk request eligibility
  - seats
  - priority processing
- preserve visible locked pages
- add quota and plan-copy tests
- add Stripe test-mode path for each paid tier
- add admin override for first-client pilots

Launch-tier target:

| Tier | Target Buyer | Price | Core Rights |
| --- | --- | --- | --- |
| Free | Curious trader or first-time evaluator | $0 | limited trade CSV runs, Overview/Distribution/Monte Carlo/Ruin preview, no export, short retention |
| Individual | Serious self-directed trader | $39/month | trade CSV and basic bundles, full report export, core diagnostics, one custom prop evaluation profile per analysis, limited shares |
| Pro | strategy seller, educator, researcher | $99/month | strongest automated suite when artifacts support it, evidence-gated parameter/regime access, saved prop evaluation profiles, report appendix, share links, longer retention |
| Team | prop desk, small fund, research group | $399/month | seats, team library, shared prop firm profiles, profile comparisons, shared reports, admin controls, higher limits, priority processing |
| Research Desk | deep validation buyer | project-based, from $1,000 | human/agent-assisted validation memo, addenda, execution/data QA, benchmark construction, prop-rule interpretation, claim formalization |

Definition of done:

- every locked diagnostic explains whether the lock is evidence-based, plan-based, or both
- checkout works for all paid tiers in test mode
- first-client accounts can be provisioned without manual database edits
- pricing matches the product promises in this doc

Pricing decision after the launch-pricing pass: use opening wedge pricing, not mature-category pricing. The product should buy adoption, uploads, shared reports, and Research Desk demand evidence before it prices like a finished institutional terminal. Keep the long-term premium ambition, but launch with Individual at $39/month, Pro at $99/month, Team at $399/month, and Research Desk from $1,000 so the market can experience the category before being asked to pay mature due-diligence prices.

### Phase 7: Research Desk Handoff And First-Client Ops

Repo ownership:

- `invariance_research`: request workflow, admin operations, addenda, client state
- `bulletproof_bt`: Research Desk packet payload and diagnostic artifacts where needed

Purpose:

Create the upgrade path from automated validation to deeper review. Research Desk should not be generic consulting. It should be a structured continuation of the evidence gaps the product already found.

Implementation tasks:

- add Research Desk packet generator:
  - report snapshot
  - artifact manifest
  - evidence ledger
  - assumption ledger
  - unsupported claims
  - diagnostic outputs
  - requested questions
  - client notes
- build request wizard from Report and Assumption Ledger pages
- add request scopes:
  - execution audit
  - data quality audit
  - benchmark construction
  - parameter stability review
  - regime/context review
  - claim validation
  - investor/buyer memo review
- add admin workflow:
  - received
  - scoped
  - quoted
  - in_review
  - addendum_draft
  - approved
  - delivered
  - closed
- add reviewer addendum approval policy
- add client-facing timeline
- add email notifications
- add manual first-client playbook

Definition of done:

- a user can request Research Desk from a specific report limitation
- admin can see the full packet and move the request through states
- approved addendum attaches to a report snapshot
- the product can sell a high-touch review without pretending the automated run proved more than it did

### Phase 8: Reliability, Security, And Deployment Hardening

Repo ownership:

- `invariance_research`: primary owner
- `bulletproof_bt`: engine runtime reliability, deterministic outputs, packaging
- Both repos: smoke fixtures and release gates

Purpose:

Make the system safe enough for a first external client using real strategy evidence.

Implementation tasks in `invariance_research`:

- formalize migration path instead of relying only on startup schema initialization
- document local SQLite setup and production Postgres setup
- add worker startup script and health checks
- verify object storage for artifacts, exports, and snapshots
- verify email flows:
  - signup
  - verification if enabled
  - password reset
  - Research Desk request
  - share notification if added
- add retention policy and cleanup jobs
- add audit logs for:
  - uploads
  - report exports
  - share creation/revoke/access
  - Research Desk packet access
  - admin addenda
- add rate limits for upload, export, share access, and Research Desk request
- add share-token brute-force protection
- add production smoke route or admin health panel for:
  - database
  - queue
  - worker heartbeat
  - engine bridge
  - storage
  - email
  - Stripe

Implementation tasks in `bulletproof_bt`:

- make engine package/runtime path explicit for the web app
- add deterministic seed tests for all major diagnostics
- add runtime failure modes with structured error envelopes
- add performance benchmarks for large trade files and full bundles
- ensure warnings and limitations are emitted rather than swallowed

Definition of done:

- local SQLite test path works
- production Postgres path works
- analysis worker and export worker can be launched with documented commands
- first-client smoke test can run from upload through share link
- failure states are visible, retryable, or intentionally terminal

## Approach A Launch Wedge Refinement: Trade-History Strategy Robustness Lab

Office Hours conclusion: the first wedge should become narrower, sharper, and more painful. The broad promise remains Strategy Truth Room, but the launch offer should overdeliver on the simplest artifact users already have: trade history. The first monopoly attempt is not "validate every strategy artifact." It is:

> Upload a trade CSV or exchange/broker export. Get a brutally clear strategy survivability verdict, with prop-firm feasibility as a high-value supported workflow when exact rules are supplied.

This is the smallest market with urgent, repeated demand:

- traders trying to pass funded-account evaluations
- traders deciding whether a strategy is challenge-ready
- prop-firm educators/sellers who need proof that their claims survive realistic rules
- strategy sellers who want a shareable validation memo for a simple trade-history artifact

### Launch Positioning

Primary line:

> Find out what your trade history can actually prove before you deploy, sell, or fund it.

Supporting line:

> Upload trades. Declare the claim, rules, and assumptions that matter. Invariance reconstructs the evidence path, exposes the first failure mode, and produces a shareable validation memo with limitations.

Prop evaluation remains a launch wedge inside the workbench, not the whole product identity. It should be visible anywhere it creates strong user value, but the Lab's language should preserve the full workbench: Overview, Execution, Distribution, Monte Carlo, Ruin, Prop Evaluation, Assumption Ledger, Report, and Research Desk escalation.

Avoid broad first-launch positioning such as:

- full strategy operating system
- universal quant research terminal
- fully automated institutional due diligence for every artifact type
- regime/parameter claims from incomplete uploads

Those remain full-ambition directions, not the opening wedge.

### Narrowest Launch Artifact Contract

Launch self-serve should strongly prefer:

1. **Trade CSV**
   Required columns: open/close timestamp where available, realized PnL, symbol where available, side where available, size/quantity where available, fees if available.

2. **Exchange or broker export**
   Parsed into the same normalized trade ledger. If format is unknown, intake should say so and route to Research Desk rather than pretending support.

3. **Optional equity curve**
   Improves drawdown timing and path reconstruction when trade-level balance is incomplete.

4. **Optional prop challenge rules**
   Account size, profit target, max daily loss, max total drawdown, minimum/maximum days, trailing/static drawdown type, payout/evaluation window.

Do not market self-serve launch as requiring OHLCV, parameter sweeps, strategy configs, multi-asset regime definitions, or broker microstructure packets. Those artifacts may enrich context, but they should not be required for the first wedge and should not create overclaiming.

### What The Product Must Become 100x Better At

The proprietary wedge is not a secret indicator. It is an evidence-grade evaluation reconstruction and falsification layer that does the boring hard thing better than spreadsheet calculators, prop-firm dashboards, and generic backtest reports.

The launch product should be materially better at:

- reconstructing the prop evaluation path from imperfect trade evidence
- identifying the exact first breach rule, date, trade index, account state, and loss path
- distinguishing daily loss breach from total drawdown breach
- showing whether target was reached before breach across rolling candidate windows
- showing how many windows pass, fail, or remain unresolved
- explaining target progress without mixing dollars, percent, and rule thresholds incorrectly
- showing fee/slippage perturbation impact on challenge viability
- showing edge concentration: how much of the result comes from the largest few trades
- showing Monte Carlo survival under shuffled trade order and stressed costs
- producing a report that a trader can send to a coach, buyer, partner, or allocator

This is the "quantum improvement" target: not more charts, but a clinical, auditable answer to "Would this trade history have survived the rules I actually face?"

### Launch Scope To Keep

Keep these self-serve workspaces for Approach A launch:

- Overview: verdict, evidence sufficiency, credibility score, claim truth state
- Prop Evaluation: challenge readiness, first breach, target progress, rolling windows, rule table
- Execution: costs, slippage, fee sensitivity, net-edge erosion
- Distribution: payoff profile, edge concentration, rare-trade dependency
- Monte Carlo: survival envelope, sequence risk, drawdown breach probability, target-before-breach probability
- Ruin: capital survivability and loss-streak interpretation
- Assumption Ledger: visible constraints and rescue evidence
- Validation Report: share/export-ready memo
- Research Desk Request: escalation for unavailable/ambiguous evidence
- Library: historical runs and report retrieval

### Launch Scope To Defer Or Route To Research Desk

Do not sell these as automated self-serve launch diagnostics:

- true parameter stability
- multi-asset regime attribution
- broker-level microstructure simulation
- strategy reconstruction from only a config/report
- portfolio-level exposure attribution across symbols
- independent validation memo without reviewer involvement

Always offer Research Desk when users need those outcomes.

### First Launch Pricing Fit

The simpler wedge supports the current lower pricing:

- Free: limited analyses/month, no export, enough preview to show value
- Individual: `$39/mo`, exports, core diagnostics, prop rule evaluation, limited shares
- Pro: `$99/mo`, richer upload capacity, saved profiles, more shares, Research Desk request eligibility
- Research Desk: from `$1,000`, human/agent-assisted validation memo and reviewer addenda

Do not reintroduce Team at launch. Team belongs after the product proves repeated sharing, buyer review, educator workflows, or desk-level use.

### Product Kill Criteria

Approach A is not launch-ready if a user can upload a trade CSV and still see:

- a prop target marked pass while final profit is negative
- target-before-breach windows that actually breached first
- benchmark disabled when the user selected a valid platform benchmark and production data exists
- Research Desk request component without an obvious packet creation button
- export/report copy that hides limitations or overstates unavailable diagnostics

These are not polish issues. They break trust in the product's core claim.

### 101% Launch Wedge Audit Bar

Current product surfaces must be judged against the narrower trade-history-first Strategy Robustness Lab promise, not the full Research OS ambition. The launch pass is acceptable only when the public copy, app intake, upload docs, pricing, and reports all say the same thing:

- self-serve launch is trade-history-first: trade CSVs and exchange/broker exports normalized into a closed trade ledger
- exact prop challenge rules are the decision-grade path; fallback rules are only preview
- prop evaluation is a major workbench capability, but it must not dominate language for Overview, Execution, Distribution, Monte Carlo, Ruin, Assumptions, Report, or Research Desk
- optional context ZIPs improve provenance, assumptions, report quality, and Research Desk packet quality
- optional context ZIPs must not be marketed as automated proof of true parameter stability, multi-asset regime attribution, broker microstructure realism, strategy reconstruction, portfolio exposure, or independent review
- Free exposes enough preview to prove value but not exports or share links
- Individual is the paid self-serve trader plan: exports, core diagnostics, exact rules per run, limited shares
- Pro is the higher-volume wedge plan: saved prop profiles, more shares, richer appendices, and Research Desk request eligibility
- Team remains deferred until repeated sharing and desk-level workflows are proven
- Research Desk is the honest escalation path whenever the upload evidence cannot support the requested conclusion

The first-client bar is higher than "the pages render." A user should be able to upload a simple trade file, enter claims and relevant rules, and receive a clinical answer to:

- Which rule failed first, on what date, at which trade index, with what account/equity state?
- Did the target happen before the daily or total drawdown breach in any rolling evaluation windows?
- How many windows passed, failed, or remained unresolved?
- How much does fee/slippage stress change the validation verdict?
- How much of the edge depends on the largest few trades?
- What does Monte Carlo say about sequence risk and target-before-breach survival?
- What evidence is missing before making stronger regime, parameter, broker, or portfolio claims?

The app should not use broad Strategy Truth Room language to hide a narrower launch product, but it also should not let prop evaluation swallow the entire workbench identity. The sharper wedge is trade-history-first validation with prop feasibility as an obvious high-value path.

### Go-To-War Plan

1. Own trade-history truth first.
   The first pages, examples, docs, and report screenshots should show concrete trade-history validation, with prop-firm feasibility as one of the clearest examples of the Lab's practical value.

2. Publish examples users recognize.
   Use simple trade CSVs, MT5/TradingView/exchange exports, and challenge rule presets. Show exactly what first breach and rolling-window feasibility look like.

3. Make the report the viral artifact.
   A trader should be able to share a report that says: "This strategy fails the 10% drawdown rule before it reaches the 8% target in X of Y windows."

4. Keep the honesty contract visible.
   If the upload cannot support regime, parameter, broker, or portfolio claims, say so and route to Research Desk.

5. Learn from every rejected or escalated upload.
   Missing fields, unsupported broker formats, and Research Desk requests become the roadmap for the next parser and diagnostic upgrade.

## Production Deployment Plan For First 100 Users

This section defines the practical launch architecture for the first real users. The goal is not enterprise scale yet. The goal is a boring, defensible production setup that protects user evidence, runs analyses reliably, charges correctly, and avoids amateur contradictions such as local-only paths in production, missing database tables, unsigned webhooks, broken exports, or diagnostics that depend on unavailable workers.

Target deployment stack:

- **Web app:** `invariance_research` on Vercel.
- **Database:** Supabase managed Postgres.
- **Object storage:** Cloudflare R2 through the app's S3-compatible object-storage adapter.
- **Analysis/export workers:** locally hosted containers controlled by the operator, connected to the same Supabase Postgres and R2 bucket.
- **Engine:** `bulletproof_bt` packaged inside the worker container and called through the versioned Python bridge.
- **Billing:** Stripe Checkout, Customer Portal, signed webhooks, and internal entitlement snapshots.
- **Email:** verified transactional provider for auth, billing events where needed, Research Desk requests, and operational notifications.

### Deployment Principles

1. **The web app is stateless.**
   Vercel must not rely on local filesystem persistence, workstation paths, long-running workers, or embedded engine state. It owns request handling, auth, upload inspection, analysis creation, report/share views, billing routes, and admin surfaces.

2. **Workers are the only long-running compute surface.**
   Analysis and export jobs should be processed by explicitly launched containers, not by Vercel request lifetimes. Embedded workers may remain a local development convenience, but production should use external workers with heartbeat records.

3. **Supabase Postgres is production source of truth.**
   SQLite remains local development and test-only. Production must not call SQLite repositories. Every route reachable in production must go through provider-aware repositories or the shared core repository contract.

4. **R2 is source of truth for uploaded and generated files.**
   Uploads, report exports, report snapshots where materialized, benchmark manifests, benchmark datasets, and derived artifacts must not depend on local storage. Worker containers may use local scratch space only as cache.

5. **Evidence privacy beats convenience.**
   Public shares default to no raw trade files, no account ids, no owner ids, no private storage keys, no raw engine payloads, and no PII. Report exports and Research Desk packets can contain more detail only for authenticated owner/admin flows.

6. **Billing never grants trust by implication.**
   Stripe plan rights control access and limits, but evidence sufficiency controls diagnostic truth. A Pro user with a weak artifact still receives limited diagnostics. A Free user with a strong artifact may see evidence support but not paid export/share rights.

7. **Every operational failure needs a visible state.**
   Missing worker, failed export, failed engine import, invalid benchmark library, Stripe webhook failure, R2 write failure, email failure, and stale schema state must appear in admin health or job state. Silent failure is unacceptable.

### Production Architecture

```text
Browser
  |
  v
Vercel / Next.js
  |-- auth/session routes
  |-- upload inspection and artifact records
  |-- analysis creation and queue records
  |-- report/share/export APIs
  |-- billing and webhook routes
  |-- admin ops and health
  |
  +--> Supabase Postgres
  |      |-- users/accounts/subscriptions/entitlements
  |      |-- artifacts/analyses/jobs/exports/shares
  |      |-- evidence events/rate limits/audit logs
  |
  +--> Cloudflare R2
         |-- uploads/{account}/{artifact}
         |-- reports/{account}/{analysis}/{export}
         |-- benchmarks/manifest.v1.yaml
         |-- benchmarks/{benchmark_id}/daily.parquet

Local Worker Host
  |
  +--> analysis-worker container
  |      |-- polls Supabase analysis_jobs
  |      |-- downloads artifacts from R2
  |      |-- calls Python bridge
  |      |-- runs bulletproof_bt diagnostics
  |      |-- writes analysis result to Supabase
  |      |-- emits evidence events and heartbeat
  |
  +--> export-worker container
         |-- polls Supabase export_jobs
         |-- renders PDF/Markdown/JSON from report snapshot
         |-- writes export artifact to R2
         |-- updates job/export state and heartbeat
```

### Environment Separation

Production, staging, and local development must be distinct.

| Environment | Web | DB | Storage | Workers | Purpose |
| --- | --- | --- | --- | --- | --- |
| Local dev | local Next.js | SQLite by default | local object storage | embedded or local worker | fast iteration |
| Local prod-like | local Next.js | Supabase staging | R2 staging bucket | local containers | pre-launch smoke test |
| Staging | Vercel preview/staging | Supabase staging | R2 staging bucket | local/staging containers | release candidate validation |
| Production | Vercel production | Supabase production | R2 production bucket | production worker containers | first 100 users |

Hard rules:

- production Vercel must set `DATABASE_PROVIDER=postgres`
- local SQLite must never point at production object storage
- staging and production must use separate Supabase projects or at minimum separate databases/schemas
- staging and production must use separate R2 buckets or prefixes
- Stripe test mode must never write production entitlements
- Stripe live mode must never run against staging callbacks
- `APP_URL`, cookie domain, webhook endpoints, and OAuth/email callback URLs must be environment-specific

### Vercel Web App Checklist

Required configuration:

- `DATABASE_PROVIDER=postgres`
- Supabase connection string for server-side Postgres access
- R2/S3-compatible object storage credentials
- Stripe secret key and webhook secret
- email provider credentials
- `APP_URL` matching the deployed production origin
- `ADMIN_EMAILS` for controlled admin bootstrap
- rate limits enabled
- embedded workers disabled for production unless explicitly running a tiny temporary single-node pilot

Vercel responsibilities:

- serve public pages, authenticated app pages, report/share pages, and admin pages
- handle signup, login, verification, password reset, and sessions
- accept uploads through intake API, validate size/type, store artifacts in R2, and create artifact records
- create analysis/export jobs in Supabase
- expose health/admin state without leaking secrets
- process Stripe webhooks idempotently
- render share views from immutable snapshots

Vercel must not:

- run long analysis jobs inside request handlers
- depend on `/home/...`, `/tmp` as durable storage, or local benchmark files
- import `bulletproof_bt` directly into the web runtime
- expose raw engine payloads or raw private artifacts through share routes
- create entitlements directly from client-submitted plan ids

### Supabase Postgres Plan

Production Postgres must be treated as a managed operational dependency, not a convenience database.

Required database practices:

- use a migration discipline before launch; startup schema initialization can remain a safety net but not the only deployment mechanism
- run migrations against staging first, then production
- every production table needed by app routes must exist before traffic:
  - users, accounts, sessions/auth tokens
  - user roles and admin roles
  - subscriptions and entitlement snapshots
  - usage snapshots
  - artifacts and analyses
  - analysis jobs and export jobs
  - exports and report snapshots
  - share tokens and share access events
  - evidence events and audit logs
  - Research Desk requests/addenda/learning events
  - rate limit buckets
  - webhook events
  - worker heartbeats
- add indexes for owner/account scoped reads, analysis lookup, job polling, share token lookup, webhook idempotency, and rate-limit buckets
- use SSL-required connections
- use connection pooling appropriate for Vercel serverless; avoid connection storms
- set statement timeout for web requests where possible
- workers can use longer timeouts than web routes, but job leases must prevent duplicate processing
- enable automated backups and point-in-time recovery
- define restore drill before first paid user

Supabase security stance:

- do not expose service-role credentials to browser code
- use server-only environment variables for Postgres
- if Supabase client/RLS is introduced later, keep raw artifact/report access server mediated
- audit any direct SQL route for tenant scoping by `account_id`
- admin routes require both authenticated session and DB/admin allowlist role

### Cloudflare R2 Storage Plan

R2 is the production object store for private evidence and generated deliverables.

Required bucket layout:

```text
uploads/{account_id}/{artifact_id}/{safe_file_name}
reports/{account_id}/{analysis_id}/{export_id}/{safe_file_name}
report-snapshots/{account_id}/{analysis_id}/{snapshot_id}.json   # optional materialized copy
benchmarks/manifest.v1.yaml
benchmarks/BTC/daily.parquet
benchmarks/SPY/daily.parquet
benchmarks/DXY/daily.parquet
benchmarks/XAUUSD/daily.parquet
```

Required storage controls:

- bucket is private
- public bucket listing disabled
- no raw upload object is served directly to unauthenticated users
- all downloads are mediated by authenticated API routes or short-lived signed URLs with account ownership checks
- object keys include account and artifact/export identifiers
- object metadata stores content type, size, checksum, and created timestamp where possible
- app DB records store storage key, checksum, byte size, and content type
- deletion policy defines tombstone versus hard delete behavior
- export retention is enforced by cleanup jobs
- R2 access keys are scoped to the exact bucket where possible
- rotate R2 credentials before public launch and after any exposure suspicion

Benchmark library controls:

- benchmark manifest cache does not equal dataset availability
- production benchmark datasets must exist in R2, not on the developer machine
- weekly benchmark update job writes manifest and dataset files to R2
- benchmark health checks must validate both manifest and dataset presence
- if benchmark data is missing, the app must disable benchmark comparison and show a clear limitation rather than fail the analysis

### Local Worker Container Plan

Workers can be locally hosted for launch as long as they are operationally disciplined.

Required containers:

- `analysis-worker`
- `export-worker`

Both containers must:

- use the same `DATABASE_PROVIDER=postgres` and Supabase production connection as the web app
- use the same R2 production credentials as the web app
- set `WORKER_MODE=external`
- set explicit worker ids
- emit heartbeat records
- use job leases and retries
- fail jobs with structured errors
- enforce maximum job runtime
- expose logs to a durable local logging target
- restart automatically through systemd, Docker Compose restart policy, or another supervisor
- run under least-privilege host user
- never mount broad host directories containing unrelated secrets

Analysis worker must:

- include the exact `bulletproof_bt` package version expected by the app contract
- run the Python bridge probe successfully before accepting jobs
- have deterministic seeds for stochastic diagnostics where applicable
- enforce max upload/job size from plan and system config
- write diagnostic limitations instead of crashing on missing optional context
- report engine import failure, timeout, malformed artifact, and unsupported artifact as different states

Export worker must:

- render PDF/Markdown/JSON from report snapshots, not mutable live analysis objects
- write exports to R2
- update export job progress
- fail closed if snapshot is missing or account ownership cannot be proven
- ensure exported PDF includes limitations, evidence coverage, and report identity

Operational minimum for local workers:

- one machine with stable network and power
- daily restart policy tested
- disk space alert for logs/cache
- worker heartbeat visible in admin health
- manual runbook for restarting workers
- manual runbook for replaying failed jobs safely
- no analysis job is considered "lost"; queued, processing, failed, dead-letter, and completed are all visible

### Stripe Billing Hardening

Stripe is a trust boundary. Client-side plan selection is only a request; server-side Stripe events and internal entitlement mapping are the source of truth.

Required launch setup:

- create products/prices for:
  - Individual: `$39/mo`
  - Pro: `$99/mo`
  - Team: `$399/mo` only if Team remains visible at launch; otherwise keep product disabled/deferred
  - Research Desk: project-based quote path, not self-serve subscription unless later needed
- map Stripe price ids to internal canonical plan ids on the server
- never accept arbitrary plan ids or price ids from the browser without server allowlist validation
- use Stripe Checkout for subscription creation
- use Stripe Customer Portal for upgrades, cancellation, and payment-method management
- process webhooks with signature verification using `STRIPE_WEBHOOK_SECRET`
- persist Stripe event ids and process webhooks idempotently
- handle at minimum:
  - `checkout.session.completed`
  - `customer.subscription.created`
  - `customer.subscription.updated`
  - `customer.subscription.deleted`
  - `invoice.payment_succeeded`
  - `invoice.payment_failed`
  - `customer.updated` where needed
- define grace behavior for failed payment:
  - keep account active during Stripe grace period if subscription remains active/past_due
  - restrict export/share creation when subscription is canceled/unpaid beyond grace
  - never delete user evidence because of payment failure without retention notice
- admin override must be explicit, audited, and visible on account admin page
- all admin/test emails can be granted highest-tier rights for testing, but that bypass must be server-side and audited

Stripe launch tests:

- test checkout for Individual and Pro
- test portal cancellation
- test plan upgrade/downgrade
- test failed invoice event
- test duplicate webhook event
- test webhook event arriving before user refreshes account page
- test account entitlement snapshot after each event
- test that paid plan unlocks billing rights but does not override evidence-limited diagnostics

### Authentication, Email, And Account Safety

Required:

- verified production email sender domain
- signup verification path works or verification requirement is disabled intentionally for beta
- password reset works in production
- auth cookies use secure settings in production
- session secret is strong and rotated if exposed
- login/register/forgot-password endpoints are rate-limited
- user-facing auth errors are clear but do not leak account existence unnecessarily
- admin accounts are role-gated, not just path-gated
- admin bootstrap allowlist is removed or kept minimal after first launch

Email deliverability:

- SPF, DKIM, and DMARC configured
- email provider dashboard monitored during first launch
- verification and reset emails tested on Gmail and at least one non-Gmail provider
- Research Desk request emails do not include raw artifact contents

### Security And Privacy Baseline

Threats to control before first users:

| Threat | Control |
| --- | --- |
| Oversized upload / memory pressure | content-length guard, per-plan max file size, parser limits, route rate limits |
| ZIP abuse | JS-based extraction, file count/size limits, reject path traversal, ignore unsupported files visibly |
| Cross-tenant data leak | account ownership checks on artifacts, analyses, exports, shares, Research Desk packets |
| Public share leak | redacted report projection, no raw files, no owner ids, no private storage keys, revoked/expired fail closed |
| Stripe spoofing | webhook signature verification, price allowlist, idempotent webhook event storage |
| Admin abuse | role checks, audit logs, least privilege, admin event visibility |
| Worker compromise | separate worker secrets, no public inbound access required, restartable containers, scoped R2 keys |
| DB credential exposure | server-only env vars, rotation plan, no credentials in client bundle or logs |
| Object-store credential exposure | scoped R2 keys, no key logging, rotation plan |
| Engine crash or malformed output | structured error envelopes, job failure states, retry/dead-letter behavior |
| Benchmark data absence | object-storage materialization, health checks, graceful benchmark-disabled state |
| Report overclaiming | diagnostic honesty model, limitation ledger, evidence/plan lock separation |

Security launch gates:

- no raw secrets in git history or logs
- dependency audit reviewed for critical issues
- all public share routes manually tested for redaction
- all admin routes manually tested as non-admin
- all API routes that mutate state require session or signed webhook verification
- rate limits verified in staging
- object keys are not guessable download URLs
- CORS is restricted to intended origins where applicable
- CSP and security headers are reviewed before launch

### Observability And Admin Operations

Admin health must show:

- database provider and connectivity
- migration/schema readiness
- R2 connectivity and write/read probe
- benchmark manifest and dataset health
- analysis queue depth and oldest queued job age
- export queue depth and oldest queued job age
- analysis worker heartbeat and stale threshold
- export worker heartbeat and stale threshold
- engine bridge probe state
- Stripe config and webhook status
- email provider config and last send status
- rate-limit event volume
- failed jobs, dead-letter jobs, and retry counts

Logs should include:

- request id where available
- account id and analysis id for authenticated operations
- no raw uploaded content
- no full share token
- no Stripe secret values
- no R2 secret values
- no private report payloads except in controlled debug fixtures

Minimum alerts for launch:

- worker heartbeat stale
- analysis queue oldest job above threshold
- export queue oldest job above threshold
- repeated engine failures
- R2 write/read failure
- Supabase connection failure
- Stripe webhook verification failure
- email send failure
- high 500 rate on Vercel
- repeated rate-limit abuse

### Release And Migration Procedure

Every production release should follow this order:

1. Merge code only after typecheck, build, and focused contract tests pass.
2. Apply database migrations to staging.
3. Deploy Vercel preview/staging.
4. Start staging workers against staging Supabase and staging R2.
5. Run smoke tests:
   - signup/login
   - upload trade CSV
   - upload reference ZIP bundle
   - create analysis
   - worker processes analysis
   - open Overview, Monte Carlo, Prop Evaluation, Report
   - export PDF
   - create/revoke share link
   - submit Research Desk request
   - verify admin ops pages
6. Apply migrations to production.
7. Deploy Vercel production.
8. Start or restart production workers.
9. Run production smoke test with an internal admin account.
10. Check admin health for green/degraded states.
11. Only then invite external users.

Rollback rule:

- If a release breaks auth, upload, report, share, billing, or tenant isolation, rollback the Vercel deployment immediately.
- If a migration is irreversible, do not deploy it without backup verification and a forward-fix plan.
- If workers fail but web app remains stable, pause analysis creation or show maintenance state rather than accepting invisible jobs.

### Data Retention, Deletion, And Backups

Launch retention policy should be explicit in product copy and internal runbooks.

Minimum policy:

- uploaded artifacts retained according to plan unless user deletes account or requests deletion
- report exports expire after configured retention period
- share access logs retained for security/audit window
- Research Desk packets retained as long as needed for service delivery and audit
- deleted user/account records should remove or tombstone private artifacts according to legal/privacy policy
- benchmark datasets are product data, not user data

Backup plan:

- Supabase automated backups enabled before first user
- R2 bucket versioning or lifecycle policy considered for critical generated artifacts
- monthly restore drill once beta begins
- manual export of schema and environment inventory before launch
- backup access limited to operator/admin

### First 100 Users Capacity Assumptions

The first 100 users do not require massive infrastructure, but they do require predictable behavior.

Baseline assumptions:

- 100 registered users
- 20 to 40 monthly active users initially
- 2 to 3 analyses/month on Free users
- 25 analyses/month on Individual users
- 100 analyses/month on Pro users
- most files under 10MB; paid users may submit 25MB to 50MB bundles
- analysis jobs are CPU-bound and should queue gracefully
- report/export jobs are lower CPU but must not starve analysis jobs

Capacity controls:

- enforce per-plan monthly analysis quotas
- enforce per-plan upload size
- enforce route rate limits
- set analysis worker concurrency conservatively at first
- separate analysis worker and export worker containers
- use priority processing by plan only after correctness is proven
- expose queue wait state to users
- route oversized, unusual, or ambiguous bundles to Research Desk rather than pretending self-serve can handle everything

Suggested launch worker setup:

- one production `analysis-worker` container with concurrency `1`
- one production `export-worker` container with concurrency `1`
- external supervisor with auto-restart
- manual scale path: add another analysis worker only after duplicate-claim/job-lock behavior is verified

### First-User Launch Checklist

No external user should be invited until all items below are true.

Product:

- homepage, pricing, lab docs, signup, login, new analysis, analysis library, report, export, share, billing, and Research Desk pages render in production
- public copy does not expose internal design notes
- upload docs match accepted formats and current self-serve limits
- reference bundle uploads successfully in production
- eligibility summary updates when runtime prop rules are entered
- diagnostics clearly distinguish available, evidence-limited, plan-locked, and Research Desk scope
- report export is polished enough to be shared
- share view is recipient-safe

Infrastructure:

- Vercel production build passes
- Supabase production schema is migrated and verified
- R2 production bucket has write/read probe passing
- benchmark manifest and datasets exist in R2
- weekly benchmark update workflow is configured
- workers are running externally and heartbeats are fresh
- engine bridge probe is healthy in worker environment
- production admin health page is usable

Security:

- rate limits enabled
- upload size guard enabled
- Stripe webhooks signed and idempotent
- admin access tested as admin and non-admin
- share revoke/expiry tested
- object storage private by default
- no raw uploaded artifacts public by default
- secrets rotated from any local/testing exposure

Billing:

- Stripe live products/prices configured
- test-mode end-to-end billing already passed
- live-mode checkout smoke tested with internal account
- Customer Portal works
- entitlements update from webhook, not just return URL
- failed-payment behavior defined
- admin override visible and audited

Operations:

- runbook exists for worker restart
- runbook exists for failed analysis/export job
- runbook exists for production rollback
- runbook exists for user deletion/export request
- support email/contact path works
- first-client onboarding script exists

### Current Infrastructure Gap Analysis Against Phase 8

This audit reflects the current `invariance_research` infrastructure shape: Vercel web app already deployed once, Supabase Postgres connected, Cloudflare R2 connected, local worker Compose files present under `invariance_research/deploy`, and Stripe integration started but not hardened.

#### What Already Exists

| Area | Current state | Production interpretation |
| --- | --- | --- |
| Web hosting | Vercel app can build/deploy and connect to Supabase/R2. | Good launch target, but must remain stateless and must not run embedded workers in production. |
| Persistence | SQLite for local dev and Postgres provider for production both exist. | Correct architecture, but all production health/report paths must be provider-aware; any SQLite call under `DATABASE_PROVIDER=postgres` is a launch blocker. |
| Queueing | DB-backed analysis/export queues, retry metadata, worker heartbeats, admin job/export views. | Good enough for first 100 users if worker concurrency stays conservative and stale-job recovery is monitored. |
| Workers | `analysis-worker` and `export-worker` entrypoints and Docker Compose deployment exist. | Viable first-user shape after runbook, env example, heartbeat, health, restart, and logging discipline are enforced. |
| Engine bridge | Worker image installs `bulletproof_bt` and uses a Python bridge instead of importing the engine into the web runtime. | Correct boundary. Engine changes must remain contract-versioned and tested against the web app payload schema. |
| Object storage | R2 adapter exists and is used for generated artifacts/exports/benchmark paths. | Correct, but production must prove all benchmark datasets and generated exports live in R2, not local cache. |
| Admin ops | Admin health/jobs/exports/research-desk surfaces exist. | Good operator base, but health must include provider-aware DB/queue checks, email config, benchmark object-storage health, and worker heartbeat freshness. |
| Billing | Stripe Checkout, Portal, signed webhook route, event idempotency, and entitlement updates exist. | Started, not finished. Subscription metadata, live price allowlists, failed-payment behavior, and Stripe Dashboard hardening are required before paid launch. |
| Rate/upload controls | Upload size and rate-limit work has been introduced. | Must be tested in production and documented per plan; rejected uploads should explain the limit without leaking internals. |

#### Must Fix Before The First Paid User

| Gap | Why it matters | Required action | Repo / system |
| --- | --- | --- | --- |
| Provider-aware startup/health | Production previously hit SQLite-only code while `DATABASE_PROVIDER=postgres`, causing report/admin failures. | Health, queue, and startup checks must query Postgres through provider contracts in production and SQLite only in local mode. | `invariance_research` |
| Worker env hygiene | A local `.env.worker` exists and real secrets must never become deployment documentation or screenshots. | Keep real env untracked, add `.env.worker.example`, rotate any exposed key, and document secret handling. | `invariance_research`, Supabase, R2 |
| Worker Compose portability | Hard-coded local build paths and mandatory Ollama dependency make production fragile. | Use configurable build root, remove hard dependency on Ollama for core analysis, add restart/log/heartbeat guidance. | `invariance_research/deploy` |
| Stripe subscription metadata | `customer.subscription.*` webhooks need `account_id` and `plan_id`; Checkout session metadata alone is not enough for reliable subscription events. | Add `subscription_data.metadata`, `client_reference_id`, and test checkout/update/cancel webhook sequence. | `invariance_research`, Stripe |
| Stripe live price allowlist | Fallback placeholder price ids are acceptable in local dev but dangerous in production. | Production startup should fail or admin health should be unhealthy when live price env vars are missing. | `invariance_research`, Vercel |
| Stripe Dashboard configuration | Products, prices, portal, webhook endpoint, receipts, tax, and live/test separation are not yet production disciplined. | Complete Stripe checklist below before enabling paid CTAs for real users. | Stripe |
| Benchmark object storage proof | Manifest cache is not dataset availability. | Run first production benchmark sync to R2, verify manifest and each dataset key, add weekly update procedure. | Worker host, R2 |
| Email deliverability | Signup verification, reset, Research Desk notifications, and operational emails affect trust. | Verify sender domain, SPF/DKIM/DMARC, provider key, and production callback URLs. | Email provider, Vercel |
| Production smoke path | The system has many moving parts; a single broken report/export path will look amateur. | Run a production internal account smoke test: signup, upload, analysis, report, export, share, billing, admin health. | All |

#### Can Ship For First 100 With Operator Discipline

| Area | Acceptable launch posture | Monitoring requirement |
| --- | --- | --- |
| DB-backed queues instead of Redis | Acceptable at low volume if leases, retries, and heartbeats are visible. | Check queue backlog, stale processing jobs, retry counts daily. |
| Locally hosted workers | Acceptable if machine is stable, containers auto-restart, secrets are scoped, and no public inbound access is needed. | Fresh heartbeat, disk/log usage, engine probe, R2 writes. |
| Lightweight PDF export | Acceptable if the export is polished, includes report identity, charts, limitations, and evidence coverage. | Export failures and sample downloaded PDF review. |
| Manual benchmark update | Acceptable briefly if weekly manual run is documented. | Last manifest timestamp and dataset coverage shown in admin health. |
| Single analysis worker concurrency | Preferred for correctness while proving queue behavior. | Queue wait messaging and oldest queued job threshold. |

#### Defer Until After First 100

| Deferred item | Reason |
| --- | --- |
| Redis/BullMQ or managed queue | DB queue is simpler and sufficient for first-user proof; move when load or reliability justifies it. |
| Full Kubernetes orchestration | Local Docker Compose is enough for launch; Kubernetes adds operational surface before demand proves it. |
| Multi-region storage/database | Not needed until clear geographic or latency requirements emerge. |
| Public multi-seat Team workflow | Team can remain deferred until Research Desk and shared report workflows show repeat demand. |
| Full command-terminal UX | Internal terminal concepts should power contracts, but public wedge remains upload-validation-report. |

### Production Worker Runbook

Worker deployment lives in `invariance_research/deploy`.

Required files:

- `docker-compose.worker.yml`
- `.env.worker.example`
- local untracked `.env.worker`
- `README.worker.md`

Launch commands from the worker host:

```bash
cd /home/omenka/Projects/invariance_research/deploy
cp .env.worker.example .env.worker
# Fill Supabase, R2, email, and optional LLM values.
docker compose -f docker-compose.worker.yml up -d --build analysis-worker export-worker
docker logs -f invariance-analysis-worker
docker logs -f invariance-export-worker
```

Optional LLM synthesis:

```bash
docker compose -f docker-compose.worker.yml --profile llm up -d ollama
docker exec -it invariance-ollama ollama pull qwen2.5:14b
```

First launch default should keep `LLM_INSIGHTS_ENABLED=false` unless LLM output has been tested end-to-end and failure states are product-safe.

Production worker environment requirements:

- `DATABASE_PROVIDER=postgres`
- `DATABASE_URL` points to Supabase production
- `POSTGRES_SCHEMA_AUTO_INIT=false` after migrations are formalized
- `OBJECT_STORAGE_PROVIDER=s3`
- R2 endpoint, bucket, access key, secret key, region, path-style setting
- `BENCHMARK_PROVIDER=object_storage`
- benchmark manifest and prefix keys
- embedded workers disabled in Vercel
- worker concurrency starts at `1`
- worker heartbeat stale threshold configured

Worker health must be visible in Admin Ops before any paid user is allowed to create an analysis.

### Vercel Production Checklist

Set production env vars in Vercel, not in the repo:

- `APP_URL`
- `DATABASE_PROVIDER=postgres`
- `DATABASE_URL`
- `INVARIANCE_EMBEDDED_WORKERS=false`
- `OBJECT_STORAGE_PROVIDER`
- `OBJECT_STORAGE_BUCKET`
- `OBJECT_STORAGE_ENDPOINT`
- `OBJECT_STORAGE_ACCESS_KEY_ID`
- `OBJECT_STORAGE_SECRET_ACCESS_KEY`
- `OBJECT_STORAGE_FORCE_PATH_STYLE=true`
- `BENCHMARK_PROVIDER=object_storage`
- `BENCHMARK_MANIFEST_OBJECT_KEY`
- `BENCHMARK_OBJECT_PREFIX`
- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`
- `STRIPE_PRICE_INDIVIDUAL`
- `STRIPE_PRICE_PRO`
- `EMAIL_PROVIDER`
- `EMAIL_FROM`
- email provider API key
- `ADMIN_EMAILS`
- session/auth secrets
- upload limits and rate-limit settings

Vercel deployment gates:

1. Production build passes.
2. `/api/health` does not call SQLite when `DATABASE_PROVIDER=postgres`.
3. Admin health shows Postgres, R2, queue, benchmark, Stripe config, email config, and worker heartbeat.
4. Uploads write to R2 and analysis jobs appear in Supabase.
5. Workers process jobs from Supabase, not local SQLite.
6. Report page renders without provider errors.
7. Export job completes and writes to R2.
8. Share link shows redacted report projection only.

### Stripe Production Hardening Checklist

Stripe must be configured in both code and dashboard.

Internal app requirements:

- server creates Checkout sessions only from allowlisted plan ids
- Checkout session sets both session metadata and `subscription_data.metadata`
- webhook route verifies Stripe signature with `STRIPE_WEBHOOK_SECRET`
- webhook event ids are persisted for idempotency
- price id maps to canonical internal plan id
- missing live price ids degrade/fail production health
- account entitlements update from webhook, not return URL
- admin subscription overrides are visible and audited
- failed-payment state does not delete evidence and does not silently preserve paid export rights forever

Stripe Dashboard steps:

1. Create live products:
   - `Invariance Individual` at `$39/month`
   - `Invariance Pro` at `$99/month`
   - `Research Desk` as quoted/manual invoice product, not self-serve by default
2. Copy live price ids into Vercel:
   - `STRIPE_PRICE_INDIVIDUAL`
   - `STRIPE_PRICE_PRO`
3. Create a live webhook endpoint:
   - URL: `https://YOUR_DOMAIN/api/webhooks/stripe`
   - Events:
     - `checkout.session.completed`
     - `customer.subscription.created`
     - `customer.subscription.updated`
     - `customer.subscription.deleted`
     - `invoice.payment_succeeded`
     - `invoice.payment_failed`
4. Copy the endpoint signing secret to `STRIPE_WEBHOOK_SECRET`.
5. Configure Customer Portal:
   - allow payment method update
   - allow cancellation
   - allow Individual to Pro upgrade
   - define downgrade/cancel behavior
   - add business terms/support links
6. Configure receipt and invoice settings:
   - business name
   - support email
   - statement descriptor
   - invoice footer
   - tax/VAT behavior if applicable
7. Run test-mode flow first:
   - new checkout
   - duplicate webhook replay
   - upgrade
   - cancellation
   - failed payment
8. Run live-mode smoke test with an internal/admin account before exposing paid CTAs.

Stripe launch rule: if a webhook fails, the user may see a pending billing state, but the system must not grant or revoke paid rights based only on a client redirect.

### First-100 User Operating Rhythm

Daily during first launch:

- check admin health
- check failed jobs
- check stale workers
- check uploads rejected by parser
- check export failures
- check Stripe webhook failures
- check share access anomalies
- review Research Desk requests

Weekly during first launch:

- run or verify benchmark library update
- review top missing-evidence patterns
- review which diagnostics users try to unlock
- review free-to-paid conversion blockers
- review report shares and recipient behavior
- review support tickets for misleading copy or unclear diagnostic language
- review cost/runtime per analysis

Launch success for first 100 users:

- no cross-tenant data incident
- no paid user unable to export because of infrastructure misconfiguration
- no report share exposes private raw artifacts
- no production route calls local SQLite when `DATABASE_PROVIDER=postgres`
- no benchmark comparison silently uses stale local files
- no Stripe webhook mismatch leaves users on the wrong plan
- no analysis job disappears without admin-visible state
- at least one user shares a report externally
- at least one user requests Research Desk from an explicit evidence limitation

### Phase 9: First-Client Beta Protocol

Repo ownership:

- Both repos

Purpose:

Prove the product can handle a real client before calling Approach A sellable.

Beta scope:

- 3 to 5 serious users
- 10 to 20 artifact uploads
- at least 3 full validation bundles
- at least 2 broker/export files
- at least 2 parameter sweeps
- at least 3 prop evaluation rule profiles tested against completed runs
- at least 2 Research Desk request simulations
- at least 2 external share recipients

First-client acceptance checklist:

- user can create account and sign in
- user can upload trade CSV
- user can upload structured bundle
- user can inspect artifact sufficiency before running analysis
- all workbench pages are visible
- unavailable pages explain missing evidence
- plan-locked pages explain subscription lock separately from evidence lock
- Overview gives a clear credibility verdict
- Execution explains what happens when fills/costs worsen
- Distribution explains rare-trade dependence
- Monte Carlo explains survival under path stress
- Ruin explains capital/risk assumptions
- Prop Evaluation Readiness can be recomputed from fallback rules to actual firm rules after the run
- Regime explains context dependence or missing context
- Parameter Stability explains sweep support or missing sweep
- Assumption Ledger shows assumptions and rescue evidence
- Report states what the artifact does not prove
- Export produces a polished PDF
- Share Room renders a recipient-safe snapshot
- Research Desk request can be submitted from a limitation
- admin can see requests, exports, shares, and worker status
- production deployment can run without missing tables
- local test deployment can run without production Postgres

Readiness metrics:

- 95% of valid trade CSV uploads inspect successfully
- 90% of full bundles produce the expected unlock matrix
- 0 hidden failed diagnostics on completed analyses
- 0 share links expose raw private artifacts by default
- 0 report exports omit limitations when diagnostics are limited
- 0 prop readiness reports omit the rule snapshot or fallback-profile warning
- median analysis runtime acceptable for first-client use
- all first-client failures produce a rescue path or admin-visible error

Exit criteria:

- at least one real user exports and shares a report without assistance
- at least one real user changes or abandons a strategy because of the output
- at least one real user asks for the next experiment or Research Desk review from an evidence limitation
- support burden is low enough that the next 10 users can be onboarded without custom database or code intervention

### Recommended Codex Implementation Order

The next implementation slices should follow this order:

1. Phase 0 contract freeze and fixture harness.
2. Phase 1 artifact intake and full bundle manifest.
3. Phase 2 evidence ledger, assumption ledger, and unsupported claims.
4. Phase 3 engine diagnostic hardening, starting with execution realism and distribution concentration.
5. Phase 4 workbench redesign page-by-page, starting with Overview and Execution.
6. Phase 5 proof report, export, and Share Room.
7. Phase 5.5 validation command layer, explainability, evidence alerts, and connected case-file timeline.
8. Phase 5.6 Prop Evaluation Readiness.
9. Phase 6 subscription and entitlement alignment.
10. Phase 7 Research Desk handoff.
11. Phase 8 reliability hardening.
12. Phase 9 first-client beta protocol.

This order matters. The app should not spend another pass polishing workbench pages before the artifact contract, evidence ledger, assumption ledger, and claim inventory are stable enough to power the product truthfully.

## Moving From Approach A To Full Ambition

Approach A should now be treated as import/audit/report infrastructure. The full ambition no longer waits behind upload validation; it starts through a narrow Research Pipeline workflow and reuses Approach A artifacts where they are useful.

### Stage 1: Artifact Validation

User action:

- upload evidence
- get validation
- export report

Product object:

- artifact
- analysis
- report

This is the current Strategy Robustness Lab path.

### Stage 1.5: Validation Command Layer

User action:

- ask why the verdict changed
- jump to missing evidence
- open unsupported claims
- create a report snapshot
- export or share the memo
- request Research Desk review from a specific limitation

Product object:

- command
- saved validation question
- evidence event
- explanation
- case-file timeline

Why this belongs before the full research OS:

It gives serious users Bloomberg-style speed and connected workflow without asking them to adopt a multi-run institutional terminal. This is the bridge between a useful dashboard and a product people trust during a real buying, allocation, education, or strategy-sale decision.

### Stage 2: Strategy Workspace

User action:

- group multiple analyses under a strategy
- compare versions
- see whether a revised version improved evidence quality

Product object:

- strategy
- project
- analysis lineage

Why this is next:

Users who return with multiple uploads need organization before they need a research OS.

### Stage 3: Market Claim Capture

User action:

- state the claim before running validation
- define what would disprove it
- attach assumptions and expected edge

Product object:

- market claim
- invalidation condition
- assumption

Important UX rule:

Do not make claim capture mandatory on first upload. Introduce it as "make this report stronger" after the user sees how unsupported claims weaken the report.

### Stage 4: Hypothesis And Experiment Planning

User action:

- convert claim into a formal hypothesis
- define required data
- define train/test or discovery/validation split
- run parameter or regime tests

Product object:

- hypothesis
- validation plan
- experiment group
- run manifest

Reuse from `bulletproof_bt`:

- `src/bt/hypotheses`
- `src/bt/experiments`
- `orchestrator/run_experiment_pipeline.py`
- `orchestrator/research_daemon.py`

### Stage 5: Tenant-Scoped Research Memory

User action:

- ask "where have I seen this before?"
- retrieve similar failures
- see hostile regimes across prior runs
- compare strategy variants

Product object:

- research finding
- state bucket
- candidate
- recommendation
- reviewer annotation

Reuse from `bulletproof_bt`:

- `orchestrator/research_memory/trade_memory.py`
- `orchestrator/research_memory/state_memory.py`
- `orchestrator/research_memory/candidate_memory.py`
- `orchestrator/research_memory/recommendation_engine.py`
- `orchestrator/research_memory/query_engine.py`

Required product changes:

- move research memory from local SQLite patterns to tenant-scoped Postgres tables
- add as-of timestamps
- add dataset version references
- prevent cross-user leakage
- keep recommendations evidence-only
- never let memory auto-approve deployment

### Stage 6: Full Claim-First Research OS

User action:

- design, validate, fork, reject, and remember strategies through a governed research process

Product object:

- claim graph
- hypothesis lineage
- experiment lineage
- evidence ledger
- research memory
- report library
- reviewer workflow

This is the 10-star product. It should be earned by the wedge, not built before the wedge works.

## Approach B: Research Pipeline First

Approach B supersedes Approach A as the main product path.

Approach B does not mean shipping every full-ambition surface at once. It means the default workflow is no longer upload-first. The default workflow is:

```text
Research Program
  -> Idea Intake
  -> Clarification
  -> Hypothesis Spec
  -> Strategy Spec
  -> Experiment Plan
  -> Queue
  -> Engine Run
  -> Verdict
  -> Memory
  -> Next Experiment
  -> Report Snapshot
```

The Strategy Robustness Lab remains as a mode inside the system:

```text
External Artifact
  -> Import / Audit
  -> Validation Report
  -> Snapshot / Export / Share
  -> Research Desk or Research Program handoff
```

### Approach B Product Objects

| Product object | Purpose | Primary repo owner |
| --- | --- | --- |
| Research Program | User-facing container for a market thesis, hypotheses, runs, reports, and memory. | `invariance_research` |
| Idea Intake | Plain-English entry point for a trading intuition. | `invariance_research` |
| Clarification Session | Structured assistant questions that turn vague intuition into testable constraints. | `invariance_research` |
| Hypothesis Spec | Versioned falsifiable claim: thesis, observables, data needs, invalidation criteria, assumptions. | Shared; schema in `bulletproof_bt`, persistence/UI in `invariance_research` |
| Strategy Spec | Engine-safe executable or declarative strategy definition generated from the hypothesis. | `bulletproof_bt` owns contract and validation; `invariance_research` owns approval UI |
| Experiment Plan | Run matrix: baseline, costs, parameter grid, regime split, holdout, null comparison, alternative exits. | `bulletproof_bt` |
| Experiment Queue | Durable SaaS job queue for research runs. | `invariance_research` owns tenant/job lifecycle; `bulletproof_bt` owns daemon execution semantics |
| Run Manifest | Immutable per-run contract: code/spec version, data version, config, assumptions, hashes, status. | `bulletproof_bt` |
| Verdict Card | Result interpretation: pass/fail/fragile, failure cause, evidence quality, next action. | `bulletproof_bt` emits; `invariance_research` displays |
| Research Memory | Tenant-scoped history of hypotheses, failures, state buckets, promoted candidates, and next experiments. | Shared; engine algorithms in `bulletproof_bt`, tenant product memory in `invariance_research` |
| Report Snapshot | Shareable proof artifact for a run or research program milestone. | `invariance_research` |

### Assistant Roles

Approach B should use assistants as constrained workflow actors, not as free-form magic.

| Assistant | Job | Guardrail |
| --- | --- | --- |
| Research Intake Assistant | Turns raw English into structured clarifying questions. | Must expose missing assumptions instead of inventing them. |
| Hypothesis Assistant | Produces falsifiable hypothesis specs and invalidation criteria. | Must output schema-valid hypotheses only. |
| Strategy Spec Assistant | Converts approved hypotheses into engine-safe strategy specs or code patches. | Must pass static validation, no-lookahead checks, and human approval before execution. |
| Experiment Planner | Proposes baseline and falsification runs. | Must label cost, data, and runtime implications. |
| Result Interpreter | Reads engine artifacts and emits verdict cards. | Must consume existing artifacts only; no unsupported claims. |
| Memory Assistant | Retrieves similar failures and prior related work. | Tenant-scoped only; no cross-user memory by default. |

## Approach B Codex Implementation Phases

Each phase should be implemented as a Codex slice with tests, migration notes, and a verification note. The goal is a solid Approach B product, not a demo. A phase is complete only when the product loop works through the web app and the engine contract is covered by fixtures.

### Phase B0: Reframe Product Contract And Navigation

Goal:

Make the product internally and externally consistent with Research Pipeline First while preserving Approach A as audit/import/report mode.

`invariance_research` owns:

- update public copy from upload-first to research-pipeline-first
- add top-level IA for Research Programs, Experiment Queue, Memory, Reports, and Audit Imports
- demote `/app/new-analysis` from primary entry to "Audit an artifact"
- create empty-state flows for "Start a Research Program" and "Import Existing Evidence"
- update pricing/upgrade copy so paid value is continuous research throughput, not only report exports
- update docs/lab to say upload artifacts are one path, not the whole product

`bulletproof_bt` owns:

- update docs to mark Strategy Robustness Lab as a service surface, not the product center
- document current research daemon, hypothesis, experiment, and research terminal capabilities as product substrate

Exit criteria:

- user-facing app does not imply upload-only is the main product
- design doc, public copy, and app IA all name Research Programs as the core object
- Approach A artifacts remain accessible as Audit/Import mode

### Phase B1: Research Program Data Model

Goal:

Introduce the durable product container that holds thesis, hypotheses, runs, memory, reports, and handoffs.

`invariance_research` owns:

- Postgres/SQLite migrations for:
  - `research_programs`
  - `program_members`
  - `program_events`
  - `program_artifacts`
  - `program_notes`
  - `program_report_snapshots`
- app routes:
  - `/app/programs`
  - `/app/programs/new`
  - `/app/programs/[id]`
- program library with status, last run, active hypothesis count, failed/promoted counts, and next action
- permissions: owner/account scoped, admin override, future team-ready shape
- attach existing analyses/imports to a program

`bulletproof_bt` owns:

- no runtime changes required
- provide program-facing terminology mapping in docs: hypothesis, experiment, run, verdict, state finding

Exit criteria:

- a user can create a program, attach an existing analysis, and see a timeline
- all program objects are tenant-scoped
- reports can reference a program ID when available

### Phase B2: Idea Intake And Clarification

Goal:

Turn raw English trading intuition into a structured research brief before any engine run.

`invariance_research` owns:

- create idea intake UI inside a program
- add fields:
  - market intuition
  - asset universe
  - timeframe
  - holding period
  - entry idea
  - exit idea
  - risk assumption
  - cost/slippage assumption
  - data source
  - what would disprove this
- implement clarification assistant endpoint using current LLM provider abstraction
- store clarification sessions and accepted answers
- require human acceptance before generating a hypothesis spec
- show "missing assumptions" as first-class cards, not hidden validation errors

`bulletproof_bt` owns:

- define `research_brief_v1` schema
- define required/optional fields by asset class and strategy family
- add fixture examples for trend, mean reversion, breakout, funding/liquidation, FX session, and equity/index cases

Exit criteria:

- user can type a vague idea and end with a structured research brief
- assistant cannot silently fill unknown strategy assumptions
- briefs are versioned and attached to program events

### Phase B3: Hypothesis Spec Contract

Goal:

Convert an accepted research brief into a falsifiable, engine-compatible hypothesis spec.

`bulletproof_bt` owns:

- formalize `hypothesis_spec_v1`
- include:
  - thesis
  - market mechanism
  - observable features
  - entry condition intent
  - exit condition intent
  - invalidation criteria
  - required datasets
  - cost model assumptions
  - benchmark/null comparison
  - expected failure modes
  - safe parameter ranges
  - out-of-sample plan
- add validation CLI:
  - `bt hypothesis validate <spec>`
  - `bt hypothesis explain-missing <spec>`
- add golden fixtures and tests
- map existing hypothesis YAML patterns into the new schema where possible

`invariance_research` owns:

- hypothesis spec editor and review UI
- version history and diff view
- approval state machine:
  - draft
  - needs clarification
  - approved for strategy generation
  - retired
- persistence:
  - `hypotheses`
  - `hypothesis_versions`
  - `hypothesis_approvals`
- display invalidation criteria and required evidence before the user can run experiments

Exit criteria:

- every program can hold one or more versioned hypotheses
- invalid specs fail closed with actionable repair guidance
- no experiment can be queued without an approved hypothesis version

### Phase B4: Strategy Spec Generation And Validation

Goal:

Generate or assemble engine-safe strategy specs from approved hypotheses.

`bulletproof_bt` owns:

- define `strategy_spec_v1`
- add validators for:
  - no lookahead
  - no interpolation assumptions unless explicit
  - data fields exist
  - timeframe compatibility
  - cost/slippage model declared
  - parameter ranges bounded
  - signal functions registered
- create strategy-spec-to-run-config compiler
- support safe templates for first strategy families:
  - trend continuation
  - mean reversion
  - breakout
  - volatility filter
  - funding/liquidation context where data exists
- add tests for invalid generated specs

`invariance_research` owns:

- strategy spec review UI
- assistant-generated spec proposal flow
- manual edit mode with schema validation
- approval gate before execution
- attach generated spec to hypothesis version
- show "what the assistant assumed" and "what the user approved"

Exit criteria:

- user can approve a generated strategy spec
- invalid specs never reach the experiment queue
- every run can trace back to user-approved hypothesis and strategy spec versions

### Phase B5: Experiment Planner And Queue

Goal:

Turn a strategy spec into a falsification-oriented experiment plan and queue.

`bulletproof_bt` owns:

- define `experiment_plan_v1`
- implement planner defaults:
  - baseline
  - cost sensitivity
  - slippage sensitivity
  - parameter grid
  - walk-forward or holdout split where data permits
  - benchmark/null comparison
  - regime/state split where data permits
  - alternative exit tests
- add validation for compute/data requirements
- expose CLI/service:
  - `bt experiment plan <strategy_spec>`
  - `bt experiment validate <plan>`

`invariance_research` owns:

- experiment plan review UI
- queue selected experiments from the web app
- database tables:
  - `experiment_plans`
  - `experiment_plan_items`
  - `experiment_jobs`
  - `experiment_job_events`
- queue controls:
  - pause
  - cancel
  - retry
  - priority
  - max concurrent per account
- entitlement limits by plan:
  - queued experiment count
  - concurrent runs
  - monthly compute budget
  - memory retention

Exit criteria:

- user can approve a plan and queue experiments
- queued experiments survive process restarts
- admin can see and manage jobs
- plan limits prevent runaway compute

### Phase B6: Engine Execution Service

Goal:

Run research experiments through `bulletproof_bt` continuously with durable artifacts.

`bulletproof_bt` owns:

- expose a stable research execution entrypoint for SaaS:
  - input: hypothesis spec, strategy spec, experiment plan item, data profile, runtime limits
  - output: run manifest, result artifacts, verdict cards, logs, failure classification
- adapt `orchestrator/research_daemon.py` and `orchestrator/run_experiment_pipeline.py` into tenant-safe service mode
- enforce deterministic/no-lookahead contracts
- emit:
  - run manifest
  - metrics
  - trades
  - equity curve
  - diagnostics
  - state findings
  - verdict bundle
  - terminal-grade intelligence cards
- add fixture-driven integration tests

`invariance_research` owns:

- new research worker or extension of analysis worker:
  - claims experiment jobs
  - calls engine service
  - streams status
  - uploads artifacts to R2
  - persists result envelope
- program run pages
- job heartbeat and stuck-job recovery
- first-pass worker deployment docs and health checks

Exit criteria:

- a queued experiment runs without manual shell access
- artifacts land in object storage
- run status and logs are visible in the SaaS app
- failed runs produce useful failure artifacts

### Phase B7: Verdict Cards And Result Interpreter

Goal:

Turn raw experiment outputs into decision-grade research feedback.

`bulletproof_bt` owns:

- standardize intelligence cards:
  - Hypothesis Card
  - Run Quality Card
  - Execution Drag Card
  - Failure Cause Card
  - Regime/State Dependency Card
  - Parameter Fragility Card
  - Null Comparison Card
  - Verdict Card
  - Next Experiment Card
- ensure cards consume only emitted artifacts
- add Markdown and JSON card outputs under run artifact directories
- register cards in research DB/artifact manifest

`invariance_research` owns:

- card renderer library
- run result page organized around:
  - verdict
  - why it failed or survived
  - evidence confidence
  - next experiment
  - artifact links
- program timeline integration
- report snapshot generation from run cards

Exit criteria:

- every completed run has cards
- missing artifacts degrade gracefully
- the UI can explain failure without raw notebook spelunking

### Phase B8: Tenant-Scoped Research Memory

Goal:

Make the system remember what each user has tried, what failed, what improved, and what to test next.

`bulletproof_bt` owns:

- memory extraction algorithms:
  - failure clustering
  - state bucket summaries
  - similar run lookup
  - parameter cliff detection
  - execution-cost kill patterns
  - promoted/scrapped criteria
- memory export contract for SaaS ingestion

`invariance_research` owns:

- Postgres memory tables:
  - `research_memory_items`
  - `research_memory_links`
  - `research_findings`
  - `program_recommendations`
  - `similar_run_index`
- tenant isolation and access checks
- memory search UI:
  - "similar failures"
  - "what changed"
  - "what should I test next"
  - "where did this thesis fail before"
- privacy guardrails:
  - no cross-tenant search
  - no cross-user learning by default
  - explicit future opt-in only

Exit criteria:

- a user can retrieve prior related failures within their own account
- next experiment recommendations cite evidence
- memory never leaks another tenant's run, symbol set, strategy text, or artifact

### Phase B9: Research Program Workbench

Goal:

Replace the analysis-only workbench with a coherent program workbench.

`invariance_research` owns:

- program overview:
  - thesis
  - active hypotheses
  - queued/running experiments
  - latest verdicts
  - promoted/scrapped candidates
  - memory highlights
  - next recommended actions
- hypothesis detail page
- experiment queue page
- run detail page
- memory page
- reports page
- import/audit tab for Approach A artifacts
- command palette for:
  - create hypothesis
  - queue next experiment
  - explain failure
  - find similar runs
  - generate report
  - request Research Desk review

`bulletproof_bt` owns:

- no UI ownership
- ensure engine outputs are stable enough for every workbench panel

Exit criteria:

- a user can manage research from program level without jumping between isolated analysis pages
- the old analysis pages still work for imported artifacts
- the main dashboard is no longer analysis-library-first

### Phase B10: Report, Share, And Research Desk For Programs

Goal:

Make proof artifacts work for both single runs and research program milestones.

`invariance_research` owns:

- program-level report snapshots
- report sections:
  - research question
  - hypotheses tested
  - experiments run
  - rejected variants
  - surviving candidates
  - evidence limits
  - next experiment plan
- Share Room for program snapshots
- Research Desk handoff packet:
  - hypothesis spec
  - strategy spec
  - experiment plan
  - run artifacts
  - verdict cards
  - memory summary
- admin reviewer workflow for program reviews

`bulletproof_bt` owns:

- package run artifacts into program-level evidence bundles
- emit program milestone summaries where enough run data exists

Exit criteria:

- a user can share not only one result but the reasoning path behind it
- Research Desk can review a complete program packet without asking engineering for raw files

### Phase B11: Billing, Entitlements, And Compute Economics

Goal:

Price and gate the real product: continuous research throughput.

`invariance_research` owns:

- replace upload/export-centric limits with:
  - programs
  - active hypotheses
  - queued experiments
  - concurrent experiments
  - monthly experiment runtime or credit budget
  - memory retention
  - assistant usage
  - Research Desk eligibility
- billing pages and Stripe mapping
- usage metering:
  - experiment queued
  - experiment runtime
  - artifact storage
  - assistant calls
  - exports/shares
- admin overrides for compute limits

`bulletproof_bt` owns:

- estimate cost/time per experiment plan item
- expose runtime budget hints before queueing
- classify heavy jobs requiring approval

Exit criteria:

- pricing maps to the product's real cost and value
- users understand what they are buying: research throughput and memory
- runaway compute is impossible by default

### Phase B12: Reliability, Security, And Production Runbooks

Goal:

Make the system safe for real user research programs.

`invariance_research` owns:

- production worker deployment for:
  - analysis/audit worker
  - export worker
  - research experiment worker
  - optional assistant worker
- queue backpressure and kill switches
- object storage lifecycle policies
- Postgres connection pooling rules
- rate limits and abuse controls
- audit logs for assistant-generated specs and approvals
- admin ops dashboards for research jobs and memory
- first-100-user monitoring

`bulletproof_bt` owns:

- deterministic replay tests
- artifact manifest integrity checks
- run resume/recovery docs
- daemon/service health checks
- data profile validation
- engine resource limits

Exit criteria:

- production can run research jobs continuously without manual babysitting
- stuck jobs are visible and recoverable
- every assistant-generated artifact has approval and provenance
- every engine run has a replayable manifest

### Phase B13: Beta Protocol For Research Pipeline

Goal:

Validate that the new product creates real pull before scaling it broadly.

Pilot customers:

- 3 systematic independent traders
- 2 crypto researchers with recurring hypotheses
- 2 strategy sellers/educators who need proof trails
- 2 prop-style operators testing challenge/strategy feasibility
- 1 emerging manager or allocator-style reviewer

Track:

- number of ideas entered
- number converted to approved hypotheses
- number of experiments queued per program
- time from idea to first verdict
- number of failed variants remembered
- number of next experiments accepted
- whether the user returns to continue the same program
- whether the user exports or shares a program report
- whether they would pay for more experiment throughput

Success signal:

Users do not merely upload files. They return because the system has become their research loop.

## Approach B Repo Ownership Summary

| Workstream | `invariance_research` | `bulletproof_bt` |
| --- | --- | --- |
| Product IA | Primary owner: routes, dashboard, copy, billing, public positioning. | Docs support only. |
| Research Programs | Primary owner: persistence, UI, permissions, events. | Provides terminology and engine linkage. |
| Idea Intake | Primary owner: forms, clarification assistant, session persistence. | Defines research brief schema and examples. |
| Hypothesis Specs | UI, persistence, approval state, version diffs. | Schema, validation CLI, fixtures, mapping to engine concepts. |
| Strategy Specs | Review UI, human approval, audit log. | Contract, compiler, validators, safe templates. |
| Experiment Planning | Plan review UI, queue records, entitlement gates. | Plan generator, validation, compute estimates. |
| Experiment Execution | Worker lifecycle, tenant/job records, storage, status UI. | Daemon/service execution, deterministic run artifacts. |
| Verdict Interpretation | Render cards, program timeline, reports. | Emit cards from artifacts, failure classifiers. |
| Research Memory | Tenant-scoped persistence, search UI, privacy controls. | Memory algorithms and export contracts. |
| Reports/Share/Desk | Snapshot, export, Share Room, Research Desk packet. | Program evidence bundle and milestone summaries. |
| Billing/Ops | Stripe, usage, admin, worker deployment, monitoring. | Runtime cost hints, health, replay/recovery docs. |

## Approach B Codex Implementation Order

1. Phase B0: product contract/navigation reframing.
2. Phase B1: Research Program data model and routes.
3. Phase B2: idea intake and clarification.
4. Phase B3: hypothesis spec contract and approval UI.
5. Phase B4: strategy spec generation and validation.
6. Phase B5: experiment planner and queue.
7. Phase B6: engine execution service and research worker.
8. Phase B7: verdict cards and result interpreter.
9. Phase B8: tenant-scoped research memory.
10. Phase B9: program workbench.
11. Phase B10: program reports, Share Room, and Research Desk handoff.
12. Phase B11: billing, entitlements, and compute economics.
13. Phase B12: reliability, security, and production runbooks.
14. Phase B13: beta protocol.

Implementation principle:

Do not build autonomous strategy generation first. Build a governed research pipeline where the user approves hypothesis specs, strategy specs, and experiment plans before compute runs. The system should accelerate disciplined research, not pretend to be an alpha vending machine.

## Target Architecture

```text
User
  |
  v
invariance_research Next.js App
  |-- public authority site
  |-- Research Pipeline / Research Program workspace
  |-- Strategy Robustness Lab as Audit Import mode
  |-- authenticated workspace
  |-- idea intake and clarification assistant
  |-- hypothesis spec editor and approval UI
  |-- experiment plan review and queue
  |-- upload inspection
  |-- diagnostic pages
  |-- report/export pages
  |-- command palette / explain layer / evidence alert center
  |-- prop evaluation readiness workspace
  |-- billing/entitlements
  |-- admin/research desk
  |
  v
TypeScript API + Services
  |-- research program lifecycle
  |-- hypothesis lifecycle
  |-- assistant clarification sessions
  |-- experiment queue lifecycle
  |-- upload intake
  |-- analysis creation
  |-- entitlement policy
  |-- queue records
  |-- export records
  |-- report renderer
  |-- evidence events and case-file timeline
  |-- prop evaluation profiles and result snapshots
  |-- product contracts
  |
  v
Worker Runtime
  |-- research experiment worker
  |-- analysis worker
  |-- export worker
  |-- health/heartbeat
  |
  v
Python Bridge
  |-- versioned request envelope
  |-- timeout/failure classification
  |-- probe command
  |
  v
bulletproof_bt
  |-- hypothesis_spec_v1 validator
  |-- strategy_spec_v1 validator/compiler
  |-- experiment_plan_v1 planner
  |-- research daemon/service execution
  |-- terminal-grade intelligence cards
  |-- tenant-safe memory export contract
  |-- bt.run_analysis_from_parsed_artifact
  |-- StrategyRobustnessLabService
  |-- diagnostics
  |-- prop evaluation feasibility
  |-- artifact contracts
  |-- benchmarks
  |-- hypothesis contracts
  |-- research memory
  |
  v
Storage
  |-- Postgres metadata
  |-- S3/R2 uploads and exports
  |-- benchmark manifests and datasets
  |-- immutable derived artifacts
  |-- program run artifacts
  |-- tenant-scoped research memory
```

## Contract Boundaries

### Web App Owns

- accounts and users
- auth and sessions
- plans and entitlements
- research programs
- hypothesis approval lifecycle
- assistant session persistence
- experiment queue lifecycle
- upload envelopes
- artifact ownership
- analysis/job lifecycle
- benchmark selection UI
- report/export lifecycle
- admin and research desk workflows
- product-safe errors
- public positioning
- user-facing contracts

### Engine Owns

- diagnostic computation
- strategy/backtest execution
- hypothesis spec validation
- strategy spec validation and compilation
- experiment plan generation
- research daemon execution semantics
- terminal-grade intelligence cards
- memory extraction algorithms
- execution assumptions
- robustness metrics
- Monte Carlo assumptions
- benchmark alignment logic
- artifact bundle definitions
- hypothesis-contract execution
- research-memory algorithms

### Shared Contract

The shared boundary should be small and versioned:

```text
ParsedArtifactInput
AnalysisRunConfig
EngineAnalysisResult
DiagnosticCapabilityProfile
EngineRunContext
DiagnosticPayload
ReportSourcePayload
ResearchBriefV1
HypothesisSpecV1
StrategySpecV1
ExperimentPlanV1
ExperimentRunEnvelope
VerdictCardV1
ResearchMemoryExportV1
```

The web app should never import engine internals. The engine should never know about web app sessions, Stripe plans, or React routes.

## Diagnostic Honesty Model

The product should always distinguish four states.

### Available

The artifact and engine support the diagnostic.

Example:

- trade distribution from trade-level CSV
- Monte Carlo by trade resampling when enough trades exist
- benchmark comparison when benchmark data overlaps the strategy period

### Limited

The system can compute a bounded proxy but cannot support a strong conclusion.

Example:

- regime comments from timestamps but no OHLCV
- execution stress using generic costs when venue-specific fee/spread data is absent
- ruin estimate without position sizing context

### Unavailable

The data cannot support the diagnostic.

Example:

- parameter stability without parameter sweep data
- MFE/MAE without market path around trades
- execution realism without fills, costs, spread, latency, or order assumptions

### Locked

The plan does not include the diagnostic.

Locked is not the same as unavailable. This distinction is central to trust.

## Product Surface Redesign

### Public `/robustness-lab`

Current surface exists and should stay.

Redesign priorities:

- replace generic "research instrument" copy with artifact-to-report specificity
- make the first CTA "Upload strategy evidence"
- show three concrete artifact types: trade CSV, structured bundle, research bundle
- preview the validation report, not just diagnostic categories
- include the sentence "We may tell you the data does not support your claim"
- route serious users to Research Desk without making the product feel manual-only

### `/app/new-analysis`

Current surface exists and should be upgraded.

Needed layout:

1. Upload artifact.
2. Inspection result.
3. Diagnostic eligibility.
4. Benchmark/runtime settings.
5. Run analysis.

Critical details:

- show accepted file docs inline
- show sample file downloads
- show upload limitations before upload
- show plan restrictions only after artifact class detection
- preserve current benchmark selector but explain why benchmark overlap matters
- add strategy name, claim field, and "what decision are you trying to make?" later

### `/app/analyses/[id]/overview`

Needed layout:

- verdict banner
- evidence sufficiency score
- trust/doubt columns
- unsupported conclusions
- benchmark status
- report CTA
- Research Desk CTA when limits matter

### Diagnostic Pages

Each page should answer:

- what was tested
- what the result says
- what evidence supports it
- what assumptions matter
- what is missing
- what to do next

Pages should not be chart-first.

### Report Page

The report is the saleable artifact.

It should have:

- reading mode
- export buttons
- sharing rules
- limitation appendix
- reproducibility appendix
- "request human review" action

## Data Model Additions For Approach A

Minimum additions or hardening:

Persistence performance requirements:

- index `report_snapshots(analysis_id, created_at DESC)` for owner report history
- index the current report snapshot lookup, either with `report_snapshots(analysis_id, superseded_by_report_id)` or an explicit current pointer
- index `evidence_ledger_snapshots(analysis_id, created_at DESC)`
- index share token lookup by token hash, never plaintext token
- index share access events by `share_id, created_at DESC`
- index prop evaluation rule profiles by `account_id, created_at DESC`
- index prop evaluation rule snapshots and result snapshots by `analysis_id, created_at DESC`
- index expired share/report cleanup by `expires_at` and status
- retention jobs delete or archive expired share access events without deleting immutable report snapshots needed for owner audit history
- Postgres and SQLite migrations stay schema-compatible until production provider is chosen

```text
analysis_engine_context
  analysis_id
  engine_name
  engine_version
  seam_version
  adapter_version
  parser_version
  benchmark_config_snapshot
  degraded
  degradation_reasons
  created_at

diagnostic_capabilities
  analysis_id
  diagnostic
  status
  reason
  required_inputs
  optional_enrichments
  source

report_snapshots
  report_id
  analysis_id
  report_version
  source_hash
  rendered_json_key
  rendered_md_key
  rendered_pdf_key
  created_at

prop_evaluation_rule_profiles
  profile_id
  account_id
  owner_user_id
  label
  firm_label
  rules_json
  rules_hash
  visibility
  created_at
  updated_at

prop_evaluation_rule_snapshots
  rule_snapshot_id
  analysis_id
  profile_id nullable
  source
  label
  rules_json
  rules_hash
  created_at

prop_evaluation_results
  result_id
  analysis_id
  rule_snapshot_id
  status
  verdict
  first_breach_json
  rule_status_json
  target_progress_json
  limitation_codes
  engine_payload_hash
  created_at
```

Likely next additions:

```text
strategies
  strategy_id
  account_id
  name
  description
  created_at

market_claims
  claim_id
  strategy_id
  plain_english
  asset_universe
  timeframe
  expected_edge
  invalidation_conditions
  assumptions
  created_at
```

Do not add cross-user research memory tables until tenant-scoped memory has clear product behavior.

## Testing Strategy

### `bulletproof_bt`

Add or preserve:

- seam probe test for `run_analysis_from_parsed_artifact`
- SaaS model fixture tests
- diagnostic eligibility tests
- parameter sweep bundle tests
- incomplete artifact tests
- non-finite artifact payload tests
- benchmark availability tests
- prop evaluation rule schema tests
- prop evaluation breach/target simulation tests
- prop evaluation recomputation fixture tests
- deterministic report payload snapshots

### `invariance_research`

Add or preserve:

- upload inspect tests by artifact class
- plan-lock versus unavailable tests
- analysis creation tests
- worker success/failure tests
- engine bridge failure classification tests
- `map-engine-analysis-record.ts` fixture tests
- report/export snapshot tests
- prop evaluation runtime rule capture tests
- prop evaluation post-run recomputation tests
- prop evaluation entitlement and fallback-label tests
- admin retry tests
- Postgres repository parity tests where production depends on Postgres
- public page smoke tests for `/robustness-lab`, `/strategy-validation`, `/pricing`, and `/research-desk`

### Cross-Repo Contract Tests

Add a small shared fixture pack:

```text
fixtures/engine-seam/
  trade_csv_basic.json
  trade_csv_limited.json
  structured_bundle_full.json
  parameter_sweep_full.json
  prop_eval_pass.json
  prop_eval_daily_loss_breach.json
  prop_eval_trailing_drawdown_breach.json
  prop_eval_limited_intraday_path.json
  prop_eval_recompute_same_run.json
  malformed_engine_payload.json
```

Use it in both repos:

- engine repo proves it emits the fixture shape
- web repo proves it maps the fixture shape

### Engineering Test Coverage Map

The wedge acceptance matrix must be implemented as layered tests, not only product acceptance bullets. Slice 1 cannot be considered complete until the cross-repo contract and evidence ledger paths are covered by deterministic fixtures.

```text
CODE PATHS                                                     USER FLOWS
[+] bulletproof_bt seam emission                               [+] Upload inspection
  |-- [GAP][UNIT] EngineEnvelopeV1 fields                         |-- [GAP][INTEGRATION] Trade CSV only -> available/limited/unavailable
  |      tests/test_saas_engine_envelope.py                       |      src/__tests__/evidence/upload-ledger-flow.test.ts
  |-- [GAP][UNIT] malformed/non-finite payload fails closed        |-- [GAP][INTEGRATION] Plan lock overlays evidence state
  |      tests/test_saas_engine_envelope.py                       |      src/__tests__/evidence/entitlement-overlay.test.ts
  |-- [GAP][CONTRACT] canonical fixture pack emitted              |-- [GAP][E2E] Validation packet template unlocks expected diagnostics
         tests/test_engine_seam_fixtures.py                             tests/validation-packet-templates.test.ts

[+] invariance_research seam adapter                            [+] Analysis completion
  |-- [GAP][CONTRACT] EngineEnvelopeV1 accepted                    |-- [GAP][INTEGRATION] worker creates ledger snapshot before report snapshot
  |      tests/engine-envelope-contract.test.ts                    |      tests/analysis-ledger-report-flow.test.ts
  |-- [GAP][CONTRACT] unknown diagnostic fails closed              |-- [GAP][INTEGRATION] engine skipped diagnostic preserves reason
  |      tests/engine-envelope-contract.test.ts                    |      tests/analysis-ledger-report-flow.test.ts
  |-- [GAP][SNAPSHOT] fixture -> AnalysisRecord projection         |-- [GAP][INTEGRATION] LLM contradiction cannot override ledger
         src/__tests__/analysis/engine-fixture-mapping.test.ts           tests/llm-ledger-authority.test.ts

[+] EvidenceLedgerService                                       [+] Report generation
  |-- [GAP][UNIT] supported/limited/unsupported/contradicted       |-- [GAP][INTEGRATION] completed analysis -> immutable report snapshot
  |      src/__tests__/evidence/evidence-ledger-service.test.ts    |      tests/report-snapshot-state-machine.test.ts
  |-- [GAP][UNIT] entitlement overlay does not rewrite evidence    |-- [GAP][INTEGRATION] retry/rerun supersedes or marks stale
  |      src/__tests__/evidence/entitlement-overlay.test.ts        |      tests/report-snapshot-state-machine.test.ts
  |-- [GAP][SNAPSHOT] report/share/LLM projections                 |-- [GAP][INTEGRATION] double-click export/share is idempotent
         src/__tests__/evidence/evidence-projections.test.ts             tests/report-snapshot-state-machine.test.ts

[+] Share-safe projection                                       [+] Shared report access
  |-- [GAP][UNIT] SharedReportViewModel allowlist                  |-- [GAP][E2E] active token renders report-safe payload only
  |      tests/share-report-threat-model.test.ts                   |      tests/share-report-threat-model.test.ts
  |-- [GAP][UNIT] sensitive fields redacted                         |-- [GAP][E2E] expired/revoked/superseded token fails closed
  |      tests/share-report-threat-model.test.ts                   |      tests/share-report-threat-model.test.ts
  |-- [GAP][UNIT] share route cannot call artifact download API     |-- [GAP][E2E] raw upload URL inaccessible from share context
         tests/share-report-threat-model.test.ts                         tests/share-report-threat-model.test.ts
```

Coverage targets for Slice 1:

- `bulletproof_bt`: 100% branch coverage for `EngineEnvelopeV1` construction, malformed payload handling, unsupported diagnostics, and fixture serialization.
- `invariance_research`: 100% branch coverage for `EvidenceLedgerService`, entitlement overlays, engine envelope validation, and adapter fail-closed behavior.
- Cross-repo fixtures: every fixture in `fixtures/engine-seam/` must be consumed by tests in both repos.

Coverage targets for Slice 2:

- report snapshot generation, regeneration, stale/superseded behavior, idempotency, export rendering from snapshots, and invalid transitions.

Coverage targets for Slice 3:

- share token lifecycle, least-privilege projection, redaction, expiry, revocation, superseded report behavior, and no raw artifact access.

## Product Metrics

Track:

- upload attempt count
- upload rejection reasons
- accepted artifact classes
- diagnostics available per analysis
- diagnostics limited per analysis
- diagnostics unavailable per analysis
- analysis completion rate
- engine failure rate
- time to completed report
- report export rate
- Research Desk request rate
- upgrade CTA click from locked diagnostics
- return uploads per strategy/account
- percentage of reports with unsupported conclusions
- percentage of users who upload richer artifacts after seeing limitations

The most important early metric:

> How often does a user take action after the product says "your data does not support that conclusion"?

## Risks And Mitigations

### Risk: Users Want Confirmation, Not Truth

Mitigation:

- target credibility-seeking users first
- make shareable reports the value, not positive verdicts
- sell "defensible evidence" rather than "better backtests"

### Risk: Current Report Is Not Premium Enough

Mitigation:

- improve report content contract before visual polish
- make limitations and reproducibility appendix excellent
- use Research Desk as premium path while automation improves

### Risk: Artifact Messiness Overwhelms Intake

Mitigation:

- make parser failures useful
- publish templates
- show exact missing fields
- treat inspection as a product experience

### Risk: Engine/App Contracts Drift

Mitigation:

- version seam payloads
- add golden fixtures
- test adapters against real engine outputs
- fail closed on unknown diagnostics

### Risk: Research Memory Creates Privacy Problems

Mitigation:

- tenant-scoped memory first
- no cross-user learning by default
- explicit opt-in aggregation only after legal/product review
- keep recommendations evidence-only

### Risk: Execution Realism Claims Outrun Data

Mitigation:

- display execution assumptions clearly
- grade realism instead of claiming binary validity
- require venue/fill/cost context for strong execution conclusions

## Do Not Build First

- live trading
- broker execution
- public strategy marketplace
- social feed
- public leaderboards
- fully autonomous strategy generation without approval gates
- natural-language compiler that bypasses hypothesis/spec review
- portfolio allocator
- cross-user intelligence
- complex visual market-structure explorer

These can exist later. They do not prove the wedge.

## The Assignment

Before broadening the platform beyond Approach B v1, collect ten real research programs from the exact first customers:

- two serious independent traders
- two strategy sellers or educators
- two systematic traders
- two crypto-native researchers
- two emerging fund, allocator, or prop-style operators

For each program, record:

- what raw idea they entered
- what clarification questions mattered
- what hypothesis spec was approved
- what strategy spec was generated or edited
- what experiments were queued
- how long it took to reach first verdict
- what failed and why
- what next experiment the system recommended
- whether the user accepted the next experiment
- whether memory helped avoid repeated work
- whether they exported or shared a report
- whether they would pay for more research throughput
- whether they requested deeper Research Desk review

This is the demand test. If users return to continue a research program, accept next experiments, and pay for more throughput, the product has real pull. If they only want one-off confirmation, keep upload validation as a side door and sell the product to serious systematic researchers instead.

## Final Recommendation

Build the full ambition, but start with the narrowest working Research Pipeline.

First:

> Make `invariance_research` a Research Program workbench where raw trading intuition becomes approved hypothesis specs, queued experiments, verdict cards, memory, and proof reports.

Then:

> Keep Strategy Robustness Lab as the import/audit/report subsystem for external artifacts and existing results.

Eventually:

> Become the claim-first strategy research OS.

The sequence matters. The existing app is not a throwaway prototype. It is the right SaaS shell. The job now is to productize the process inside `bulletproof_bt`: disciplined hypothesis formation, safe strategy spec generation, continuous experiment execution, hostile interpretation, and persistent research memory.
