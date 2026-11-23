# Method assurance contracts

`design-adequacy-contract-v2` adds `assurance` to the existing structured design
contract. V1 remains replayable; it does not acquire these checks retroactively.
This increment has local native validation and matching control-plane/worker
contract changes, not deployed authoring support or scientific acceptance.

## Definitions

`definitions` is a bounded list of uniquely identified `primitive` or `derived`
nodes. Each has `dependencies`, `statement`, `evidence_digest` and `pointer`.
Supply either a new inline statement or an exact content-addressed evidence
reference and RFC 6901 JSON pointer, with the unused alternative set to null.
Evidence is supplied through the existing diagnostic evidence catalog. Reference
resolution never fetches arbitrary URLs or executes a definition.

Primitives cannot depend on other nodes; derived nodes require dependencies.
All dependencies and `required_outputs` must exist. Cycles are rejected; lagged
state must be represented through initialized inputs and explicit transitions.
Duplicate definitions are rejected, including a symbol declared both primitive
and derived. Dependency completeness, time semantics and mathematical validity
still require independent review. A resolved prose statement is not a proof
that its equation is complete or correct.

## Work ledger

`workload` contains `caps` (unit name to positive integer bound) and `stages`.
Each stage declares `id`, `phase`, `unit`, `factors` and
`not_applicable_reason`. Phase coverage includes generation, fit, solve,
verification and bootstrap. Counted stages have positive integer upper-bound
factors and a null exemption reason. Exempt phases need a substantive reason,
an empty factors object and null unit; reviewers assess whether the exemption
is legitimate.

Products of factors are summed by unit, never across incomparable units.
Every counted unit needs a cap. `statistic_evaluation` must equal the calibration
`max_work_units` bound. Other caps are proposals, not authorization to consume
those resources. Matching authoring changes freeze approved caps and output
obligations while allowing bounded corrections to definitions/factors; the API
also checks these invariants when verifying the published result.

For example, a solver ledger may count starts, iterations, Jacobian evaluations
and histories per evaluation. Reusing innovations does not eliminate evaluation
work. Factors are declared upper bounds: a structural pass cannot establish
that an author counted every operation or that a runtime enforces those bounds.
Those remain explicit unverified obligations and execution gates.

## Boundary and commissioning

The existing `design_adequacy_v1` diagnostic transports both contract versions
and binds the new implementation file into its receipt. V2 findings request
method correction before scientific review. V2 never runs calibration or grants
engineering, market-data, order or promotion authority. The structured-calibration
execution lane remains unavailable until separately implemented and authorized.

Before production use, finish cross-repository parity and adversarial validation
of the matching worker/API contracts and strict author output, release an authorized pinned source,
and activate an exact-digest successor. Historical campaign contracts stay intact.
The current BTC retained-evidence audit is one regression case, not a special
case in the validator.
