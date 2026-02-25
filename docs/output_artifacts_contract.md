# Output Artifacts Contract

## Stable contract (Stage F)

### Always-present artifacts
Required artifact validation currently enforces:
- `config_used.yaml`
- `performance.json`
- `equity.csv`
- `trades.csv`
- `fills.jsonl`
- `decisions.jsonl`
- `performance_by_bucket.csv`
- `run_status.json` (required by artifacts manifest contract)

Repo Evidence: `src/bt/logging/run_contract.py::REQUIRED_ARTIFACTS`, `src/bt/logging/artifacts_manifest.py::_artifact_definitions`, `tests/test_run_artifact_contract.py`.

### Conditional artifacts
- Benchmark-enabled:
  - `benchmark_equity.csv`
  - `benchmark_metrics.json`
  - `comparison_summary.json`
- Scope-knobs active:
  - `data_scope.json`
- Summary enabled flow:
  - `summary.txt`

Repo Evidence: `src/bt/logging/artifacts_manifest.py::_artifact_definitions`, `src/bt/api.py::run_backtest`, `src/bt/logging/trades.py::write_data_scope`.

## Artifact purpose + minimum schema guarantee
| Artifact | Purpose | Minimum guarantee |
| --- | --- | --- |
| `config_used.yaml` | resolved run config | YAML mapping of resolved config |
| `run_status.json` | pass/fail + diagnostics | includes `schema_version`, status/error fields |
| `equity.csv` | time-series account state | header includes equity/cash/pnl/margin fields |
| `trades.csv` | closed-trade ledger | stable writer columns listed below |
| `fills.jsonl` | fill events | per-line JSON with canonical `fee_cost/slippage_cost/spread_cost` enrichment |
| `decisions.jsonl` | per-step strategy/risk decisions | per-line JSON records |
| `performance.json` | aggregate metrics | includes `schema_version` and key top-level metrics |
| `performance_by_bucket.csv` | bucketed EV view | columns: `bucket,n_trades,ev_net` |
| `cost_breakdown.json` | machine-readable cost totals for reporting | includes `schema_version`, `totals`, and `notes` |

Repo Evidence: `src/bt/logging/trades.py::TradesCsvWriter`, `src/bt/core/engine.py::_write_equity_header`, `src/bt/logging/jsonl.py::JsonlWriter.write`, `src/bt/metrics/performance.py::write_performance_artifacts`.

### Stable columns: `trades.csv`
Writer `_columns` contract:
- `entry_ts`
- `exit_ts`
- `symbol`
- `side`
- `qty`
- `entry_qty` *(entry-sized quantity used for risk normalization; defaults to `qty` when unavailable)*
- `exit_qty` *(realized/closed quantity for the trade row; mirrors `qty`)*
- `entry_price`
- `exit_price`
- `pnl`
- `pnl_price`
- `fees_paid`
- `pnl_net`
- `fees`
- `slippage`
- `mae_price`
- `mfe_price`
- `risk_amount` *(nullable; risk metadata dependent)*
- `stop_distance` *(nullable; risk metadata dependent)*
- `entry_stop_distance` *(nullable; initial stop distance used for risk normalization when available)*
- `r_multiple_gross` *(nullable when risk_amount missing/invalid)*
- `r_multiple_net` *(nullable when risk_amount missing/invalid)*

Repo Evidence: `src/bt/logging/trades.py::TradesCsvWriter._columns`, `src/bt/logging/trades.py::TradesCsvWriter.write_trade`.

### JSON schema versioning status
- `performance.json`: has `schema_version` (`PERFORMANCE_SCHEMA_VERSION`, now `2`) with additive extensions:
  - `costs.{fees_total,slippage_total,spread_total,commission_total}`
  - `margin.{peak_used_margin,avg_used_margin,peak_utilization_pct,avg_utilization_pct,min_free_margin,min_free_margin_pct}`
- `cost_breakdown.json`: has `schema_version` (`1`) with stable totals + reporting notes.
- `run_status.json`: has `schema_version` (`RUN_STATUS_SCHEMA_VERSION`).
- `benchmark_metrics.json` and `comparison_summary.json`: have schema versions.
- Schema bump policy: additive-only payload changes are preferred; bump schema only when adding a contract-level block or making a non-backward-compatible change.

Repo Evidence: `src/bt/contracts/schema_versions.py`, `src/bt/metrics/performance.py::write_performance_artifacts`, `src/bt/logging/cost_breakdown.py::write_cost_breakdown_json`, `src/bt/experiments/grid_runner.py::_write_run_status`, `tests/test_output_extensions_costs_and_margin.py`.

## FAQ / common failure modes
- **Artifact validator fails for missing files**  
  Treat as hard failure; run did not satisfy Stage F contract.

- **`summary.txt` missing**  
  It is conditional in manifest contract; validate required artifacts first.

- **Can I parse `trades.csv` by position index?**  
  No, parse by header names; rely on stable column names above.

Repo Evidence: `src/bt/logging/run_contract.py`, `src/bt/logging/artifacts_manifest.py`, `src/bt/logging/summary.py`.

## Order side derivation rule

Order direction is canonically derived from signed `order_qty` (delta quantity): positive => `BUY`, negative => `SELL`, zero is invalid. Decision logging and JSONL write-boundary invariants enforce that `order.side`, `order_qty` sign, and `signal.side` (when present) are aligned before persistence.

