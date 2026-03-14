## Copyright

Copyright © 2026 Chinedum Iwueke.

# Bulletproof BT

bulletproof_bt is a deterministic, event-driven quantitative research engine designed for institutional-grade strategy validation across crypto, foreign exchange, equities, and basic futures modeling.

It is built around a strict invariant:

> Same data + same configuration = identical outputs.  
> No lookahead. No interpolation. No silent assumptions.

bulletproof_bt is a reproducible research system.

---

## System Philosophy

bulletproof_bt enforces explicit contracts between:
- Strategy
- Risk Engine
- Execution Model
- Portfolio
- Data Feed
- Benchmark Layer
- Artifact Outputs

Each layer is deterministic, validated, and version-stable.

Every run produces a structured artifact bundle suitable for audit, client delivery, or regression locking.

---

## Market Support (V1 Feature Freeze)

### Crypto (24/7 Markets)
- Tiered execution profiles (tier1 / tier2 / tier3 / custom)
- Spread, slippage, and fee modeling
- Deterministic intrabar pricing
- Stop-distance based risk normalization
- Buy & hold benchmark

---

### Foreign Exchange (24x5)
- Mandatory spread modeling (entry and exit)
- Commission per lot (configurable)
- Lot-size rounding (micro / mini / standard)
- Risk-percentage position sizing
- Basic leverage and margin modeling
- Weekend enforcement
- Flat or baseline-strategy benchmark

---

### Equities (Session-Based)
- Commission per share or per trade
- Market hours enforcement
- Gap-preserving behavior
- Cash-account modeling (no implicit leverage)
- Buy & hold or baseline benchmark

---

## Core Design Principles

### Determinism
- Fully reproducible runs
- Config resolution canonicalization
- Schema-versioned artifacts
- No stochastic execution components

---

### Strict No-Lookahead
- Strategies receive closed bars only
- Higher-timeframe resampling enforces strict completeness
- No future leakage permitted

---

### Explicit Cost Modeling

Execution modeling includes:
- Spread (entry + exit)
- Slippage
- Fees
- Commissions
- Margin usage

All cost components are surfaced in output artifacts.

---

### Risk Normalization

Strategies are evaluated using:
- % equity risk per trade
- Stop-distance based sizing
- R-multiple normalization
- Margin-aware execution constraints

---

## Instrument Abstraction Layer

All markets are modeled via explicit instrument specifications:

```yaml
instrument:
  type: forex | equity | crypto | futures
  symbol: EURUSD
  tick_size: 0.0001
  contract_size: 100000
  pip_value: auto
```

This eliminates hardcoded crypto assumptions and ensures execution and risk logic adapt correctly per asset class.

---

## Benchmark Framework

Supported benchmark modes:
- buy_hold
- flat (no-trade baseline)
- baseline_strategy (e.g., MA cross)

Benchmark artifacts include:
- benchmark_equity.csv
- benchmark_metrics.json
- comparison_summary.json

---

## Data Contract

Supported input modes:
- Single-file dataset
- Dataset directory with manifest (recommended)

Validation guarantees:
- UTC tz-aware timestamps
- Strict monotonic ordering
- OHLC consistency checks
- Duplicate detection
- Session enforcement (FX/equity)
- No interpolation of missing bars

---

## Run Artifact Contract

Every run produces:

```text
run_xxx/
  config_used.yaml
  performance.json
  equity.csv
  trades.csv
  fills.jsonl
  decisions.jsonl
  performance_by_bucket.csv
  cost_breakdown.json
  summary.txt
  run_manifest.json
  run_status.json
  benchmark_* (if enabled)
```

Artifacts are stable and schema-versioned.

---

## Explicitly Out of Scope (V1)

To preserve rigor and reproducibility, V1 intentionally excludes:
- Multi-strategy blending
- Portfolio allocation engines
- Tick-level simulation
- Order book modeling
- Swap/rollover modeling
- Multi-broker comparison
- Web dashboards

bulletproof_bt V1 is a single-strategy institutional research OS.

---

## Project Structure

```text
src/bt/
  core/            # Engine loop, configuration resolution
  data/            # Dataset loading, validation, resampling
  execution/       # Execution profiles, pricing, slippage, spread
  risk/            # Position sizing, margin, stop handling
  portfolio/       # Cash, positions, liquidation logic
  metrics/         # Performance computation, attribution
  logging/         # Artifact writers, summary, run status
  benchmark/       # Benchmark modes and comparison layer
  instruments/     # Instrument abstraction layer (FX/equity/crypto)

configs/
  engine.yaml      # Stable system defaults
  packs/           # Market-specific packs (crypto, fx_trad_v1)
  overrides/       # Strategy experiment overrides

scripts/
  run_backtest.py
  run_experiment_grid.py

tests/
  Deterministic regression + contract validation
```

Core engine modules remain instrument-agnostic. Market differences are handled through instrument specs and execution adapters.

---

## Prerequisites

### System Requirements
- Python 3.10+
- Linux, macOS, or WSL recommended
- 8GB+ RAM (16GB+ recommended for multi-asset research)

---

### Required CLI Tools

Recommended:
- git
- rg (ripgrep) for repository inspection
- tree (optional)
- make (if Makefile commands are used)

## Release Status

The engine is:
- Instrument-aware
- Spread-aware
- Margin-aware
- Risk-normalized
- Benchmark-contextualized
- Deterministic
- Regression-locked

---
## Install

bulletproof_bt uses modern PEP 621 packaging. All dependencies are defined in pyproject.toml.

```bash
git clone https://github.com/Chinedum-iwueke/bulletproof_bt.git
cd bulletproof_bt

python -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
```
Run tests to verify installation:

```bash
pytest -q
```

## Run a backtest (CLI)

```bash
python scripts/run_backtest.py --data <PATH> --config configs/engine.yaml
```

## Run an experiment grid (CLI)

```bash
python scripts/run_experiment_grid.py --config configs/engine.yaml --experiment configs/experiments/h1_volfloor_donchian.yaml --data <PATH> --out <OUT_DIR>
```

### Quick-slice workflow (e.g., 6 months on BTC only)

Use a local overlay with data-scope controls:

```yaml
data:
  symbols_subset: ["BTCUSDT"]
  max_symbols: 1
  date_range:
    start: "2023-01-01T00:00:00Z"  # inclusive
    end: "2023-07-01T00:00:00Z"    # exclusive
```

Then run:

```bash
python -u scripts/run_experiment_grid.py   --config configs/engine.yaml   --experiment configs/experiments/h1_volfloor_donchian.yaml   --data <PATH>   --out outputs/grids   --local-config configs/local/engine.volfloor_donchian.yaml
```

Keep `--local-config` for single-asset / short-window checks, and remove or swap it when running full-universe experiments.

## Overrides (recommended workflow)

- Add one or more overlays with `--override path/to/override.yaml` (flag is repeatable).
- For local-only edits, use `--local-config configs/local/engine.local.yaml`.
- Effective merge order is:
  1. base config (`--config`)
  2. `configs/fees.yaml`
  3. `configs/slippage.yaml`
  4. each `--override` in the order provided
  5. `--local-config` (if supplied)


## Execution profiles

Execution profiles are reusable presets. Overrides are only allowed when `execution.profile: custom`.
If you use `tier1`, `tier2`, or `tier3` (or omit the profile, which defaults to `tier2`), do not set any of:
`maker_fee`, `taker_fee`, `slippage_bps`, `delay_bars`, `spread_bps`.

Valid tier preset example (no overrides):

```yaml
execution:
  profile: tier2
```

If you enable spread modeling with `execution.spread_mode: fixed_bps` while using a tier preset,
`execution.spread_bps` is auto-filled from the tier (`tier1=0.0`, `tier2=1.0`, `tier3=3.0`).

Valid custom example (all override fields required):

```yaml
execution:
  profile: custom
  maker_fee: 0.0
  taker_fee: 0.001
  slippage_bps: 2.0
  delay_bars: 1
  spread_bps: 1.0
```

## Stop Contract (Safe vs Strict)

Strategies should provide stop intent on entry signals via either:
- `signal.stop_price`, or
- `signal.metadata.stop_spec`

Use the client-safe pack (fallback explicitly enabled):

```bash
python scripts/run_backtest.py \
  --data <PATH> \
  --config configs/engine.yaml \
  --override configs/examples/safe_client.yaml
```

Use the research-strict pack (no fallback/proxy sizing):

```bash
python scripts/run_backtest.py \
  --data <PATH> \
  --config configs/engine.yaml \
  --override configs/examples/strict_research.yaml
```

Minimal `stop_spec` example in strategy code:

```python
signal = Signal(
    ts=ts,
    symbol=symbol,
    side=Side.BUY,
    signal_type="entry",
    confidence=1.0,
    metadata={
        "stop_spec": {"contract_version": 1, "kind": "atr", "atr_multiple": 2.0}
    },
)
```

## Public API

```python
from bt import run_backtest, run_grid

run_dir = run_backtest(
    config_path="configs/engine.yaml",
    data_path="data/curated/sample.csv",
    out_dir="outputs/runs",
)

experiment_dir = run_grid(
    config_path="configs/engine.yaml",
    experiment_path="configs/experiments/h1_volfloor_donchian.yaml",
    data_path="data/curated/sample.csv",
    out_dir="outputs/experiments",
)
```

## How to add a strategy

1. Copy `src/bt/strategy/templates/client_strategy_template.py`.
2. Rename class/file and place your strategy in `src/bt/strategy/`.
3. Register it with `register_strategy(...)`.
4. Strategy must emit `Signal` objects only.
5. `ctx` is read-only (`StrategyContextView`).

### DO NOT

- Do **not** edit `bt/core/engine.py`.
- Do **not** mutate `ctx`.
- Do **not** access portfolio/execution internals from a strategy.


## Data market modes

Crypto defaults (no market key needed):

```yaml
data:
  mode: streaming
```

FX 24x5 example:

```yaml
data:
  market: fx_24x5
  allow_weekend_bars: false
```

Equity session example:

```yaml
data:
  market: equity_session
  equity_session:
    timezone: America/New_York
    open_time: "09:30"
    close_time: "16:00"
    trading_days: [Mon, Tue, Wed, Thu, Fri]
```

## Streaming indicator library

All indicators are stateful and updated bar-by-bar (`update(bar)`), with explicit warmups (`warmup_bars`) and no lookahead.

### Trend / Moving averages
- EMA, SMA, WMA, DEMA, TEMA, HMA
- KAMA, RMA, VWMA, T3

### Momentum / Oscillators
- RSI, Stochastic, Stoch RSI
- CCI, ROC, Momentum, Williams %R
- TSI, Ultimate Oscillator, Fisher Transform

### Volatility / Bands / Channels
- True Range, ATR
- Bollinger Bands, Keltner Channel, Donchian Channel
- Choppiness Index, Ulcer Index, Historical Volatility

### Trend strength / directional movement
- DMI/ADX, Aroon
- MACD, PPO, TRIX, Vortex

### Volume / Money flow
- OBV, CMF, MFI
- VPT, ADL, Chaikin Oscillator, Force Index

### Range / price-action / stops
- Parabolic SAR, Supertrend
- Pivot Points (streaming daily UTC session pivots)
- Heikin Ashi

### Candle features
- Body/range/wicks/body ratio
- Gap, close position in range
- Rolling z-scores for returns/range/volume

### Usage

```python
from bt.indicators import make_indicator

ind = make_indicator("rsi", period=14)
for bar in bars:
    ind.update(bar)
    if ind.is_ready:
        print(ind.value)
```

## Troubleshooting: Parquet / PyArrow

- Symptom: `AttributeError: module 'pyarrow' has no attribute 'parquet'` when pandas reads/writes parquet.
- Cause: in some environments `import pyarrow.parquet` works, but `pyarrow.parquet` (`pa.parquet`) is not attached as a module attribute that pandas may expect.
- Implemented fix: parquet IO now runs a runtime guard `ensure_pyarrow_parquet()` before parquet operations.
- Quick workaround: upgrade `pyarrow`/`pandas`, or run `import pyarrow.parquet` before parquet IO.
- In this project, the guard is already applied, so manual import is usually unnecessary.

## Run artifacts

`run_dir/performance.json` includes cost-attribution keys (always present):

- `gross_pnl`
- `net_pnl`
- `fee_total`
- `slippage_total`
- `spread_total`
- `fee_drag_pct`
- `slippage_drag_pct`
- `spread_drag_pct`

## Client contracts

- [docs/dataset_contract.md](docs/dataset_contract.md)
- [docs/data_market_contract.md](docs/data_market_contract.md)
- [docs/execution_model_contract.md](docs/execution_model_contract.md)
- [docs/strategy_contract.md](docs/strategy_contract.md)
- [docs/portfolio_risk_contract.md](docs/portfolio_risk_contract.md)
- [docs/error_and_run_status_contract.md](docs/error_and_run_status_contract.md)
- [docs/output_artifacts_contract.md](docs/output_artifacts_contract.md)
- [docs/config_layering_contract.md](docs/config_layering_contract.md)
- [docs/beginner_vs_pro_contract.md](docs/beginner_vs_pro_contract.md)
