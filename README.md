# Bulletproof BT

Scaffold for an event-driven, bar-by-bar backtesting engine.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
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
python -u scripts/run_experiment_grid.py   --config configs/engine.yaml   --experiment configs/experiments/h1_volfloor_donchian.yaml   --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical   --out outputs/grids   --local-config configs/local/engine.volfloor_donchian.locked_btc_audit.yaml
```

That command is correct. Keep `--local-config` for single-asset / short-window checks, and remove or swap it when running full-universe experiments.

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
