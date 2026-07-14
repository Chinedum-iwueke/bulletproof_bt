# Research Data Subsystem

`bt.research_data` builds local canonical perpetual futures and spot datasets under `research_data/`.

It stores:

- 1m trade OHLCV candles
- 1m mark-price candles
- 1m index-price candles
- funding events
- historical open interest where each exchange exposes it
- canonical research panels for Bulletproof_bt loaders

Perpetual futures support Binance, Bybit, and OKX adapters. Spot support is
available for Binance and Bybit. Exchange APIs are always
called with the exchange-native symbol, while cross-exchange research uses a
canonical market-specific symbol.

All timestamps are normalized to UTC. The storage layer performs atomic parquet upserts by reading the existing file, concatenating new rows, dropping duplicate keys, sorting by timestamp, and renaming a temporary parquet file into place.

## Layout

```text
research_data/
  manifests/
    instruments.parquet
    fetch_state.parquet
    coverage.parquet
    validation_report.parquet
    stable_universe.parquet
    stable_universe_spot.parquet
    volatile_universe_membership.parquet
    volatile_universe_membership_spot.parquet
  raw/
    perp/<exchange>/<native_symbol>/
      ohlcv/timeframe=1m/data.parquet
      mark/timeframe=1m/data.parquet
      index/timeframe=1m/data.parquet
      funding/timeframe=event/data.parquet
      oi/timeframe=5m/data.parquet
    spot/<exchange>/<native_symbol>/
      ohlcv/timeframe=1m/data.parquet
  canonical/
    perp/<exchange>/<native_symbol>/timeframe=1m/
      ohlcv.parquet
      perp_features.parquet
      research_panel.parquet
    spot/<exchange>/<native_symbol>/timeframe=1m/
      ohlcv.parquet
      research_panel.parquet
```

Older perp files in `research_data/raw/<exchange>/...` and
`research_data/canonical/<exchange>/...` remain readable for backward
compatibility. New writes use the explicit `perp` or `spot` namespace so native
symbols such as Binance `BTCUSDT` cannot collide across markets.

## Causal Joins

Panel construction is backtest-safe:

- OHLCV, mark, and index candles join exactly on candle-open `ts`.
- Funding joins with `merge_asof(..., direction="backward")`.
- Open interest joins with `merge_asof(..., direction="backward")`.
- `funding_source_ts` and `oi_source_ts` are preserved.
- Validation fails if either source timestamp is later than the panel bar timestamp.
- Missing OHLCV candles are reported; they are never interpolated or forward-filled.

Binance does not provide true historical 1m open interest through the public historical endpoint. The adapter fetches `/futures/data/openInterestHist` at `period=5m`; panels use causal backward as-of joins from those observations.

## Example Commands

Refresh the canonical instrument map for every supported exchange:

```bash
python -m bt.research_data.cli refresh-instruments --exchange all
```

Refresh spot instruments for Binance and Bybit:

```bash
python -m bt.research_data.cli refresh-instruments --exchange binance --market spot
python -m bt.research_data.cli refresh-instruments --exchange bybit --market spot
```

Resumable chunked backfill for one dataset:

```bash
python -m bt.research_data.cli fetch-backfill \
  --market perp \
  --exchange binance \
  --dataset ohlcv \
  --symbol BTCUSDT \
  --timeframe 1m \
  --start 2021-01-01 \
  --end now
```

Resumable spot OHLCV backfill for one symbol:

```bash
python -m bt.research_data.cli fetch-backfill \
  --market spot \
  --exchange binance \
  --dataset ohlcv \
  --symbol BTCUSDT \
  --timeframe 1m \
  --start 2021-01-01 \
  --end now
```

Backfill Bybit or OKX using native symbols:

```bash
python -m bt.research_data.cli backfill \
  --exchange bybit \
  --symbols BTCUSDT,ETHUSDT \
  --start 2021-01-01 \
  --end now \
  --datasets ohlcv,mark,index,funding,oi \
  --timeframe 1m

python -m bt.research_data.cli backfill \
  --exchange okx \
  --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \
  --start 2021-01-01 \
  --end now \
  --datasets ohlcv,mark,index,funding,oi \
  --timeframe 1m
```

Incremental update with overlap safety:

```bash
python -m bt.research_data.cli fetch-update \
  --market perp \
  --exchange binance
```

Incremental spot update with overlap safety:

```bash
python -m bt.research_data.cli fetch-update \
  --market spot \
  --exchange bybit
```

After the full bootstrap, omit `--all` to update the locally bootstrapped
stable plus volatile-seed universe across every raw dataset:
`ohlcv`, `mark`, `index`, `funding`, and `oi`. Use `--all` only when you
intentionally want to include every currently listed exchange instrument, even
new listings that were not part of the bootstrapped universe.

Inspect resumable fetch state:

```bash
python -m bt.research_data.cli fetch-status
```

Backfill the stable universe:

```bash
python -m bt.research_data.cli backfill-stable \
  --market perp \
  --exchange binance \
  --start 2021-01-01 \
  --end now \
  --timeframe 1m
```

Backfill the configured spot stable basket:

```bash
python -m bt.research_data.cli backfill-stable \
  --market spot \
  --exchange binance \
  --start 2021-01-01 \
  --end now \
  --timeframe 1m
```

Backfill selected symbols:

```bash
python -m bt.research_data.cli backfill \
  --exchange binance \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --start 2021-01-01 \
  --end now \
  --datasets ohlcv,mark,index,funding,oi \
  --timeframe 1m
```

Build the volatile historical top-movers universe:

```bash
python -m bt.research_data.cli build-volatile-universe \
  --market perp \
  --exchange binance \
  --start 2021-01-01 \
  --end now \
  --rebalance-freq 2h \
  --lookback 24h \
  --top-gainers 20 \
  --top-losers 10 \
  --min-age-days 30 \
  --min-median-dollar-volume-7d 5000000
```

Build a spot volatile historical top-movers universe from downloaded spot
OHLCV:

```bash
python -m bt.research_data.cli build-volatile-universe \
  --market spot \
  --exchange bybit \
  --start 2025-01-01 \
  --end now \
  --rebalance-freq 2h \
  --lookback 24h \
  --top-gainers 20 \
  --top-losers 10 \
  --min-age-days 30 \
  --min-median-dollar-volume-7d 5000000
```

Build canonical panels:

```bash
python -m bt.research_data.cli build-panel \
  --market perp \
  --exchange binance \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --timeframe 1m
```

Build spot OHLCV research panels:

```bash
python -m bt.research_data.cli build-panel \
  --market spot \
  --exchange binance \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --timeframe 1m
```

Parallel spot bootstrap commands for dedicated tmux sessions:

```bash
tmux new-session -d -s research_data_binance_spot \
  'cd /home/omenka/Projects/bulletproof_bt && PYTHONPATH=src python3 scripts/bootstrap_research_data_spot_fast.py --exchange binance --workers 6 --stable-start 2021-01-01 --volatile-start 2025-01-01 --end now 2>&1 | tee -a logs/research_data_binance_spot.log'

tmux new-session -d -s research_data_bybit_spot \
  'cd /home/omenka/Projects/bulletproof_bt && PYTHONPATH=src python3 scripts/bootstrap_research_data_spot_fast.py --exchange bybit --workers 6 --stable-start 2021-01-01 --volatile-start 2025-01-01 --end now 2>&1 | tee -a logs/research_data_bybit_spot.log'
```

Materialize the active volatile fast-path panel after rebuilding volatile
membership and symbol panels:

```bash
python -m bt.research_data.cli materialize-volatile-panel \
  --exchange binance \
  --timeframe 1m \
  --membership-path research_data/manifests/volatile_universe_membership.parquet \
  --start 2025-01-01 \
  --end now
```

Build compiled L7-H1 strategy-family feature columns after panel/materialized
panel refreshes:

```bash
python -m bt.research_data.cli build-l7h1-kernel-features \
  --exchange binance \
  --universe stable \
  --timeframe 1m \
  --signal-timeframes 15m,1h

python -m bt.research_data.cli build-l7h1-kernel-features \
  --exchange binance \
  --universe volatile-active \
  --timeframe 1m \
  --signal-timeframes 15m,1h
```

Validate coverage and causal source timestamps:

```bash
python -m bt.research_data.cli validate \
  --all
```

Build coverage manifest:

```bash
python -m bt.research_data.cli coverage --all
```

Generate the static coverage dashboard:

```bash
python -m bt.research_data.cli dashboard
```

Collect live liquidation events forward from now:

```bash
python -m bt.research_data.cli collect-liquidations \
  --exchange binance \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT
```

Aggregate collected liquidation events into 1m buckets:

```bash
python -m bt.research_data.cli aggregate-liquidations \
  --exchange binance \
  --timeframe 1m
```

## Stable Universe

The configured stable basket is 30 major Binance USDT perpetuals:

`BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `XRPUSDT`, `BNBUSDT`, `ADAUSDT`, `DOGEUSDT`, `AVAXUSDT`, `LINKUSDT`, `TRXUSDT`, `DOTUSDT`, `LTCUSDT`, `BCHUSDT`, `NEARUSDT`, `ATOMUSDT`, `APTUSDT`, `ARBUSDT`, `OPUSDT`, `FILUSDT`, `ETCUSDT`, `INJUSDT`, `SUIUSDT`, `AAVEUSDT`, `UNIUSDT`, `MKRUSDT`, `RNDRUSDT`, `SEIUSDT`, `POLUSDT`, `FETUSDT`, `TONUSDT`.

Aliases are resolved where applicable, such as `POLUSDT`/`MATICUSDT` and `FETUSDT`/`ASIUSDT`. Late-listed symbols are not treated as errors; their `first_seen_ts` is recorded in `stable_universe.parquet`.

## Native vs Canonical Symbols

`native_symbol` is the symbol required by an exchange API and used in storage paths:

- Binance: `BTCUSDT`
- Bybit: `BTCUSDT`
- OKX: `BTC-USDT-SWAP`

`canonical_symbol` is the cross-exchange research identity:

- Binance `BTCUSDT` -> `BTC-USDT-PERP`
- Bybit `BTCUSDT` -> `BTC-USDT-PERP`
- OKX `BTC-USDT-SWAP` -> `BTC-USDT-PERP`
- Binance spot `BTCUSDT` -> `BTC-USDT-SPOT`
- Bybit spot `BTCUSDT` -> `BTC-USDT-SPOT`

The instrument manifest at `research_data/manifests/instruments.parquet` stores:

```text
market
exchange
native_symbol
canonical_symbol
base_asset
quote_asset
settle_asset
contract_type
status
first_seen_ts
last_seen_ts
price_precision
qty_precision
```

Raw storage remains exchange/native-symbol based inside an explicit market
namespace:

```text
research_data/raw/<market>/<exchange>/<native_symbol>/<dataset>/timeframe=<timeframe>/data.parquet
```

Canonical panel rows include both `symbol` (the native symbol for backward compatibility) and `canonical_symbol`.

## Volatile Universe

The volatile universe is rebuilt historically from all available Binance USDT perpetual OHLCV data already present in the local raw library. At each rebalance timestamp it:

- considers only candles with `ts <= rebalance_ts`
- requires at least `min_age_days` of history
- requires positive price and fresh candles
- requires a 7d median dollar volume threshold
- computes the lookback return from data available at that time
- selects the configured top gainers and losers

This avoids using today's listed symbols or future candles to create past baskets.

For fast backtests, the active volatile rows can be materialized into:

```text
research_data/canonical/perp/<exchange>/_volatile_active/timeframe=1m/research_panel.parquet
```

This file is derived from `volatile_universe_membership.parquet`; it does not
rerank symbols. A symbol is active for bars where
`rebalance_ts <= bar_ts < next_rebalance_ts`. The materialized file stores only
those active rows, sorted by `ts, symbol`, and marks them with
`volatile_active=true` and `universe_active=true`. The research panel loader
automatically uses this file for volatile backtests when it exists, otherwise it
falls back to the slower membership-aware streaming path.

The bootstrap/update helper scripts refresh the materialized volatile panel
after rebuilding volatile membership and canonical panels. If you run the steps
manually, run `materialize-volatile-panel` after `build-volatile-universe` and
`build-panel`.

## Fetching Subsystem

The `bt.research_data.fetching` package provides resumable historical and incremental fetching:

- `fetch_state.parquet` stores one checkpoint per `market/exchange/symbol/dataset/timeframe`.
- Historical backfills run in chronological chunks and checkpoint after each successful chunk.
- Incremental updates overlap prior successful windows: 3 days for OHLCV, mark, index, and OI; 14 days for funding.
- Writes validate each fetched chunk before persistence, then use atomic parquet upserts with duplicate-key removal.
- `coverage.parquet` tracks expected rows, actual rows, missing rows, largest gap, first timestamp, and last timestamp.
- `logs/research_data_fetch.log` records structured JSON lines for each chunk, including retry count and row counts.

Binance 1m kline-style datasets are chunked at 1500 candles per request, about 25 hours. Funding and OI use endpoint-specific pagination windows while preserving exact source timestamps.

## Daily Validation

Daily validation writes:

```text
research_data/manifests/coverage.parquet
research_data/manifests/validation_report.parquet
research_data/reports/coverage_summary.html
```

Coverage tracks expected rows, actual rows, missing rows, duplicate rows, gap counts, largest gap minutes, first/last timestamps, and status per exchange/native symbol/dataset/timeframe.

Example cron schedule:

```cron
# update every 15 minutes
*/15 * * * * cd /path/to/bulletproof_bt && python -m bt.research_data.cli fetch-update --exchange binance
*/15 * * * * cd /path/to/bulletproof_bt && python -m bt.research_data.cli fetch-update --exchange bybit

# build panels hourly
5 * * * * cd /path/to/bulletproof_bt && python -m bt.research_data.cli build-panel --exchange binance --symbols BTCUSDT,ETHUSDT,SOLUSDT --timeframe 1m

# refresh volatile fast-path after weekly/monthly volatile universe rebuilds
20 3 * * 0 cd /path/to/bulletproof_bt && python -m bt.research_data.cli materialize-volatile-panel --exchange binance --timeframe 1m --membership-path research_data/manifests/volatile_universe_membership.parquet --start 2025-01-01 --end now

# validate daily
30 2 * * * cd /path/to/bulletproof_bt && python -m bt.research_data.cli validate --all

# dashboard daily
45 2 * * * cd /path/to/bulletproof_bt && python -m bt.research_data.cli dashboard
```

## Orchestration

Parallel hypothesis grids can consume this library directly:

```bash
python scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/tier2/l1_h7c_parallel_stable \
  --manifest outputs/tier2/l1_h7c_parallel_stable/manifests/l1_h7c_high_selectivity_regime_tier2_grid.csv \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data-root research_data \
  --data-kind research_panel \
  --exchange binance \
  --universe stable \
  --timeframe 1m \
  --max-workers 6 \
  --skip-completed
```

The old curated-folder `--data /home/omenka/research_data/bt/curated/...` mode
still works. See `docs/research_orchestration.md` for stable and volatile
examples.

## Live Liquidations

Liquidation data is forward-collected only. The subsystem does not attempt a historical liquidation backfill unless an exchange explicitly provides such an endpoint in a future adapter.

Raw events are append-only JSONL:

```text
research_data/raw/<exchange>/<native_symbol>/liquidations/timeframe=event/data.jsonl
```

Each raw event stores:

```text
ts
exchange
native_symbol
canonical_symbol
side
price
qty
notional
event_id
raw
```

Aggregated liquidation candles are written to:

```text
research_data/canonical/<exchange>/<native_symbol>/timeframe=1m/liquidation_1m.parquet
```

Panel building includes liquidation columns only when `liquidation_1m.parquet` exists for that exchange/native symbol. Missing liquidation history is therefore unavailable, not zero. Aggregation only summarizes observed collected events.
