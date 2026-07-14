"""Precompute causal state features into canonical research panels."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from bt.engine.fast_path.l7_h1_kernel import L7H1KernelParams, build_l7_h1_feature_frame
from bt.engine.fast_path.feature_registry import (
    CausalityContract,
    FeatureSpec,
    GLOBAL_FEATURE_REGISTRY,
)
from bt.features.online_state import OnlineStateFeatureLayer
from bt.data.resample import normalize_timeframe
from bt.research_data.jobs.materialize import MATERIALIZED_VOLATILE_SYMBOL
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts


def build_panel_state_features(
    exchange: str,
    timeframe: str = "1m",
    *,
    symbols: Iterable[str] | None = None,
    universe: str | None = None,
    start: object | None = None,
    end: object | None = None,
    store: ResearchDataStore | None = None,
) -> pd.DataFrame:
    """Stamp causal ``entry_state_*`` columns into canonical research panels.

    The feature builder uses sorted per-symbol history and rolling/expanding
    past-only operations. The write is atomic per symbol, so interrupted runs can
    safely resume by rerunning the same command.
    """

    store = store or ResearchDataStore()
    resolved_symbols = _resolve_symbols(store, exchange=exchange, timeframe=timeframe, symbols=symbols, universe=universe)
    rows: list[dict[str, object]] = []
    for idx, symbol in enumerate(resolved_symbols, start=1):
        path = _existing_research_panel_path(store, exchange=exchange, symbol=symbol, timeframe=timeframe)
        if not path.exists():
            rows.append({"exchange": exchange, "symbol": symbol, "status": "missing_panel", "rows": 0, "path": str(path)})
            continue
        panel = pd.read_parquet(path)
        if panel.empty:
            rows.append({"exchange": exchange, "symbol": symbol, "status": "empty_panel", "rows": 0, "path": str(path)})
            continue
        enriched = _with_state_features(panel, exchange=exchange, symbol=symbol, timeframe=timeframe, start=start, end=end)
        store.write_atomic(enriched, path)
        rows.append({"exchange": exchange, "symbol": symbol, "status": "ok", "rows": len(enriched), "path": str(path)})
        print(f"{exchange} state features {idx}/{len(resolved_symbols)} {symbol} rows={len(enriched)}", flush=True)
    return pd.DataFrame(rows)


def build_l7_h1_kernel_features(
    exchange: str,
    timeframe: str = "1m",
    *,
    signal_timeframes: Iterable[str] = ("15m", "1h"),
    symbols: Iterable[str] | None = None,
    universe: str | None = None,
    start: object | None = None,
    end: object | None = None,
    store: ResearchDataStore | None = None,
) -> pd.DataFrame:
    """Stamp compiled L7-H1 family feature columns into research panels.

    These columns are causal decision-time features. They are optional: L7-H1
    falls back to its original Python feature path whenever a compiled feature
    column is unavailable for a decision timestamp.
    """

    store = store or ResearchDataStore()
    resolved_symbols = _resolve_symbols(store, exchange=exchange, timeframe=timeframe, symbols=symbols, universe=universe)
    rows: list[dict[str, object]] = []
    for idx, symbol in enumerate(resolved_symbols, start=1):
        path = _existing_research_panel_path(store, exchange=exchange, symbol=symbol, timeframe=timeframe)
        if not path.exists():
            rows.append({"exchange": exchange, "symbol": symbol, "status": "missing_panel", "rows": 0, "path": str(path)})
            continue
        panel = pd.read_parquet(path)
        if panel.empty:
            rows.append({"exchange": exchange, "symbol": symbol, "status": "empty_panel", "rows": 0, "path": str(path)})
            continue
        enriched = _with_l7_h1_kernel_features(panel, signal_timeframes=signal_timeframes, start=start, end=end)
        store.write_atomic(enriched, path)
        rows.append({"exchange": exchange, "symbol": symbol, "status": "ok", "rows": len(enriched), "path": str(path)})
        print(f"{exchange} l7_h1 kernel features {idx}/{len(resolved_symbols)} {symbol} rows={len(enriched)}", flush=True)
    return pd.DataFrame(rows)


def build_htf_context_features(
    exchange: str,
    timeframe: str = "1m",
    *,
    signal_timeframes: Iterable[str] = ("5m", "15m", "1h"),
    symbols: Iterable[str] | None = None,
    universe: str | None = None,
    start: object | None = None,
    end: object | None = None,
    store: ResearchDataStore | None = None,
) -> pd.DataFrame:
    """Stamp strict closed-HTF context columns into research panels."""

    store = store or ResearchDataStore()
    resolved_symbols = _resolve_symbols(store, exchange=exchange, timeframe=timeframe, symbols=symbols, universe=universe)
    rows: list[dict[str, object]] = []
    for idx, symbol in enumerate(resolved_symbols, start=1):
        path = _existing_research_panel_path(store, exchange=exchange, symbol=symbol, timeframe=timeframe)
        if not path.exists():
            rows.append({"exchange": exchange, "symbol": symbol, "status": "missing_panel", "rows": 0, "path": str(path)})
            continue
        panel = pd.read_parquet(path)
        if panel.empty:
            rows.append({"exchange": exchange, "symbol": symbol, "status": "empty_panel", "rows": 0, "path": str(path)})
            continue
        enriched = _with_htf_context_features(panel, signal_timeframes=signal_timeframes, start=start, end=end)
        store.write_atomic(enriched, path)
        rows.append({"exchange": exchange, "symbol": symbol, "status": "ok", "rows": len(enriched), "path": str(path)})
        print(f"{exchange} htf context features {idx}/{len(resolved_symbols)} {symbol} rows={len(enriched)}", flush=True)
    return pd.DataFrame(rows)


def build_registered_panel_features(
    exchange: str,
    timeframe: str = "1m",
    *,
    features: Iterable[str] = ("engine_state", "htf_context", "l7h1_csi_displacement"),
    feature_params: dict[str, dict[str, object]] | None = None,
    symbols: Iterable[str] | None = None,
    universe: str | None = None,
    start: object | None = None,
    end: object | None = None,
    store: ResearchDataStore | None = None,
) -> pd.DataFrame:
    """Build registered causal features into canonical research panels.

    This is the generic replacement for one-off feature stamping jobs. It keeps
    the same atomic per-symbol write behavior as the older commands while each
    feature declares its inputs, warmup, causality, hash, output dtype, and
    readiness/candidate columns through ``FeatureSpec``.
    """

    register_research_data_feature_builders()
    store = store or ResearchDataStore()
    resolved_symbols = _resolve_symbols(store, exchange=exchange, timeframe=timeframe, symbols=symbols, universe=universe)
    requested = tuple(str(feature) for feature in features)
    params = feature_params or {}
    rows: list[dict[str, object]] = []
    for idx, symbol in enumerate(resolved_symbols, start=1):
        path = _existing_research_panel_path(store, exchange=exchange, symbol=symbol, timeframe=timeframe)
        if not path.exists():
            rows.append({"exchange": exchange, "symbol": symbol, "status": "missing_panel", "rows": 0, "path": str(path)})
            continue
        panel = pd.read_parquet(path)
        if panel.empty:
            rows.append({"exchange": exchange, "symbol": symbol, "status": "empty_panel", "rows": 0, "path": str(path)})
            continue
        base = panel.copy()
        base["ts"] = pd.to_datetime(base["ts"], utc=True)
        if "symbol" not in base.columns:
            base["symbol"] = symbol
        base["symbol"] = base["symbol"].astype(str)
        base = base.drop_duplicates(["ts", "symbol"], keep="last").sort_values(["symbol", "ts"], kind="mergesort")
        feature_input = base
        if start is not None:
            feature_input = feature_input[feature_input["ts"].ge(utc_ts(start))]
        if end is not None:
            feature_input = feature_input[feature_input["ts"].lt(utc_ts(end))]
        merged = base
        for feature_name in requested:
            spec = GLOBAL_FEATURE_REGISTRY.get(feature_name)
            spec.validate_inputs(feature_input)
            features_df = spec.build(feature_input, params=params.get(feature_name, {}))
            if features_df.empty:
                continue
            key_cols = ["ts", "symbol"]
            new_cols = [col for col in features_df.columns if col not in key_cols]
            merged = merged.drop(columns=[col for col in new_cols if col in merged.columns], errors="ignore")
            merged = merged.merge(features_df[key_cols + new_cols], on=key_cols, how="left", validate="one_to_one")
            for col in spec.readiness_columns + spec.candidate_columns:
                if "*" in col:
                    continue
                if col in merged.columns:
                    merged[col] = merged[col].where(merged[col].notna(), False).astype(bool)
        merged = merged.sort_values("ts", kind="mergesort").reset_index(drop=True)
        store.write_atomic(merged, path)
        rows.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "status": "ok",
                "rows": len(merged),
                "features": ",".join(requested),
                "path": str(path),
            }
        )
        print(f"{exchange} registered features {idx}/{len(resolved_symbols)} {symbol} features={','.join(requested)} rows={len(merged)}", flush=True)
    return pd.DataFrame(rows)


def _resolve_symbols(
    store: ResearchDataStore,
    *,
    exchange: str,
    timeframe: str,
    symbols: Iterable[str] | None,
    universe: str | None,
) -> list[str]:
    if symbols is not None:
        return sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip() and not str(symbol).startswith("_")})
    normalized_universe = str(universe or "all").strip().lower()
    if normalized_universe == "stable":
        path = store.manifest_path("stable_universe")
        if not path.exists():
            raise FileNotFoundError(f"stable universe manifest missing: {path}")
        frame = pd.read_parquet(path)
        if "exchange" in frame.columns:
            frame = frame[frame["exchange"].astype(str).str.lower().eq(exchange.lower())]
        col = "native_symbol" if "native_symbol" in frame.columns else "symbol"
        return sorted(frame[col].dropna().astype(str).unique().tolist())
    if normalized_universe == "volatile-active":
        return [MATERIALIZED_VOLATILE_SYMBOL]
    if normalized_universe == "volatile":
        path = store.manifest_path("volatile_universe_membership")
        if not path.exists():
            raise FileNotFoundError(f"volatile universe membership missing: {path}")
        frame = pd.read_parquet(path, columns=["exchange", "symbol"])
        frame = frame[frame["exchange"].astype(str).str.lower().eq(exchange.lower())]
        return sorted(frame["symbol"].dropna().astype(str).unique().tolist())
    root = store.root / "canonical" / exchange
    if not root.exists():
        return []
    out = []
    for panel in sorted(root.glob(f"*/timeframe={timeframe}/research_panel.parquet")):
        symbol = panel.parent.parent.name
        if not symbol.startswith("_"):
            out.append(symbol)
    return out


def _existing_research_panel_path(
    store: ResearchDataStore,
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Path:
    """Return the active panel path across the perp namespace transition.

    New spot/perp-separated stores write under ``canonical/perp``. Older
    Binance backfills in this workspace still live under the legacy
    ``canonical/<exchange>`` path. Feature stamping is semantics-neutral, so it
    should update the panel that already exists instead of creating a parallel
    half-populated copy.
    """
    path = store.canonical_path(exchange, symbol, timeframe, "research_panel")
    if path.exists():
        return path
    legacy = store.legacy_canonical_symbol_dir(exchange, symbol, timeframe) / "research_panel.parquet"
    return legacy if legacy.exists() else path


def _with_state_features(
    panel: pd.DataFrame,
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
    start: object | None,
    end: object | None,
) -> pd.DataFrame:
    required = {"ts", "open", "high", "low", "close", "volume"}
    missing = required - set(panel.columns)
    if missing:
        raise ValueError(f"research panel for {exchange}/{symbol} missing columns: {sorted(missing)}")
    base = panel.drop(columns=[col for col in panel.columns if col.startswith("entry_state_")], errors="ignore").copy()
    base["ts"] = pd.to_datetime(base["ts"], utc=True)
    if "symbol" not in base.columns:
        base["symbol"] = symbol
    base["symbol"] = base["symbol"].astype(str)
    base = base.drop_duplicates(["ts", "symbol"], keep="last")
    base = base.sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)
    feature_input = base
    if start is not None:
        feature_input = feature_input[feature_input["ts"].ge(utc_ts(start))]
    if end is not None:
        feature_input = feature_input[feature_input["ts"].lt(utc_ts(end))]
    features = _build_online_state_features(feature_input)
    if features.empty:
        return base.sort_values("ts", kind="mergesort").reset_index(drop=True)
    feature_cols = ["ts", "symbol"] + [col for col in features.columns if col.startswith("entry_state_")]
    merged = base.merge(features[feature_cols], on=["ts", "symbol"], how="left", validate="one_to_one")
    return merged.sort_values("ts", kind="mergesort").reset_index(drop=True)


def _with_l7_h1_kernel_features(
    panel: pd.DataFrame,
    *,
    signal_timeframes: Iterable[str],
    start: object | None,
    end: object | None,
) -> pd.DataFrame:
    base = panel.drop(columns=[col for col in panel.columns if col.startswith("l7h1_")], errors="ignore").copy()
    base["ts"] = pd.to_datetime(base["ts"], utc=True)
    base = base.drop_duplicates(["ts", "symbol"], keep="last")
    output_sort = ["ts", "symbol"] if base["symbol"].astype(str).nunique() > 1 else ["symbol", "ts"]
    base = base.sort_values(output_sort, kind="mergesort").reset_index(drop=True)
    feature_base = base
    if start is not None:
        feature_base = feature_base[feature_base["ts"].ge(utc_ts(start))]
    if end is not None:
        feature_base = feature_base[feature_base["ts"].lt(utc_ts(end))]
    merged = base
    for signal_timeframe in signal_timeframes:
        features = build_l7_h1_feature_frame(
            feature_base,
            params=L7H1KernelParams(signal_timeframe=str(signal_timeframe)),
        )
        if features.empty:
            continue
        merged = merged.merge(features, on=["ts", "symbol"], how="left", validate="one_to_one")
        ready_cols = [col for col in features.columns if col.endswith("compiled_feature_ready")]
        for col in ready_cols:
            if col in merged.columns:
                merged[col] = merged[col].where(merged[col].notna(), False).astype(bool)
    return merged.sort_values(output_sort, kind="mergesort").reset_index(drop=True)


def _with_htf_context_features(
    panel: pd.DataFrame,
    *,
    signal_timeframes: Iterable[str],
    start: object | None,
    end: object | None,
) -> pd.DataFrame:
    base = panel.drop(columns=[col for col in panel.columns if col.startswith("htf_")], errors="ignore").copy()
    base["ts"] = pd.to_datetime(base["ts"], utc=True)
    base = base.drop_duplicates(["ts", "symbol"], keep="last")
    output_sort = ["ts", "symbol"] if base["symbol"].astype(str).nunique() > 1 else ["symbol", "ts"]
    base = base.sort_values(output_sort, kind="mergesort").reset_index(drop=True)
    feature_base = base
    if start is not None:
        feature_base = feature_base[feature_base["ts"].ge(utc_ts(start))]
    if end is not None:
        feature_base = feature_base[feature_base["ts"].lt(utc_ts(end))]
    merged = base
    for signal_timeframe in signal_timeframes:
        tf = normalize_timeframe(str(signal_timeframe), key_path="signal_timeframe")
        features = _build_htf_context_frame(feature_base, tf)
        if features.empty:
            continue
        merged = merged.merge(features, on=["ts", "symbol"], how="left", validate="one_to_one")
        ready_col = f"htf_{tf}_ready"
        if ready_col in merged.columns:
            merged[ready_col] = merged[ready_col].where(merged[ready_col].notna(), False).astype(bool)
    return merged.sort_values(output_sort, kind="mergesort").reset_index(drop=True)


def _timeframe_minutes(tf: str) -> int:
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 1440
    raise ValueError(f"unsupported timeframe: {tf}")


def _build_htf_context_frame(panel: pd.DataFrame, signal_timeframe: str) -> pd.DataFrame:
    required = {"ts", "symbol", "open", "high", "low", "close", "volume"}
    missing = required - set(panel.columns)
    if missing:
        raise ValueError(f"HTF context input missing required columns: {sorted(missing)}")
    tf = normalize_timeframe(signal_timeframe, key_path="signal_timeframe")
    minutes = _timeframe_minutes(tf)
    prefix = f"htf_{tf}_"
    frames: list[pd.DataFrame] = []
    work = panel.loc[:, ["ts", "symbol", "open", "high", "low", "close", "volume"]].copy()
    work["ts"] = pd.to_datetime(work["ts"], utc=True)
    work = work.sort_values(["symbol", "ts"], kind="mergesort")
    bucket_freq = "h" if tf == "1h" else f"{minutes}min"
    for symbol, group in work.groupby("symbol", sort=False):
        group = group.sort_values("ts", kind="mergesort").reset_index(drop=True)
        group["bucket_start"] = group["ts"].dt.floor(bucket_freq)
        agg = group.groupby("bucket_start", sort=True).agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            n_bars=("close", "size"),
        )
        agg = agg[agg["n_bars"].eq(minutes)].reset_index()
        if agg.empty:
            continue
        agg["ts"] = agg["bucket_start"] + pd.to_timedelta(minutes, unit="m")
        available_ts = set(pd.to_datetime(group["ts"], utc=True))
        agg = agg[agg["ts"].isin(available_ts)]
        if agg.empty:
            continue
        frames.append(
            pd.DataFrame(
                {
                    "ts": agg["ts"],
                    "symbol": str(symbol),
                    f"{prefix}ready": True,
                    f"{prefix}ts": agg["bucket_start"],
                    f"{prefix}open": agg["open"],
                    f"{prefix}high": agg["high"],
                    f"{prefix}low": agg["low"],
                    f"{prefix}close": agg["close"],
                    f"{prefix}volume": agg["volume"],
                    f"{prefix}n_bars": agg["n_bars"].astype("int64"),
                    f"{prefix}expected_bars": minutes,
                    f"{prefix}is_complete": True,
                }
            )
        )
    if not frames:
        return pd.DataFrame(columns=["ts", "symbol"])
    return pd.concat(frames, ignore_index=True).sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)


def _build_online_state_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Build exact engine-compatible state snapshots from historical rows."""
    extra_cols = [
        col
        for col in frame.columns
        if col
        not in {
            "ts",
            "exchange",
            "symbol",
            "canonical_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        }
        and not col.startswith("entry_state_")
    ]
    rows: list[dict[str, object]] = []
    for symbol, group in frame.groupby("symbol", sort=False):
        layer = OnlineStateFeatureLayer(enabled=True, profile="full")
        columns = ["ts", "open", "high", "low", "close", "volume"] + extra_cols
        for values in group.loc[:, columns].itertuples(index=False, name=None):
            ts, open_px, high, low, close, volume, *extra_values = values
            extra = {
                col: value
                for col, value in zip(extra_cols, extra_values)
                if value is not None and not pd.isna(value)
            }
            layer.update(
                symbol=str(symbol),
                ts=pd.Timestamp(ts),
                open_px=float(open_px),
                high=float(high),
                low=float(low),
                close=float(close),
                volume=float(volume),
                extra=extra,
            )
            snapshot = layer.snapshot(symbol=str(symbol))
            snapshot["ts"] = pd.Timestamp(ts)
            snapshot["symbol"] = str(symbol)
            rows.append(snapshot)
    return pd.DataFrame(rows)


def _registered_engine_state_builder(frame: pd.DataFrame, params: dict[str, object]) -> pd.DataFrame:
    return _build_online_state_features(frame)


def _registered_htf_context_builder(frame: pd.DataFrame, params: dict[str, object]) -> pd.DataFrame:
    signal_timeframes = params.get("signal_timeframes", ("5m", "15m", "1h"))
    if isinstance(signal_timeframes, str):
        signal_timeframes = [item.strip() for item in signal_timeframes.split(",") if item.strip()]
    merged = frame.loc[:, ["ts", "symbol"]].copy()
    for signal_timeframe in signal_timeframes:  # type: ignore[assignment]
        tf = normalize_timeframe(str(signal_timeframe), key_path="signal_timeframe")
        features = _build_htf_context_frame(frame, tf)
        if features.empty:
            continue
        new_cols = [col for col in features.columns if col not in {"ts", "symbol"}]
        merged = merged.drop(columns=[col for col in new_cols if col in merged.columns], errors="ignore")
        merged = merged.merge(features[["ts", "symbol"] + new_cols], on=["ts", "symbol"], how="left", validate="one_to_one")
    return merged


def _registered_l7h1_builder(frame: pd.DataFrame, params: dict[str, object]) -> pd.DataFrame:
    signal_timeframes = params.get("signal_timeframes", ("15m", "1h"))
    if isinstance(signal_timeframes, str):
        signal_timeframes = [item.strip() for item in signal_timeframes.split(",") if item.strip()]
    merged = frame.loc[:, ["ts", "symbol"]].copy()
    for signal_timeframe in signal_timeframes:  # type: ignore[assignment]
        features = build_l7_h1_feature_frame(
            frame,
            params=L7H1KernelParams(signal_timeframe=str(signal_timeframe)),
        )
        if features.empty:
            continue
        new_cols = [col for col in features.columns if col not in {"ts", "symbol"}]
        merged = merged.drop(columns=[col for col in new_cols if col in merged.columns], errors="ignore")
        merged = merged.merge(features[["ts", "symbol"] + new_cols], on=["ts", "symbol"], how="left", validate="one_to_one")
    return merged


def register_research_data_feature_builders() -> None:
    GLOBAL_FEATURE_REGISTRY.register(
        FeatureSpec(
            name="engine_state",
            required_inputs=("ts", "symbol", "open", "high", "low", "close", "volume"),
            output_columns=("entry_state_*",),
            output_dtype="mixed",
            warmup_bars=1,
            version="2",
            causality=CausalityContract(
                mode="online_state_layer_past_only",
                description="Replays the engine online state layer per symbol in timestamp order.",
            ),
            builder=_registered_engine_state_builder,
            metadata={"column_prefixes": ["entry_state_"]},
        ),
        replace=True,
    )
    GLOBAL_FEATURE_REGISTRY.register(
        FeatureSpec(
            name="htf_context",
            required_inputs=("ts", "symbol", "open", "high", "low", "close", "volume"),
            output_columns=("htf_*",),
            output_dtype="mixed",
            warmup_bars=1,
            version="2",
            causality=CausalityContract(
                mode="strict_closed_htf",
                description="Emits context only after complete higher-timeframe candles close.",
            ),
            readiness_columns=("htf_*_ready",),
            candidate_columns=("htf_*_ready",),
            builder=_registered_htf_context_builder,
            metadata={"column_prefixes": ["htf_"]},
        ),
        replace=True,
    )
    GLOBAL_FEATURE_REGISTRY.register(
        FeatureSpec(
            name="l7h1_csi_displacement",
            required_inputs=("ts", "symbol", "open", "high", "low", "close", "volume"),
            output_columns=("l7h1_*",),
            output_dtype="mixed",
            warmup_bars=15,
            version="2",
            causality=CausalityContract(
                mode="strict_closed_signal_timeframe",
                description="Emits L7-H1 decision features at base-bar decision time from closed signal bars.",
            ),
            readiness_columns=("l7h1_*_compiled_feature_ready",),
            candidate_columns=("l7h1_*_compiled_feature_ready",),
            builder=_registered_l7h1_builder,
            metadata={"column_prefixes": ["l7h1_"]},
        ),
        replace=True,
    )


register_research_data_feature_builders()


__all__ = [
    "build_htf_context_features",
    "build_l7_h1_kernel_features",
    "build_panel_state_features",
    "build_registered_panel_features",
    "register_research_data_feature_builders",
]
