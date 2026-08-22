"""Daily validation checks for research_data."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.research_data.jobs.coverage import build_coverage
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts

VALIDATION_COLUMNS = (
    "check_name",
    "exchange",
    "native_symbol",
    "dataset",
    "timeframe",
    "status",
    "message",
    "path",
    "checked_at",
)


def run_daily_validation(
    store: ResearchDataStore | None = None,
    exchange: str | None = None,
) -> pd.DataFrame:
    store = store or ResearchDataStore()
    rows: list[dict[str, object]] = []
    coverage = build_coverage(store, exchange)
    for path in _panel_paths(store.root, exchange):
        rows.extend(validate_panel(path, store))
    for path in _raw_paths(store.root, exchange):
        rows.extend(validate_raw_dataset(path))
    rows.extend(validate_volatile_membership(store))
    rows.extend(validate_stable_universe(store))
    for row in coverage.itertuples(index=False):
        if row.status != "ok":
            rows.append(
                _row(
                    "coverage",
                    row.exchange,
                    row.native_symbol,
                    row.dataset,
                    row.timeframe,
                    "warning",
                    f"missing={row.missing_rows} duplicates={row.duplicate_rows} gaps={row.gap_count}",
                    "",
                )
            )
    report = pd.DataFrame(rows, columns=VALIDATION_COLUMNS)
    store.write_atomic(report, store.manifest_path("validation_report"))
    return report


def validate_panel(path: Path, store: ResearchDataStore | None = None) -> list[dict[str, object]]:
    exchange, native_symbol, timeframe = _panel_identity(path)
    rows: list[dict[str, object]] = []
    try:
        df = pd.read_parquet(path)
        _validate_ts(df)
        _validate_no_duplicates(df)
        _validate_monotonic(df)
        _validate_ohlcv(df)
        _validate_causal_sources(df)
        _validate_mark_index(df)
        _validate_panel_vs_ohlcv(path, df, store)
        rows.append(_row("panel", exchange, native_symbol, "research_panel", timeframe, "ok", "", str(path)))
    except Exception as exc:
        rows.append(_row("panel", exchange, native_symbol, "research_panel", timeframe, "failed", str(exc), str(path)))
    return rows


def validate_raw_dataset(path: Path) -> list[dict[str, object]]:
    exchange, native_symbol, dataset, timeframe = _raw_identity(path)
    try:
        df = pd.read_parquet(path)
        _validate_ts(df)
        _validate_no_duplicates(df)
        _validate_monotonic(df)
        if dataset == "ohlcv":
            _validate_ohlcv(df)
        return [_row("raw_dataset", exchange, native_symbol, dataset, timeframe, "ok", "", str(path))]
    except Exception as exc:
        return [_row("raw_dataset", exchange, native_symbol, dataset, timeframe, "failed", str(exc), str(path))]


def validate_volatile_membership(store: ResearchDataStore) -> list[dict[str, object]]:
    path = store.manifest_path("volatile_universe_membership")
    if not path.exists():
        return []
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return [_row("volatile_membership", "", "", "manifest", "", "warning", "membership is empty", str(path))]
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        if df.duplicated(["exchange", "ts", "symbol"]).any():
            raise ValueError("duplicate exchange/ts/symbol membership rows")
        if not df.sort_values(["exchange", "symbol", "ts"])["ts"].reset_index(drop=True).notna().all():
            raise ValueError("membership contains null timestamps")
        # Future leakage guard: ranks must be attached to the rebalance timestamp,
        # not to a later synthetic timestamp. This catches malformed manifests;
        # the builder itself uses only <= rebalance_ts bars.
        if "rebalance_id" in df.columns and df.groupby("rebalance_id")["ts"].nunique().max() > 1:
            raise ValueError("rebalance_id spans multiple timestamps")
        return [_row("volatile_membership", "", "", "manifest", "", "ok", "", str(path))]
    except Exception as exc:
        return [_row("volatile_membership", "", "", "manifest", "", "failed", str(exc), str(path))]


def validate_stable_universe(store: ResearchDataStore) -> list[dict[str, object]]:
    path = store.manifest_path("stable_universe")
    if not path.exists():
        return []
    try:
        df = pd.read_parquet(path)
        if "available" in df.columns and "first_seen_ts" in df.columns:
            unavailable_with_seen = df["available"].eq(False) & pd.to_datetime(df["first_seen_ts"], utc=True, errors="coerce").notna()
            if unavailable_with_seen.any():
                raise ValueError("unavailable symbols must not have first_seen_ts")
        return [_row("stable_universe", "", "", "manifest", "", "ok", "missing symbols marked by availability", str(path))]
    except Exception as exc:
        return [_row("stable_universe", "", "", "manifest", "", "failed", str(exc), str(path))]


def _validate_ts(df: pd.DataFrame) -> None:
    if "ts" not in df.columns:
        raise ValueError("missing ts")
    ts = pd.to_datetime(df["ts"], utc=True, errors="raise")
    if not isinstance(ts.dtype, pd.DatetimeTZDtype) or str(ts.dt.tz) != "UTC":
        raise ValueError("timestamps must be UTC")


def _validate_no_duplicates(df: pd.DataFrame) -> None:
    key = ["ts"]
    if "exchange" in df.columns:
        key.insert(0, "exchange")
    if "symbol" in df.columns:
        key.insert(1, "symbol")
    elif "native_symbol" in df.columns:
        key.insert(1, "native_symbol")
    if df.duplicated(key).any():
        raise ValueError("duplicate timestamps")


def _validate_monotonic(df: pd.DataFrame) -> None:
    sort_cols = [col for col in ("exchange", "symbol", "native_symbol", "ts") if col in df.columns]
    if sort_cols and not df.sort_values(sort_cols)["ts"].reset_index(drop=True).equals(df["ts"].reset_index(drop=True)):
        grouped_cols = [col for col in ("exchange", "symbol", "native_symbol") if col in df.columns]
        if grouped_cols:
            for _, group in df.groupby(grouped_cols, sort=False):
                if not pd.to_datetime(group["ts"], utc=True).is_monotonic_increasing:
                    raise ValueError("timestamps are not monotonic")


def _validate_ohlcv(df: pd.DataFrame) -> None:
    required = {"open", "high", "low", "close", "volume"}
    if not required.issubset(df.columns):
        return
    if (df["high"] < df[["open", "close", "low"]].max(axis=1)).any():
        raise ValueError("OHLC high/low/open/close inconsistency")
    if (df["low"] > df[["open", "close", "high"]].min(axis=1)).any():
        raise ValueError("OHLC high/low/open/close inconsistency")
    if (df["volume"] < 0).any():
        raise ValueError("negative volume")


def _validate_causal_sources(df: pd.DataFrame) -> None:
    ts = pd.to_datetime(df["ts"], utc=True)
    for col in ("funding_source_ts", "oi_source_ts"):
        if col in df.columns:
            source = pd.to_datetime(df[col], utc=True, errors="coerce")
            mask = source.notna()
            if (source[mask] > ts[mask]).any():
                raise ValueError(f"{col} > ts")


def _validate_mark_index(df: pd.DataFrame) -> None:
    for col in ("mark_close", "index_close"):
        if col in df.columns and df[col].isna().any():
            raise ValueError(f"{col} contains missing candles")


def _validate_panel_vs_ohlcv(path: Path, panel: pd.DataFrame, store: ResearchDataStore | None) -> None:
    if store is None:
        return
    exchange, native_symbol, timeframe = _panel_identity(path)
    raw_path = store.raw_path(exchange, native_symbol, "ohlcv", timeframe)
    if raw_path.exists():
        ohlcv = pd.read_parquet(raw_path)
        if len(panel) != len(ohlcv):
            raise ValueError("panel row count does not match base OHLCV rows")


def _panel_paths(root: Path, exchange: str | None) -> list[Path]:
    exchange_glob = exchange or "*"
    paths = (root / "canonical").glob(f"{exchange_glob}/*/timeframe=*/research_panel.parquet")
    namespaced = (root / "canonical").glob(f"*/{exchange_glob}/*/timeframe=*/research_panel.parquet")
    return sorted(set(paths) | set(namespaced))


def _raw_paths(root: Path, exchange: str | None) -> list[Path]:
    exchange_glob = exchange or "*"
    paths = (root / "raw").glob(f"{exchange_glob}/*/*/timeframe=*/data.parquet")
    namespaced = (root / "raw").glob(f"*/{exchange_glob}/*/*/timeframe=*/data.parquet")
    return sorted(set(paths) | set(namespaced))


def _panel_identity(path: Path) -> tuple[str, str, str]:
    parts = path.parts
    idx = parts.index("canonical")
    offset = 2 if parts[idx + 1] in {"perp", "spot"} else 1
    timeframe = parts[idx + offset + 2].split("=", 1)[1]
    return parts[idx + offset], parts[idx + offset + 1], timeframe


def _raw_identity(path: Path) -> tuple[str, str, str, str]:
    parts = path.parts
    idx = parts.index("raw")
    offset = 2 if parts[idx + 1] in {"perp", "spot"} else 1
    timeframe = parts[idx + offset + 3].split("=", 1)[1]
    return parts[idx + offset], parts[idx + offset + 1], parts[idx + offset + 2], timeframe


def _row(
    check_name: str,
    exchange: str,
    native_symbol: str,
    dataset: str,
    timeframe: str,
    status: str,
    message: str,
    path: str,
) -> dict[str, object]:
    return {
        "check_name": check_name,
        "exchange": exchange,
        "native_symbol": native_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "status": status,
        "message": message,
        "path": path,
        "checked_at": utc_ts("now"),
    }
