"""Resolve and preflight research_data profiles for grid orchestration."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
import yaml

REQUIRED_PANEL_COLUMNS = {"ts", "symbol", "open", "high", "low", "close", "volume"}


@dataclass(frozen=True)
class ResearchDataProfile:
    universe: str
    data_kind: str = "research_panel"
    root: Path = Path("research_data")
    exchange: str = "binance"
    timeframe: str = "1m"
    stable_manifest: Path = Path("research_data/manifests/stable_universe.parquet")
    membership_path: Path = Path("research_data/manifests/volatile_universe_membership.parquet")

    def to_config(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "dataset_kind": self.data_kind,
            "root": str(self.root),
            "exchange": self.exchange,
            "universe": self.universe,
            "timeframe": self.timeframe,
        }
        if self.universe == "stable":
            data["stable_manifest"] = str(self.stable_manifest)
        if self.universe == "volatile":
            data["membership_path"] = str(self.membership_path)
        return {"data": data}


def resolve_data_profile(
    *,
    universe: str,
    data_root: str | Path = "research_data",
    data_kind: str = "research_panel",
    exchange: str = "binance",
    timeframe: str = "1m",
    stable_manifest: str | Path | None = None,
    membership_path: str | Path | None = None,
) -> ResearchDataProfile:
    root = Path(data_root)
    stable = Path(stable_manifest) if stable_manifest else root / "manifests" / "stable_universe.parquet"
    membership = Path(membership_path) if membership_path else root / "manifests" / "volatile_universe_membership.parquet"
    if data_kind != "research_panel":
        raise ValueError(f"unsupported data_kind for research data profile: {data_kind}")
    if universe not in {"stable", "volatile"}:
        raise ValueError(f"universe must be stable or volatile, got: {universe}")
    return ResearchDataProfile(
        universe=universe,
        data_kind=data_kind,
        root=root,
        exchange=exchange,
        timeframe=timeframe,
        stable_manifest=stable,
        membership_path=membership,
    )


def write_data_profile_config(profile: ResearchDataProfile, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(profile.to_config(), sort_keys=False), encoding="utf-8")
    return output_path


def preflight_research_data_profile(
    profile: ResearchDataProfile,
    *,
    symbols_subset: list[str] | None = None,
    max_symbols: int | None = None,
) -> None:
    if not profile.root.exists():
        raise FileNotFoundError(f"research_data root does not exist: {profile.root}")
    if profile.universe == "volatile":
        materialized_path = _volatile_materialized_panel_path(profile)
        if materialized_path.exists() and symbols_subset is None and max_symbols is None:
            _validate_panel_path(materialized_path, label=f"{profile.exchange}/_volatile_active")
            return
    if profile.universe == "stable":
        symbols = _stable_symbols(profile)
    else:
        symbols = _volatile_symbols(profile)
    symbols = _apply_symbol_scope(symbols, symbols_subset=symbols_subset, max_symbols=max_symbols)
    if not symbols:
        raise ValueError(f"No symbols resolved for research data profile: {profile.universe}")
    for symbol in symbols:
        _validate_panel_file(profile, symbol)


def _stable_symbols(profile: ResearchDataProfile) -> list[str]:
    if not profile.stable_manifest.exists():
        raise FileNotFoundError(f"stable universe manifest missing: {profile.stable_manifest}")
    stable = pd.read_parquet(profile.stable_manifest)
    if stable.empty:
        raise ValueError(f"stable universe manifest is empty: {profile.stable_manifest}")
    native_col = "native_symbol" if "native_symbol" in stable.columns else "symbol"
    filtered = stable[stable["exchange"].eq(profile.exchange)] if "exchange" in stable.columns else stable
    if "available" in filtered.columns:
        filtered = filtered[filtered["available"].astype(bool)]
    if "first_seen_ts" in filtered.columns:
        pd.to_datetime(filtered["first_seen_ts"], utc=True, errors="coerce")
    return filtered[native_col].astype(str).drop_duplicates().tolist()


def _volatile_symbols(profile: ResearchDataProfile) -> list[str]:
    if not profile.membership_path.exists():
        raise FileNotFoundError(f"volatile membership missing: {profile.membership_path}")
    membership = pd.read_parquet(profile.membership_path)
    if membership.empty:
        raise ValueError(f"volatile membership is empty: {profile.membership_path}")
    membership["ts"] = pd.to_datetime(membership["ts"], utc=True, errors="raise")
    filtered = membership[membership["exchange"].eq(profile.exchange)] if "exchange" in membership.columns else membership
    filtered = _dedupe_volatile_membership(filtered)
    return filtered["symbol"].astype(str).drop_duplicates().tolist()


def _validate_panel_file(profile: ResearchDataProfile, symbol: str) -> None:
    path = profile.root / "canonical" / profile.exchange / symbol / f"timeframe={profile.timeframe}" / "research_panel.parquet"
    if not path.exists():
        raise FileNotFoundError(f"research panel missing for {profile.exchange}/{symbol}: {path}")
    _validate_panel_path(path, label=f"{profile.exchange}/{symbol}")


def _volatile_materialized_panel_path(profile: ResearchDataProfile) -> Path:
    return (
        profile.root
        / "canonical"
        / profile.exchange
        / "_volatile_active"
        / f"timeframe={profile.timeframe}"
        / "research_panel.parquet"
    )


def _validate_panel_path(path: Path, *, label: str) -> None:
    available_columns = set(pq.ParquetFile(path).schema_arrow.names)
    missing = REQUIRED_PANEL_COLUMNS - available_columns
    if missing:
        raise ValueError(f"research panel missing required columns {sorted(missing)}: {path}")
    read_columns = sorted(REQUIRED_PANEL_COLUMNS | ({"funding_source_ts", "oi_source_ts"} & available_columns))
    df = pd.read_parquet(path, columns=read_columns)
    ts = pd.to_datetime(df["ts"], utc=True, errors="raise")
    if str(ts.dt.tz) != "UTC":
        raise ValueError(f"research panel timestamps must be UTC: {path}")
    for source_col in ("funding_source_ts", "oi_source_ts"):
        if source_col in df.columns:
            source = pd.to_datetime(df[source_col], utc=True, errors="coerce")
            mask = source.notna()
            if (source[mask] > ts[mask]).any():
                raise ValueError(f"{source_col} exceeds bar ts in {path}")


def _dedupe_volatile_membership(membership: pd.DataFrame) -> pd.DataFrame:
    if membership.empty:
        return membership
    out = membership.copy()
    out["symbol"] = out["symbol"].astype(str)
    if "score" in out.columns:
        out["_score"] = pd.to_numeric(out["score"], errors="coerce")
    else:
        out["_score"] = pd.Series(float("nan"), index=out.index, dtype="float64")
    out["_abs_score"] = out["_score"].abs().fillna(-1.0)
    rank_type = out["rank_type"].astype(str) if "rank_type" in out.columns else pd.Series("", index=out.index)
    out["_side_priority"] = 1
    out.loc[out["_score"].gt(0) & rank_type.eq("gainer"), "_side_priority"] = 0
    out.loc[out["_score"].lt(0) & rank_type.eq("loser"), "_side_priority"] = 0
    out.loc[out["_score"].eq(0) & rank_type.eq("gainer"), "_side_priority"] = 0
    out["_rank"] = pd.to_numeric(out["rank"], errors="coerce").fillna(1_000_000) if "rank" in out.columns else 1_000_000
    out = out.sort_values(
        ["ts", "symbol", "_abs_score", "_side_priority", "_rank"],
        ascending=[True, True, False, True, True],
        kind="mergesort",
    )
    return out.drop_duplicates(["ts", "symbol"], keep="first").drop(
        columns=["_score", "_abs_score", "_side_priority", "_rank"]
    )


def _apply_symbol_scope(
    symbols: list[str],
    *,
    symbols_subset: list[str] | None = None,
    max_symbols: int | None = None,
) -> list[str]:
    out = list(dict.fromkeys(symbols))
    if symbols_subset is not None:
        allowed = set(symbols_subset)
        out = [symbol for symbol in out if symbol in allowed]
    if max_symbols is not None and max_symbols > 0:
        out = out[:max_symbols]
    return out
