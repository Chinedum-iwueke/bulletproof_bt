"""Parquet storage layout and atomic upserts."""
from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

import pandas as pd

from bt.data.parquet_io import ensure_pyarrow_parquet
from bt.research_data.config import RESEARCH_DATA_ROOT
from bt.research_data.schemas import SCHEMAS, normalize_frame

ensure_pyarrow_parquet()


class ResearchDataStore:
    """Local parquet library for raw, canonical, and manifest datasets."""

    def __init__(self, root: Path | str = RESEARCH_DATA_ROOT) -> None:
        self.root = Path(root)

    @property
    def manifests_dir(self) -> Path:
        return self.root / "manifests"

    def ensure_layout(self) -> None:
        for path in (
            self.root,
            self.manifests_dir,
            self.root / "raw",
            self.root / "canonical",
        ):
            path.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def write_lock(self):
        self.ensure_layout()
        lock_path = self.root / ".research_data_write.lock"
        with open(lock_path, "a", encoding="utf-8") as handle:
            try:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            except (ImportError, OSError):
                pass
            try:
                yield
            finally:
                try:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except (ImportError, OSError):
                    pass

    @staticmethod
    def _market(market: str | None) -> str:
        value = str(market or "perp").strip().lower()
        if value not in {"perp", "spot"}:
            raise ValueError("market must be one of: perp, spot")
        return value

    def raw_path(self, exchange: str, symbol: str, dataset: str, timeframe: str, market: str = "perp") -> Path:
        tf = timeframe if dataset not in {"funding", "oi", "liquidations"} else ("event" if dataset in {"funding", "liquidations"} else timeframe)
        return self.root / "raw" / self._market(market) / exchange / symbol / dataset / f"timeframe={tf}" / "data.parquet"

    def legacy_raw_path(self, exchange: str, symbol: str, dataset: str, timeframe: str) -> Path:
        tf = timeframe if dataset not in {"funding", "oi", "liquidations"} else ("event" if dataset in {"funding", "liquidations"} else timeframe)
        return self.root / "raw" / exchange / symbol / dataset / f"timeframe={tf}" / "data.parquet"

    def raw_dataset_dir(self, exchange: str, symbol: str, dataset: str, timeframe: str, market: str = "perp") -> Path:
        return self.raw_path(exchange, symbol, dataset, timeframe, market=market).parent

    def raw_chunks_dir(self, exchange: str, symbol: str, dataset: str, timeframe: str, market: str = "perp") -> Path:
        return self.raw_dataset_dir(exchange, symbol, dataset, timeframe, market=market) / "chunks"

    def raw_chunk_path(
        self,
        exchange: str,
        symbol: str,
        dataset: str,
        timeframe: str,
        start: object,
        end: object,
        market: str = "perp",
    ) -> Path:
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        if start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize("UTC")
        else:
            start_ts = start_ts.tz_convert("UTC")
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize("UTC")
        else:
            end_ts = end_ts.tz_convert("UTC")
        year = f"year={start_ts.year:04d}"
        month = f"month={start_ts.month:02d}"
        name = f"part-{start_ts.strftime('%Y%m%dT%H%M%S')}-{end_ts.strftime('%Y%m%dT%H%M%S')}.parquet"
        return self.raw_chunks_dir(exchange, symbol, dataset, timeframe, market=market) / year / month / name

    def raw_jsonl_path(self, exchange: str, symbol: str, dataset: str, timeframe: str = "event", market: str = "perp") -> Path:
        return self.root / "raw" / self._market(market) / exchange / symbol / dataset / f"timeframe={timeframe}" / "data.jsonl"

    def canonical_symbol_dir(self, exchange: str, symbol: str, timeframe: str, market: str = "perp") -> Path:
        return self.root / "canonical" / self._market(market) / exchange / symbol / f"timeframe={timeframe}"

    def legacy_canonical_symbol_dir(self, exchange: str, symbol: str, timeframe: str) -> Path:
        return self.root / "canonical" / exchange / symbol / f"timeframe={timeframe}"

    def canonical_path(self, exchange: str, symbol: str, timeframe: str, name: str, market: str = "perp") -> Path:
        return self.canonical_symbol_dir(exchange, symbol, timeframe, market=market) / f"{name}.parquet"

    def manifest_path(self, name: str) -> Path:
        return self.manifests_dir / f"{name}.parquet"

    def read(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            legacy = self._legacy_compatible_path(path)
            if legacy is not None and legacy.exists():
                path = legacy
        if path.name == "data.parquet":
            return self.read_raw_compatible(path)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def _legacy_compatible_path(self, path: Path) -> Path | None:
        try:
            rel = path.relative_to(self.root)
        except ValueError:
            return None
        parts = rel.parts
        if len(parts) < 4:
            return None
        if parts[0] == "raw" and parts[1] == "perp":
            return self.root / "raw" / Path(*parts[2:])
        if parts[0] == "canonical" and parts[1] == "perp":
            return self.root / "canonical" / Path(*parts[2:])
        return None

    def read_raw_compatible(self, path: Path) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        if path.exists():
            frames.append(pd.read_parquet(path))
        chunk_root = path.parent / "chunks"
        if chunk_root.exists():
            for chunk_path in sorted(chunk_root.glob("year=*/month=*/*.parquet")):
                frames.append(pd.read_parquet(chunk_path))
        if not frames:
            return pd.DataFrame()
        non_empty = [frame for frame in frames if not frame.empty]
        if not non_empty:
            return pd.DataFrame()
        combined = pd.concat(non_empty, ignore_index=True)
        key_cols = [col for col in ("exchange", "symbol", "ts") if col in combined.columns]
        if key_cols:
            combined = combined.drop_duplicates(subset=key_cols, keep="last")
        sort_cols = [col for col in ("exchange", "symbol", "ts") if col in combined.columns]
        if sort_cols:
            combined = combined.sort_values(sort_cols).reset_index(drop=True)
        return combined

    def write_atomic(self, df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp = Path(handle.name)
            df.to_parquet(handle.name, index=False)
            try:
                handle.flush()
                os.fsync(handle.fileno())
            except OSError:
                pass
        try:
            os.replace(tmp, path)
            try:
                dir_fd = os.open(path.parent, os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        finally:
            if tmp.exists():
                tmp.unlink()

    def upsert_parquet(
        self,
        path: Path,
        new_df: pd.DataFrame,
        key: Iterable[str],
        columns: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        """Read, concatenate, deduplicate by key, sort by timestamp, atomically write."""
        with self.write_lock():
            key_cols = list(key)
            incoming = normalize_frame(new_df, columns)
            existing = self.read(path)
            if not existing.empty:
                existing = normalize_frame(existing, columns)
            frames = [frame for frame in (existing, incoming) if not frame.empty]
            if frames:
                combined = pd.concat(frames, ignore_index=True)
            else:
                combined = incoming
            if combined.empty:
                self.write_atomic(combined, path)
                return combined
            missing_keys = [col for col in key_cols if col not in combined.columns]
            if missing_keys:
                raise ValueError(f"upsert missing key columns {missing_keys} for {path}")
            combined = combined.drop_duplicates(subset=key_cols, keep="last")
            sort_cols = [col for col in ("exchange", "symbol", "ts") if col in combined.columns]
            combined = combined.sort_values(sort_cols).reset_index(drop=True)
            self.write_atomic(combined, path)
            return combined

    def upsert_dataset(
        self,
        exchange: str,
        symbol: str,
        dataset: str,
        timeframe: str,
        df: pd.DataFrame,
        market: str = "perp",
    ) -> pd.DataFrame:
        schema = SCHEMAS[dataset]
        return self.upsert_parquet(
            self.raw_path(exchange, symbol, dataset, timeframe, market=market),
            df,
            key=schema.key,
            columns=schema.columns,
        )

    def write_dataset_chunk(
        self,
        exchange: str,
        symbol: str,
        dataset: str,
        timeframe: str,
        df: pd.DataFrame,
        start: object,
        end: object,
        market: str = "perp",
    ) -> pd.DataFrame:
        """Write one raw fetch chunk without rewriting the full historical file."""
        schema = SCHEMAS[dataset]
        incoming = normalize_frame(df, schema.columns)
        if incoming.empty:
            return incoming
        key_cols = list(schema.key)
        missing_keys = [col for col in key_cols if col not in incoming.columns]
        if missing_keys:
            raise ValueError(f"chunk write missing key columns {missing_keys}")
        path = self.raw_chunk_path(exchange, symbol, dataset, timeframe, start, end, market=market)
        existing = pd.read_parquet(path) if path.exists() else pd.DataFrame()
        if not existing.empty:
            existing = normalize_frame(existing, schema.columns)
        frames = [frame for frame in (existing, incoming) if not frame.empty]
        combined = pd.concat(frames, ignore_index=True) if frames else incoming
        combined = combined.drop_duplicates(subset=key_cols, keep="last")
        sort_cols = [col for col in ("exchange", "symbol", "ts") if col in combined.columns]
        combined = combined.sort_values(sort_cols).reset_index(drop=True)
        self.write_atomic(combined, path)
        return combined
