from __future__ import annotations

import csv
import difflib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from bt.api import run_backtest

_REQUIRED_ARTIFACTS = (
    "config_used.yaml",
    "performance.json",
    "equity.csv",
    "trades.csv",
    "run_status.json",
)

_OPTIONAL_BENCHMARK_ARTIFACTS = (
    "benchmark_equity.csv",
    "benchmark_metrics.json",
    "comparison_summary.json",
)


def _manifest(dataset_dir: Path, symbols: list[str]) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    payload = {"format": "per_symbol_parquet", "symbols": symbols, "path": "symbols/{symbol}.parquet"}
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "symbol"])
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path)


def _gen_rows(symbol: str, bars: int, start_ts: pd.Timestamp, base: float, step: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i in range(bars):
        ts = start_ts + pd.Timedelta(minutes=i)
        open_px = base + i * step
        close_px = open_px + (0.08 if i % 2 == 0 else -0.03)
        high_px = max(open_px, close_px) + 0.05
        low_px = min(open_px, close_px) - 0.05
        rows.append(
            {
                "ts": ts,
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": float(i + 1),
                "symbol": symbol,
            }
        )
    return rows


def _gen_equity_session_rows(symbol: str, bars: int, include_out_of_session: bool) -> list[dict[str, Any]]:
    # 14:30-21:00 UTC maps to 09:30-16:00 America/New_York during winter.
    start = pd.Timestamp("2024-01-02T14:30:00Z")
    rows = _gen_rows(symbol=symbol, bars=bars, start_ts=start, base=150.0, step=0.2)
    if include_out_of_session:
        rows.insert(
            0,
            {
                "ts": pd.Timestamp("2024-01-02T13:00:00Z"),  # 08:00 NY (outside regular session)
                "open": 149.0,
                "high": 149.1,
                "low": 148.9,
                "close": 149.05,
                "volume": 1.0,
                "symbol": symbol,
            },
        )
    return rows


def _run_case(config: dict[str, Any], dataset_dir: Path, out_dir: Path, run_name: str) -> Path:
    cfg_path = out_dir / f"{run_name}.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    run_dir = Path(
        run_backtest(
            config_path=str(cfg_path),
            data_path=str(dataset_dir),
            out_dir=str(out_dir),
            run_name=run_name,
        )
    )
    return run_dir


def _iso_utc(value: str) -> str:
    return pd.Timestamp(value).tz_convert("UTC").isoformat()


def _round_or_none(value: str | None, digits: int = 12) -> float | None:
    if value in (None, ""):
        return None
    return round(float(value), digits)


def _normalize_scalar(value: str | None) -> object:
    if value in (None, ""):
        return None
    try:
        return _round_or_none(value)
    except ValueError:
        return value


def _normalized_equity(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {"ts": _iso_utc(row["ts"])}
        for key in row:
            if key == "ts":
                continue
            item[key] = _round_or_none(row.get(key))
        normalized.append(item)
    return normalized


def _normalized_trades(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            if key in {"run_id", "identity_run_id", "identity_trade_id"}:
                continue
            if key.endswith("_ts") and value:
                item[key] = _iso_utc(value)
            elif key in {"symbol", "side", "run_id"}:
                item[key] = value
            else:
                item[key] = _normalize_scalar(value)
        normalized.append(item)
    normalized.sort(key=lambda r: (r.get("entry_ts") or "", r.get("exit_ts") or "", r.get("symbol") or ""))
    return normalized


def _canonicalize_json(value: Any) -> Any:
    if isinstance(value, dict):
        ignore = {"run_id", "created_at", "created_at_utc", "generated_at", "timestamp", "duration_seconds"}
        return {k: _canonicalize_json(v) for k, v in sorted(value.items()) if k not in ignore}
    if isinstance(value, list):
        return [_canonicalize_json(v) for v in value]
    if isinstance(value, float):
        return round(value, 12)
    return value


def _normalized_json(path: Path) -> str:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return json.dumps(_canonicalize_json(payload), sort_keys=True, separators=(",", ":"))


def _normalized_summary(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Generated:") or "elapsed" in stripped.lower() or "seconds" in stripped.lower():
            continue
        lines.append(stripped)
    return lines


def _assert_equal_with_diff(label: str, left: Any, right: Any) -> None:
    if left == right:
        return
    left_text = json.dumps(left, sort_keys=True, indent=2) if not isinstance(left, str) else left
    right_text = json.dumps(right, sort_keys=True, indent=2) if not isinstance(right, str) else right
    diff = "\n".join(
        difflib.unified_diff(
            left_text.splitlines(),
            right_text.splitlines(),
            fromfile=f"{label}:run_a",
            tofile=f"{label}:run_b",
            lineterm="",
        )
    )
    pytest.fail(f"{label} mismatch after normalization.\n{diff}")


def _required_artifacts(case_name: str) -> tuple[str, ...]:
    if case_name == "crypto":
        return _REQUIRED_ARTIFACTS + _OPTIONAL_BENCHMARK_ARTIFACTS
    return _REQUIRED_ARTIFACTS


def _normalized_bundle(run_dir: Path, case_name: str) -> dict[str, Any]:
    bundle: dict[str, Any] = {
        "equity": _normalized_equity(run_dir / "equity.csv"),
        "trades": _normalized_trades(run_dir / "trades.csv"),
        "performance": _normalized_json(run_dir / "performance.json"),
        "run_status": _normalized_json(run_dir / "run_status.json"),
        "summary": _normalized_summary(run_dir / "summary.txt"),
        "config_used": (run_dir / "config_used.yaml").read_text(encoding="utf-8"),
    }
    if case_name == "crypto":
        bundle["benchmark_equity"] = _normalized_equity(run_dir / "benchmark_equity.csv")
        bundle["benchmark_metrics"] = _normalized_json(run_dir / "benchmark_metrics.json")
        bundle["comparison_summary"] = _normalized_json(run_dir / "comparison_summary.json")
    return bundle


def _assert_required_artifacts(run_dir: Path, case_name: str) -> None:
    for filename in _required_artifacts(case_name):
        assert (run_dir / filename).exists(), f"missing required artifact: {run_dir / filename}"


def _save_bundle_as_fixture(bundle: dict[str, Any], fixture_case_dir: Path) -> None:
    if fixture_case_dir.exists():
        shutil.rmtree(fixture_case_dir)
    fixture_case_dir.mkdir(parents=True, exist_ok=True)

    (fixture_case_dir / "config_used.yaml").write_text(str(bundle["config_used"]), encoding="utf-8")
    (fixture_case_dir / "performance.json").write_text(str(bundle["performance"]), encoding="utf-8")
    (fixture_case_dir / "run_status.json").write_text(str(bundle["run_status"]), encoding="utf-8")
    if bundle["summary"]:
        (fixture_case_dir / "summary.txt").write_text("\n".join(bundle["summary"]) + "\n", encoding="utf-8")

    pd.DataFrame(bundle["equity"]).to_csv(fixture_case_dir / "equity.csv", index=False)
    pd.DataFrame(bundle["trades"]).to_csv(fixture_case_dir / "trades.csv", index=False)

    if "benchmark_equity" in bundle:
        pd.DataFrame(bundle["benchmark_equity"]).to_csv(fixture_case_dir / "benchmark_equity.csv", index=False)
        (fixture_case_dir / "benchmark_metrics.json").write_text(str(bundle["benchmark_metrics"]), encoding="utf-8")
        (fixture_case_dir / "comparison_summary.json").write_text(str(bundle["comparison_summary"]), encoding="utf-8")


def _load_fixture_bundle(case_name: str, fixture_root: Path) -> dict[str, Any] | None:
    case_dir = fixture_root / case_name
    if not case_dir.exists():
        return None
    required = _required_artifacts(case_name)
    if not all((case_dir / name).exists() for name in required):
        return None
    return _normalized_bundle(case_dir, case_name)


def _run_twice_and_assert(case_name: str, config: dict[str, Any], dataset_dir: Path, tmp_path: Path) -> None:
    run_a = _run_case(config=config, dataset_dir=dataset_dir, out_dir=tmp_path / f"{case_name}_a", run_name=f"{case_name}-a")
    run_b = _run_case(config=config, dataset_dir=dataset_dir, out_dir=tmp_path / f"{case_name}_b", run_name=f"{case_name}-b")

    _assert_required_artifacts(run_a, case_name)
    _assert_required_artifacts(run_b, case_name)

    bundle_a = _normalized_bundle(run_a, case_name)
    bundle_b = _normalized_bundle(run_b, case_name)

    for key in sorted(bundle_a.keys()):
        _assert_equal_with_diff(f"{case_name}:{key}", bundle_a[key], bundle_b[key])

    fixture_root = Path("tests/fixtures/regression_runs")
    fixture_bundle = _load_fixture_bundle(case_name, fixture_root)
    if fixture_bundle is not None:
        for key in sorted(bundle_a.keys()):
            _assert_equal_with_diff(f"{case_name}:fixture:{key}", bundle_a[key], fixture_bundle[key])

    if os.getenv("UPDATE_V1_FIXTURES") == "1":
        _save_bundle_as_fixture(bundle_a, fixture_root / case_name)


def _crypto_config() -> dict[str, Any]:
    return {
        "initial_cash": 10_000.0,
        "max_leverage": 2.0,
        "risk": {
            "mode": "r_fixed",
            "r_per_trade": 0.005,
            "max_positions": 1,
        },
        "strategy": {"name": "coinflip", "seed": 7, "p_trade": 1.0, "cooldown_bars": 0, "max_hold_bars": 3},
        "data": {"mode": "streaming", "symbols_subset": ["BTCUSDT"]},
        "execution": {"profile": "tier2"},
        "benchmark": {"enabled": True, "symbol": "BTCUSDT", "price_field": "close", "initial_equity": 10_000.0},
    }


def _fx_config() -> dict[str, Any]:
    return {
        "initial_cash": 20_000.0,
        "max_leverage": 10.0,
        "risk": {
            "mode": "r_fixed",
            "r_per_trade": 0.005,
            "max_positions": 1,
            "fx": {"lot_step": 0.01},
            "margin": {"leverage": 20.0},
        },
        "instrument": {
            "type": "forex",
            "symbol": "EURUSD",
            "tick_size": 0.0001,
            "pip_size": 0.0001,
            "contract_size": 100000,
            "account_currency": "USD",
            "quote_currency": "USD",
        },
        "strategy": {"name": "coinflip", "seed": 9, "p_trade": 1.0, "cooldown_bars": 0, "max_hold_bars": 4},
        "data": {"mode": "streaming", "symbols_subset": ["EURUSD"]},
        "execution": {
            "profile": "tier2",
            "spread_mode": "fixed_pips",
            "spread_pips": 1.2,
            "commission": {"mode": "per_lot", "per_lot": 2.5},
        },
        "benchmark": {"enabled": False},
    }


def _equity_config() -> dict[str, Any]:
    return {
        "initial_cash": 100_000.0,
        "max_leverage": 1.0,
        "risk": {
            "mode": "r_fixed",
            "r_per_trade": 0.001,
            "max_positions": 1,
            "margin": {"leverage": 1.0},
        },
        "instrument": {
            "type": "equity",
            "symbol": "AAPL",
            "tick_size": 0.01,
        },
        "strategy": {"name": "coinflip", "seed": 13, "p_trade": 1.0, "cooldown_bars": 0, "max_hold_bars": 4},
        "data": {
            "mode": "streaming",
            "symbols_subset": ["AAPL"],
            "market": "equity_session",
            "equity_session": {
                "timezone": "America/New_York",
                "open_time": "09:30",
                "close_time": "16:00",
                "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
            },
        },
        "execution": {
            "profile": "tier2",
            "commission": {"mode": "per_share", "per_share": 0.005},
        },
        "benchmark": {"enabled": False},
    }


def test_v1_freeze_crypto_deterministic(tmp_path: Path) -> None:
    dataset = tmp_path / "crypto_dataset"
    _manifest(dataset, ["BTCUSDT"])
    _write_parquet(
        dataset / "symbols" / "BTCUSDT.parquet",
        _gen_rows("BTCUSDT", bars=32, start_ts=pd.Timestamp("2024-01-01T00:00:00Z"), base=30_000.0, step=3.0),
    )

    _run_twice_and_assert("crypto", _crypto_config(), dataset, tmp_path)


def test_v1_freeze_fx_deterministic(tmp_path: Path) -> None:
    dataset = tmp_path / "fx_dataset"
    _manifest(dataset, ["EURUSD"])
    _write_parquet(
        dataset / "symbols" / "EURUSD.parquet",
        _gen_rows("EURUSD", bars=36, start_ts=pd.Timestamp("2024-01-01T00:00:00Z"), base=1.08, step=0.0004),
    )

    _run_twice_and_assert("fx", _fx_config(), dataset, tmp_path)


def test_v1_freeze_equity_deterministic(tmp_path: Path) -> None:
    # Verify the implemented enforcement path for out-of-session data.
    bad_dataset = tmp_path / "equity_bad_dataset"
    _manifest(bad_dataset, ["AAPL"])
    _write_parquet(
        bad_dataset / "symbols" / "AAPL.parquet",
        _gen_equity_session_rows("AAPL", bars=20, include_out_of_session=True),
    )

    with pytest.raises(ValueError, match=r"equity_session trading hours|equity_session trading day"):
        _run_case(_equity_config(), bad_dataset, tmp_path / "equity_bad_out", "equity-out-session")

    dataset = tmp_path / "equity_dataset"
    _manifest(dataset, ["AAPL"])
    _write_parquet(dataset / "symbols" / "AAPL.parquet", _gen_equity_session_rows("AAPL", bars=30, include_out_of_session=False))

    _run_twice_and_assert("equity", _equity_config(), dataset, tmp_path)


def test_docs_freeze_doc_exists_and_mentions_key_contracts() -> None:
    path = Path("docs/fx_traditional_v1_freeze.md")
    assert path.exists(), "Expected docs/fx_traditional_v1_freeze.md"
    text = path.read_text(encoding="utf-8")

    assert "FX Currency Assumption" in text
    assert "Execution Pack / Profile Interaction Rules" in text
    assert "Margin / Leverage / Liquidation Policy" in text
    assert "Portfolio Scope Limitation" in text
