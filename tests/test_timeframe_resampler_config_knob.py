from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pytest

from bt.api import _build_engine
from bt.core.config_resolver import resolve_config
from bt.data.feed import HistoricalDataFeed
from bt.strategy.base import Strategy


@dataclass
class _CaptureStrategy(Strategy):
    emitted_htf: list[tuple[pd.Timestamp, str, str, int, int]] = field(default_factory=list)

    def on_bars(self, ts, bars_by_symbol, tradeable, ctx: Mapping[str, Any]):
        htf = ctx.get("htf", {}) if isinstance(ctx, Mapping) else {}
        if isinstance(htf, Mapping):
            for tf, by_symbol in htf.items():
                if not isinstance(by_symbol, Mapping):
                    continue
                for symbol, bar in by_symbol.items():
                    self.emitted_htf.append((bar.ts, str(tf), str(symbol), int(bar.n_bars), int(bar.expected_bars)))
        return []


def _bars_df(minutes: list[int]) -> pd.DataFrame:
    rows = []
    for minute in minutes:
        ts = pd.Timestamp("2025-01-01 00:00:00", tz="UTC") + pd.Timedelta(minutes=minute)
        rows.append(
            {
                "ts": ts,
                "symbol": "AAA",
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 1.0,
            }
        )
    return pd.DataFrame(rows)


def _run_with_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, cfg: dict[str, Any], bars_df: pd.DataFrame):
    capture = _CaptureStrategy()

    def _factory(name: str, seed: int = 42, **kwargs: Any):
        return capture

    monkeypatch.setattr("bt.strategy.make_strategy", _factory)

    resolved = resolve_config(cfg)
    engine = _build_engine(
        resolved,
        HistoricalDataFeed(bars_df),
        tmp_path,
    )
    engine.run()
    return capture.emitted_htf


def _base_config() -> dict[str, Any]:
    return {
        "strategy": {"name": "coinflip"},
        "risk": {"mode": "equity_pct", "r_per_trade": 0.005},
        "htf_resampler": {"timeframes": ["5m"], "strict": True},
    }


@pytest.mark.parametrize("timeframe", ["7m", "12m", "2h"])
def test_classic_engine_accepts_arbitrary_signal_duration(monkeypatch, tmp_path, timeframe):
    cfg = _base_config()
    cfg["htf_resampler"] = {"timeframes": [timeframe], "strict": True}
    emitted = _run_with_config(monkeypatch, tmp_path / timeframe, cfg, _bars_df(list(range(0, 241))))
    assert emitted
    assert all(item[1] == timeframe for item in emitted)


def test_default_preserves_behavior_when_timeframe_unset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bars_df = _bars_df(list(range(0, 16)))

    baseline = _run_with_config(monkeypatch, tmp_path / "baseline", _base_config(), bars_df)

    cfg_with_data_but_no_timeframe = _base_config()
    cfg_with_data_but_no_timeframe["data"] = {"mode": "dataframe"}
    comparison = _run_with_config(monkeypatch, tmp_path / "comparison", cfg_with_data_but_no_timeframe, bars_df)

    assert comparison == baseline


def test_timeframe_15m_alias_is_not_used_for_htf_context_when_driving_engine_timeframe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    minutes = [m for m in range(0, 31) if m != 20]
    bars_df = _bars_df(minutes)

    cfg = _base_config()
    cfg["data"] = {"mode": "dataframe", "engine_timeframe": "15m"}
    emitted = _run_with_config(monkeypatch, tmp_path / "tf15m", cfg, bars_df)

    assert emitted and all(item[1] == "5m" for item in emitted)


def test_research_panel_timeframe_does_not_override_signal_htf_resampler(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    bars_df = _bars_df(list(range(0, 16)))
    cfg = _base_config()
    cfg["data"] = {
        "mode": "dataframe",
        "dataset_kind": "research_panel",
        "timeframe": "1m",
        "engine_timeframe": None,
        "entry_timeframe": None,
        "exit_timeframe": "1m",
    }
    cfg["htf_resampler"] = {"timeframes": ["15m"], "strict": True}

    emitted = _run_with_config(monkeypatch, tmp_path / "research_panel", cfg, bars_df)

    assert emitted
    assert all(item[1] == "15m" for item in emitted)


def test_resolved_config_omits_legacy_htf_aliases_when_resampler_is_effective() -> None:
    cfg = _base_config()
    cfg["htf_timeframes"] = ["5m", "15m", "1h"]
    cfg["htf_strict"] = True
    cfg["htf_resampler"] = {"timeframes": ["1h"], "strict": True}

    resolved = resolve_config(cfg)

    assert resolved["htf_resampler"] == {"timeframes": ["1h"], "strict": True}
    assert "htf_timeframes" not in resolved
    assert "htf_strict" not in resolved


def test_invalid_timeframe_raises_valueerror(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bars_df = _bars_df(list(range(0, 6)))
    cfg = _base_config()
    cfg["data"] = {"mode": "dataframe", "timeframe": "banana"}

    with pytest.raises(ValueError, match=r"data\.engine_timeframe") as exc_info:
        _run_with_config(monkeypatch, tmp_path / "invalid", cfg, bars_df)

    assert "1m" in str(exc_info.value)
