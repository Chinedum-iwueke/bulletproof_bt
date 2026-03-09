import json
from pathlib import Path

import pandas as pd

from bt.core.types import Bar
from bt.data.resample import TimeframeResampler
from bt.logging.jsonl import JsonlWriter
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.strategy.l1_h1_vol_floor_trend import L1H1VolFloorTrendStrategy


def test_signal_metadata_contains_required_l1_h1_fields(tmp_path: Path) -> None:
    strategy = HTFContextStrategyAdapter(
        inner=L1H1VolFloorTrendStrategy(timeframe="15m", theta_vol=0.0),
        resampler=TimeframeResampler(timeframes=["15m"], strict=True),
    )
    writer = JsonlWriter(tmp_path / "decisions.jsonl")
    for i in range(900):
        ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
        close = 100 + i
        bar = Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 1, low=close - 1, close=close, volume=1000)
        signals = strategy.on_bars(ts, {"BTCUSDT": bar}, {"BTCUSDT"}, {})
        for sig in signals:
            writer.write({"ts": ts, "symbol": "BTCUSDT", "signal": sig, "approved": False, "reason": "test"})
    writer.close()
    rows = [json.loads(line) for line in (tmp_path / "decisions.jsonl").read_text().splitlines() if line.strip()]
    assert rows
    meta = rows[-1]["signal"]["metadata"]
    for key in ["rv_t", "vol_pct_t", "gate_pass", "trend_dir_t", "stop_distance"]:
        assert key in meta
