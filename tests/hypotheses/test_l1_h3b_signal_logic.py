import pandas as pd

from bt.core.types import Bar
from bt.data.resample import TimeframeResampler
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.strategy.l1_h3b_har_rv_gate_mean_reversion import L1H3BHarRVGateMeanReversionStrategy


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2023-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2, low=close - 0.2, close=close, volume=1000)


def _build(**kwargs: float) -> HTFContextStrategyAdapter:
    return HTFContextStrategyAdapter(
        inner=L1H3BHarRVGateMeanReversionStrategy(timeframe="5m", fit_window_days=30, **kwargs),
        resampler=TimeframeResampler(timeframes=["5m"], strict=True),
    )


def test_l1_h3b_waits_for_vwap_atr_and_har_readiness() -> None:
    s = _build(gate_quantile_low=1.0, z0=0.0)
    seen = []
    for i in range(3000):
        b = _bar(i, 100 + i * 0.01)
        seen.extend(s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"positions": {}}))
    assert seen == []


def test_l1_h3b_low_vol_gate_and_zvwap_emit_entry() -> None:
    s = _build(gate_quantile_low=1.0, z0=0.0)
    seen = []
    for i in range(50000):
        b = _bar(i, 100 + i * 0.0005)
        seen.extend(s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"positions": {}}))
    assert seen
    assert seen[-1].signal_type == "l1_h3b_har_rv_gate_mean_reversion"
    assert seen[-1].metadata["gate_pass"] is True
    assert seen[-1].metadata["RV_hat_t"] is not None
    assert seen[-1].metadata["session_vwap_t"] is not None


def test_l1_h3b_no_entry_when_gate_fails() -> None:
    s = _build(gate_quantile_low=0.0, z0=0.0)
    seen = []
    for i in range(50000):
        b = _bar(i, 100 + i * 0.0005)
        seen.extend(s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"positions": {}}))
    assert seen == []
