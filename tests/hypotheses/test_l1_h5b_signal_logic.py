import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h5b_vol_managed_har_trend import L1H5BVolManagedHarTrendStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, side: str | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {"15m": {"BTCUSDT": signal_bar}}, "positions": positions, "equity": 100000.0, "risk": {"r_per_trade": 0.01}}


def _warm_ready(strategy: L1H5BVolManagedHarTrendStrategy) -> None:
    for i in range(7000):
        b = _bar(i * 15, 100.0 + i * 0.005, high=100.2 + i * 0.005, low=99.8 + i * 0.005)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))


def test_l1_h5b_uses_h3a_gate_and_emits_overlay_size() -> None:
    strategy = L1H5BVolManagedHarTrendStrategy(gate_quantile=0.0, fit_window_days=30, vol_window_hours=24)
    _warm_ready(strategy)
    b = _bar(70000, 300.0, high=300.5, low=299.5)
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))
    assert out
    md = out[0].metadata
    assert md["gate_pass"] is True
    assert md["trend_dir_t"] in (1, -1)
    assert md["RV_hat_t"] is not None
    assert 0.25 <= md["s_t"] <= 1.5


def test_l1_h5b_scaling_changes_size_only_not_entry_existence() -> None:
    s_lo = L1H5BVolManagedHarTrendStrategy(gate_quantile=0.0, fit_window_days=30, s_min=0.25, s_max=1.0)
    s_hi = L1H5BVolManagedHarTrendStrategy(gate_quantile=0.0, fit_window_days=30, s_min=0.5, s_max=1.5)
    _warm_ready(s_lo)
    _warm_ready(s_hi)
    b = _bar(71000, 305.0, high=305.5, low=304.5)

    out_lo = s_lo.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))
    out_hi = s_hi.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))

    assert bool(out_lo) == bool(out_hi)
    assert out_lo[0].side == out_hi[0].side
    assert out_lo[0].metadata["qty_final"] != out_hi[0].metadata["qty_final"]
