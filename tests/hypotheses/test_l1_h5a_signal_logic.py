import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h5a_vol_managed_trend import L1H5AVolManagedTrendStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, side: str | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {"15m": {"BTCUSDT": signal_bar}}, "positions": positions, "equity": 100000.0, "risk": {"r_per_trade": 0.01}}


def _warm_ready(strategy: L1H5AVolManagedTrendStrategy) -> None:
    for i in range(420):
        b = _bar(i * 15, 100.0 + i * 0.05, high=100.3 + i * 0.05, low=99.7 + i * 0.05)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))


def test_entry_uses_l1_h1_trend_family_and_emits_size_overlay() -> None:
    strategy = L1H5AVolManagedTrendStrategy(timeframe="15m", vol_window_hours=24, sigma_reference_window_hours=72, s_min=0.25, s_max=1.5)
    _warm_ready(strategy)
    b = _bar(7000, 140.0, high=140.3, low=139.7)
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))
    assert out
    md = out[0].metadata
    assert md["trend_dir_t"] in (1, -1)
    assert md["size_factor_t"] is not None
    assert 0.25 <= md["size_factor_t"] <= 1.5


def test_scaling_changes_size_metadata_not_entry_condition() -> None:
    s_lo = L1H5AVolManagedTrendStrategy(timeframe="15m", s_min=0.25, s_max=1.0)
    s_hi = L1H5AVolManagedTrendStrategy(timeframe="15m", s_min=0.5, s_max=1.5)
    _warm_ready(s_lo)
    _warm_ready(s_hi)
    b = _bar(8000, 150.0, high=150.3, low=149.7)

    out_lo = s_lo.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))
    out_hi = s_hi.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))

    assert bool(out_lo) == bool(out_hi)
    assert out_lo[0].side == out_hi[0].side
    assert out_lo[0].metadata["qty_final"] != out_hi[0].metadata["qty_final"]
