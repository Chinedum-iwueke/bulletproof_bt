import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h5a_vol_managed_trend import L1H5AVolManagedTrendStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, side: Side | None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side.value.lower()}}
    return {"htf": {"15m": {"BTCUSDT": signal_bar}}, "positions": positions, "equity": 100000.0, "risk": {"r_per_trade": 0.01}}


def _entry(strategy: L1H5AVolManagedTrendStrategy) -> tuple[object, int]:
    for i in range(420):
        b = _bar(i * 15, 100.0 + i * 0.05, high=100.3 + i * 0.05, low=99.7 + i * 0.05)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, None))
    b = _bar(9000, 170.0, high=170.3, low=169.7)
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, None))
    return out[0], 9000


def test_entry_on_completed_signal_bar_only() -> None:
    strategy = L1H5AVolManagedTrendStrategy(timeframe="15m")
    entry, _ = _entry(strategy)
    assert entry.metadata["signal_timeframe"] == "15m"
    b = _bar(9001, 170.1)
    assert strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(_bar(9000, 170.0), None)) == []


def test_stop_monitoring_and_time_stop_semantics() -> None:
    strategy = L1H5AVolManagedTrendStrategy(timeframe="15m", T_hold=2)
    entry, minute = _entry(strategy)
    frozen = float(entry.metadata["stop_price"])

    hit = _bar(minute + 1, 170.0, low=frozen - 0.01, high=171.0)
    out_stop = strategy.on_bars(hit.ts, {"BTCUSDT": hit}, {"BTCUSDT"}, _ctx(hit, Side.BUY))
    assert out_stop and out_stop[0].metadata["exit_reason"] == "atr_stop"

    strategy2 = L1H5AVolManagedTrendStrategy(timeframe="15m", T_hold=2)
    _, minute2 = _entry(strategy2)
    sig1 = _bar(minute2 + 15, 172.0)
    assert strategy2.on_bars(sig1.ts, {"BTCUSDT": sig1}, {"BTCUSDT"}, _ctx(sig1, Side.BUY)) == []
    sig2 = _bar(minute2 + 30, 172.0)
    out_time = strategy2.on_bars(sig2.ts, {"BTCUSDT": sig2}, {"BTCUSDT"}, _ctx(sig2, Side.BUY))
    assert out_time and out_time[0].metadata["exit_reason"] == "time_stop"
    assert out_time[0].metadata["hold_time_unit"] == "signal_bars"
