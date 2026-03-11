"""HAR-style realised-volatility feature and deterministic OLS forecast helpers."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from math import log
from typing import Deque

import numpy as np
import pandas as pd


_BARS_PER_DAY = {
    "5m": 288,
    "15m": 96,
    "1h": 24,
}


@dataclass(frozen=True)
class HarFitRecord:
    fit_ts: pd.Timestamp
    fit_window_days: int
    train_start_ts: pd.Timestamp
    train_end_ts: pd.Timestamp
    n_obs: int
    a: float
    b: float
    c: float
    d: float
    feature_definition: str = "RV_hat = a + b*RV_d + c*RV_w + d*RV_m"


@dataclass(frozen=True)
class HarObservation:
    ts: pd.Timestamp
    rv1: float
    rv_d: float | None
    rv_w: float | None
    rv_m: float | None


def bars_per_day(timeframe: str) -> int:
    try:
        return _BARS_PER_DAY[timeframe]
    except KeyError as exc:
        raise ValueError(f"Unsupported timeframe '{timeframe}'. Expected one of {sorted(_BARS_PER_DAY)}") from exc


def rv1_from_close(prev_close: float | None, close: float) -> float | None:
    if prev_close is None or prev_close <= 0.0 or close <= 0.0:
        return None
    ret = log(close / prev_close)
    return float(ret * ret)


def deterministic_ols_coefficients(rows: list[HarObservation]) -> tuple[float, float, float, float] | None:
    eligible = [row for row in rows if row.rv_d is not None and row.rv_w is not None and row.rv_m is not None]
    if len(eligible) < 10:
        return None
    x = np.array([[1.0, float(row.rv_d), float(row.rv_w), float(row.rv_m)] for row in eligible], dtype=float)
    y = np.array([float(row.rv1) for row in eligible], dtype=float)
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    return (float(beta[0]), float(beta[1]), float(beta[2]), float(beta[3]))


class HarRVForecaster:
    """Causal HAR RV forecaster with daily refit cadence and rolling fit windows."""

    def __init__(self, *, timeframe: str, fit_window_days: int, refit_cadence: str = "daily_on_completed_signal_day") -> None:
        self.timeframe = timeframe
        self.fit_window_days = int(fit_window_days)
        if self.fit_window_days <= 0:
            raise ValueError("fit_window_days must be > 0")
        if refit_cadence != "daily_on_completed_signal_day":
            raise ValueError("Only daily_on_completed_signal_day is supported for HAR hypotheses")
        self.refit_cadence = refit_cadence

        self._bars_per_day = bars_per_day(timeframe)
        self._rv_d_window = self._bars_per_day
        self._rv_w_window = 7 * self._bars_per_day
        self._rv_m_window = 30 * self._bars_per_day

        self._rv_history: Deque[float] = deque(maxlen=self._rv_m_window)
        self._observations: list[HarObservation] = []
        self._coefficients: tuple[float, float, float, float] | None = None
        self._fit_history: list[HarFitRecord] = []
        self._forecast_history: list[tuple[pd.Timestamp, float]] = []

        self._prev_close: float | None = None
        self._last_fit_day: pd.Timestamp | None = None

    @property
    def fit_history(self) -> tuple[HarFitRecord, ...]:
        return tuple(self._fit_history)

    @property
    def forecast_history(self) -> tuple[tuple[pd.Timestamp, float], ...]:
        return tuple(self._forecast_history)

    @property
    def bars_per_day(self) -> int:
        return self._bars_per_day

    @property
    def warmup_bars_rv_m(self) -> int:
        return self._rv_m_window

    def _rolling_mean(self, window: int) -> float | None:
        if len(self._rv_history) < window:
            return None
        values = list(self._rv_history)
        return float(np.mean(values[-window:]))

    def _refit_if_due(self, ts: pd.Timestamp) -> None:
        day = ts.normalize()
        if self._last_fit_day is not None and day == self._last_fit_day:
            return
        train_end_ts = ts
        train_start_ts = ts - pd.Timedelta(days=self.fit_window_days)
        train_rows = [
            row
            for row in self._observations
            if row.ts < train_end_ts and row.ts >= train_start_ts and row.rv_d is not None and row.rv_w is not None and row.rv_m is not None
        ]
        coeffs = deterministic_ols_coefficients(train_rows)
        self._last_fit_day = day
        if coeffs is None:
            return
        self._coefficients = coeffs
        self._fit_history.append(
            HarFitRecord(
                fit_ts=ts,
                fit_window_days=self.fit_window_days,
                train_start_ts=train_start_ts,
                train_end_ts=train_end_ts,
                n_obs=len(train_rows),
                a=coeffs[0],
                b=coeffs[1],
                c=coeffs[2],
                d=coeffs[3],
            )
        )

    def update(self, ts: pd.Timestamp, close: float) -> dict[str, float | str | None]:
        rv1 = rv1_from_close(self._prev_close, close)
        self._prev_close = float(close)
        if rv1 is None:
            return {"rv1_t": None, "rv_d": None, "rv_w": None, "rv_m": None, "rv_hat_t": None, "fit_ts_used": None}

        self._rv_history.append(rv1)
        rv_d = self._rolling_mean(self._rv_d_window)
        rv_w = self._rolling_mean(self._rv_w_window)
        rv_m = self._rolling_mean(self._rv_m_window)

        obs = HarObservation(ts=ts, rv1=rv1, rv_d=rv_d, rv_w=rv_w, rv_m=rv_m)
        self._observations.append(obs)

        rv_hat = None
        fit_ts_used = None
        if self._coefficients is not None and rv_d is not None and rv_w is not None and rv_m is not None:
            a, b, c, d = self._coefficients
            rv_hat = float(max(0.0, a + b * rv_d + c * rv_w + d * rv_m))
            fit_ts_used = str(self._fit_history[-1].fit_ts) if self._fit_history else None
            self._forecast_history.append((ts, rv_hat))

        self._refit_if_due(ts)

        return {
            "rv1_t": rv1,
            "rv_d": rv_d,
            "rv_w": rv_w,
            "rv_m": rv_m,
            "rv_hat_t": rv_hat,
            "fit_ts_used": fit_ts_used,
        }
