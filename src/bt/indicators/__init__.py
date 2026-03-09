"""Streaming indicator implementations and factory."""
from __future__ import annotations

from bt.indicators.base import BaseIndicator, Indicator, MultiValueIndicator
from bt.indicators.registry import INDICATOR_REGISTRY, make_indicator, register

from .adl import ADL
from .aroon import Aroon
from .atr import ATR
from .bollinger import BollingerBands
from .candle_features import CandleFeatures
from .cci import CCI
from .chaikin_osc import ChaikinOscillator
from .choppiness import ChoppinessIndex
from .cmf import CMF
from .dmi_adx import DMIADX
from .trend import ADX, EfficiencyRatio
from .donchian import DonchianChannel
from .ema import EMA
from .fisher import FisherTransform
from .force_index import ForceIndex
from .heikin_ashi import HeikinAshi
from .historical_vol import HistoricalVolatility
from .hma import HMA
from .kama import KAMA
from .keltner import KeltnerChannel
from .macd import MACD
from .mfi import MFI
from .momentum import Momentum
from .obv import OBV
from .parabolic_sar import ParabolicSAR
from .pivot_points import PivotPoints
from .ppo import PPO
from .rma import RMA
from .roc import ROC
from .rsi import RSI
from .sma import SMA
from .stoch_rsi import StochRSI
from .stochastic import Stochastic
from .supertrend import Supertrend
from .t3 import T3
from .tema import TEMA
from .trix import TRIX
from .true_range import TrueRange
from .tsi import TSI
from .ultimate_oscillator import UltimateOscillator
from .vortex import Vortex
from .vpt import VPT
from .vwap import AnchoredVWAP, SessionVWAP, VWAP
from .vwma import VWMA
from .volatility import BollingerBandWidth
from .williams_r import WilliamsR
from .wma import WMA
from .dema import DEMA

__all__ = [
    "Indicator","BaseIndicator","MultiValueIndicator",
    "INDICATOR_REGISTRY","register","make_indicator",
    "EMA","ATR","VWAP","SessionVWAP","AnchoredVWAP","SMA","WMA","DEMA","TEMA","HMA","KAMA","RMA","VWMA","T3",
    "RSI","Stochastic","StochRSI","CCI","ROC","Momentum","WilliamsR","TSI","UltimateOscillator","FisherTransform",
    "TrueRange","BollingerBands","BollingerBandWidth","KeltnerChannel","DonchianChannel","ChoppinessIndex","UlcerIndex","HistoricalVolatility",
    "DMIADX","ADX","EfficiencyRatio","Aroon","MACD","PPO","TRIX","Vortex",
    "OBV","CMF","MFI","VPT","ADL","ChaikinOscillator","ForceIndex",
    "ParabolicSAR","Supertrend","PivotPoints","HeikinAshi","CandleFeatures",
]

from .ulcer_index import UlcerIndex
