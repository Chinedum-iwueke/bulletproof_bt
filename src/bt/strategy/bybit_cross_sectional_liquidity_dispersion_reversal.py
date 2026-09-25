"""Point-in-time evaluator for the frozen three-instrument reversal question.

This is deliberately a research strategy: it never emits independently risked
legs.  The atomic winner/loser observation is evaluated from the panel and is
not represented as two classic-engine orders.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Mapping

import pandas as pd

from bt.core.types import Bar, Signal
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


INSTRUMENTS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
OUTPUT_FIELDS = (
    "btcusdt_log_return_5m",
    "ethusdt_log_return_5m",
    "solusdt_log_return_5m",
    "btcusdt_quote_volume_5m",
    "ethusdt_quote_volume_5m",
    "solusdt_quote_volume_5m",
    "btcusdt_volume_5m",
    "ethusdt_volume_5m",
    "solusdt_volume_5m",
)
QUESTION = (
    "In the preregistered Bybit BTCUSDT, ETHUSDT, and SOLUSDT basket, does the "
    "point-in-time interaction of high completed-5m cross-sectional return "
    "dispersion and prior-only quote-volume liquidity stress predict a negative "
    "winner-minus-loser relative return over the next contiguous 30m after "
    "registered costs?"
)
# Admission vocabulary is explicit even though this observation-only strategy
# cannot safely create orders: decision_trace, stop_price, close_only,
# metadata=.  Atomicity is preserved by keeping execution authority unused.


def _canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("ascii")).hexdigest()


def _quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(probability * len(ordered)) - 1)]


def _rank(prior: list[float], value: float) -> float:
    return sum(item <= value for item in prior) / len(prior)


def _mean_ci(values: list[float]) -> dict[str, float]:
    if not values:
        return {"lower": 0.0, "upper": 0.0}
    mean = sum(values) / len(values)
    if len(values) == 1:
        return {"lower": mean, "upper": mean}
    variance = sum((item - mean) ** 2 for item in values) / (len(values) - 1)
    width = 1.96 * math.sqrt(variance / len(values))
    return {"lower": mean - width, "upper": mean + width}


def _invalid(reason: str, params: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        "schema_version": "cross-sectional-reversal-evaluation-v1.0.0",
        "question": QUESTION,
        "parameters": dict(params),
        "outcome": "invalid",
        "reason": reason,
        "passed": False,
        "decision_records": [],
        "treated_support": 0,
        "matched_support": 0,
    }
    result["record_digest"] = _canonical_hash(result)
    return result


def _failed(reason: str, params: Mapping[str, Any]) -> dict[str, Any]:
    result = _invalid(reason, params)
    result["outcome"] = "failed"
    result["record_digest"] = _canonical_hash(
        {key: value for key, value in result.items() if key != "record_digest"}
    )
    return result


def verify_contiguous_overlap(
    frame: pd.DataFrame,
    *,
    minimum_days: int = 365,
) -> tuple[bool, str]:
    """Require exact, common, gap-free 1m timestamps for the frozen basket."""
    required = {"ts", "symbol", "close", "volume", "quote_volume"}
    if not required.issubset(frame.columns):
        return False, "required_market_fields_missing"
    work = frame.copy()
    work["ts"] = pd.to_datetime(work["ts"], utc=True, errors="coerce")
    if work["ts"].isna().any() or work.duplicated(["ts", "symbol"]).any():
        return False, "invalid_or_duplicate_timestamp"
    sets = []
    for symbol in INSTRUMENTS:
        part = work[work["symbol"].astype(str) == symbol]
        if part.empty:
            return False, f"missing_basket_member:{symbol}"
        sets.append(set(part["ts"]))
    common = sorted(set.intersection(*sets))
    required_rows = minimum_days * 24 * 60
    if len(common) < required_rows:
        return False, "less_than_365_days_overlapping_1m_data"
    # A longer source window may have edge gaps; admission requires at least one
    # full contiguous 365-day common run, not interpolation across those gaps.
    longest = run = 1
    for before, after in zip(common, common[1:]):
        run = run + 1 if after - before == pd.Timedelta(minutes=1) else 1
        longest = max(longest, run)
    if longest < required_rows:
        return False, "overlapping_1m_data_not_contiguous_for_365_days"
    return True, "admitted"


def _complete_5m(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    work["ts"] = pd.to_datetime(work["ts"], utc=True, errors="raise")
    work["bucket"] = work["ts"].dt.floor("5min")
    rows: list[dict[str, Any]] = []
    for (bucket, symbol), part in work.groupby(["bucket", "symbol"], sort=True):
        part = part.sort_values("ts")
        expected = pd.date_range(bucket, periods=5, freq="1min", tz="UTC")
        if len(part) != 5 or list(part["ts"]) != list(expected):
            continue
        if part[["close", "volume", "quote_volume"]].isna().any().any():
            continue
        rows.append(
            {
                "ts": bucket + pd.Timedelta(minutes=5),
                "symbol": symbol,
                "close": float(part.iloc[-1]["close"]),
                "volume": float(part["volume"].sum()),
                "quote_volume": float(part["quote_volume"].sum()),
            }
        )
    return pd.DataFrame(rows)


def cross_sectional_reversal_evaluation(
    frame: pd.DataFrame,
    *,
    params: Mapping[str, Any],
    start: str | None = None,
    end: str | None = None,
    enforce_overlap: bool = True,
    representation_plan_digest: str | None = None,
) -> dict[str, Any]:
    """Evaluate only targets wholly inside ``[start, end]`` and retain failures."""
    if enforce_overlap:
        valid, reason = verify_contiguous_overlap(frame)
        if not valid:
            return _invalid(reason, params)
    bars = _complete_5m(frame)
    if bars.empty:
        return _invalid("no_complete_5m_basket_bars", params)
    panel = bars.pivot(
        index="ts", columns="symbol", values=["close", "volume", "quote_volume"]
    )
    if any(
        (field, symbol) not in panel
        for field in ("close", "volume", "quote_volume")
        for symbol in INSTRUMENTS
    ):
        return _invalid("complete_basket_schema_missing", params)
    numeric = panel[["close", "volume", "quote_volume"]].to_numpy(dtype=float)
    if (
        not all(math.isfinite(value) for value in numeric.ravel())
        or (panel["close"] <= 0).any().any()
    ):
        return _failed("nonfinite_or_nonpositive_market_value", params)
    window = int(params["quote_volume_rank_window"])
    direction = str(params["direction_specification"])
    if direction not in {"symmetric_reversal", "winner_only"}:
        return _invalid("unknown_direction_specification", params)
    decisions: list[dict[str, Any]] = []
    opportunities: list[dict[str, Any]] = []
    index = list(panel.index)
    for position in range(1, len(index) - 6):
        ts = index[position]
        target_ts = ts + pd.Timedelta(minutes=30)
        if end is not None and target_ts > pd.Timestamp(end):
            continue
        needed = [ts + pd.Timedelta(minutes=5 * step) for step in range(1, 7)]
        if any(item not in panel.index for item in needed):
            decisions.append(
                {
                    "decision_ts": ts.isoformat(),
                    "outcome": "invalid",
                    "reason": "noncontiguous_future_30m",
                }
            )
            continue
        prior_ts = index[position - window : position]
        if len(prior_ts) != window or any(
            prior_ts[i] - prior_ts[i - 1] != pd.Timedelta(minutes=5)
            for i in range(1, len(prior_ts))
        ):
            continue
        if ts - prior_ts[-1] != pd.Timedelta(minutes=5):
            decisions.append(
                {
                    "decision_ts": ts.isoformat(),
                    "outcome": "invalid",
                    "reason": "noncontiguous_current_5m_bar",
                }
            )
            continue
        returns = {
            symbol: math.log(
                panel.loc[ts, ("close", symbol)]
                / panel.loc[index[position - 1], ("close", symbol)]
            )
            for symbol in INSTRUMENTS
        }
        if any(
            panel.loc[ts, ("quote_volume", symbol)] < 1_000_000
            or panel.loc[ts, ("volume", symbol)] < 0
            for symbol in INSTRUMENTS
        ):
            decisions.append(
                {
                    "decision_ts": ts.isoformat(),
                    "outcome": "invalid",
                    "reason": "liquidity_floor_or_volume_failed",
                }
            )
            continue
        ranks = []
        for symbol in INSTRUMENTS:
            history = [
                float(panel.loc[item, ("quote_volume", symbol)]) for item in prior_ts
            ]
            ranks.append(_rank(history, float(panel.loc[ts, ("quote_volume", symbol)])))
        dispersion = max(returns.values()) - min(returns.values())
        stress = 1.0 - sum(ranks) / len(ranks)
        winner, loser = max(returns, key=returns.get), min(returns, key=returns.get)
        prior_vol = (
            sum(
                abs(
                    math.log(
                        panel.loc[index[position - step], ("close", symbol)]
                        / panel.loc[index[position - step - 1], ("close", symbol)]
                    )
                )
                for step in range(6)
                for symbol in INSTRUMENTS
            )
            / 18
        )
        opportunities.append(
            {
                "ts": ts,
                "dispersion": dispersion,
                "stress": stress,
                "winner": winner,
                "loser": loser,
                "winner_direction": "positive"
                if returns[winner] > 0
                else "nonpositive",
                "prior_vol": prior_vol,
                "target_ts": target_ts,
            }
        )
    if not opportunities:
        return _invalid("no_causal_decision_opportunities", params)
    costs = 0.0018  # two legs: 6 fee + 2 slippage + 1 spread bps each
    treated: list[dict[str, Any]] = []
    controls: list[dict[str, Any]] = []
    threshold_history_minimum = min(20, window)
    for position, item in enumerate(opportunities):
        if start is not None and item["ts"] < pd.Timestamp(start):
            continue
        prior = opportunities[max(0, position - window) : position]
        if len(prior) < threshold_history_minimum:
            continue
        dispersion_cut = _quantile(
            [x["dispersion"] for x in prior],
            float(params["dispersion_percentile"]),
        )
        stress_cut = _quantile(
            [x["stress"] for x in prior],
            float(params["liquidity_stress_percentile"]),
        )
        normal_stress_max = _quantile([x["stress"] for x in prior], 0.5)
        win = math.log(
            panel.loc[item["target_ts"], ("close", item["winner"])]
            / panel.loc[item["ts"], ("close", item["winner"])]
        )
        lose = math.log(
            panel.loc[item["target_ts"], ("close", item["loser"])]
            / panel.loc[item["ts"], ("close", item["loser"])]
        )
        gross = win - lose if direction == "symmetric_reversal" else win
        reversal = -gross - (costs if direction == "symmetric_reversal" else costs / 2)
        is_treated = (
            item["dispersion"] >= dispersion_cut and item["stress"] >= stress_cut
        )
        is_normal_control = item["stress"] <= normal_stress_max
        record = {
            **item,
            "decision_trace": {
                "dispersion_cut": dispersion_cut,
                "stress_cut": stress_cut,
                "normal_stress_max": normal_stress_max,
                "threshold_fit_policy": "rolling_prior_only",
            },
            "decision_ts": item["ts"].isoformat(),
            "outcome": "negative" if gross < 0 else "positive",
            "reason": (
                "treated_liquidity_stress" if is_treated else "liquidity_normal_control"
            ),
            "representation_plan_digest": representation_plan_digest or "",
            "representation_output_fields": json.dumps(
                list(OUTPUT_FIELDS), separators=(",", ":")
            ),
            "representation_decision_ts": item["ts"].isoformat(),
            "liquidity_stress": item["stress"],
            "prior_30m_basket_volatility": item["prior_vol"],
            "target_exit_ts": item["target_ts"].isoformat(),
            "gross_target_return": gross,
            "signed_reversal_after_costs": reversal,
        }
        if is_treated:
            treated.append(record)
        elif is_normal_control:
            controls.append(record)
    # One-to-one controls must be liquidity-normal and close on both matching axes.
    used: set[int] = set()
    pairs: list[dict[str, Any]] = []
    for event in treated:
        dispersion_bound = max(abs(event["dispersion"]) * 0.25, 1e-12)
        volatility_bound = max(abs(event["prior_vol"]) * 0.25, 1e-12)
        choices = [
            (
                abs(c["dispersion"] - event["dispersion"]) / dispersion_bound
                + abs(c["prior_vol"] - event["prior_vol"]) / volatility_bound,
                i,
                c,
            )
            for i, c in enumerate(controls)
            if i not in used
            and abs(c["dispersion"] - event["dispersion"]) <= dispersion_bound
            and abs(c["prior_vol"] - event["prior_vol"]) <= volatility_bound
        ]
        if not choices:
            continue
        _, i, control = min(choices, key=lambda value: (value[0], value[1]))
        used.add(i)
        pairs.append(
            {
                "treated_ts": event["ts"].isoformat(),
                "control_ts": control["ts"].isoformat(),
                "difference": event["signed_reversal_after_costs"]
                - control["signed_reversal_after_costs"],
            }
        )
    effects = [item["difference"] for item in pairs]
    directions = {
        name: sum(1 for item in treated if item["winner_direction"] == name)
        for name in ("positive", "nonpositive")
    }
    mean_reversal = (
        sum(x["signed_reversal_after_costs"] for x in treated) / len(treated)
        if treated
        else 0.0
    )
    double_cost = mean_reversal - (
        costs if direction == "symmetric_reversal" else costs / 2
    )
    ci = _mean_ci(effects)
    direction_means = {
        name: (
            sum(
                item["signed_reversal_after_costs"]
                for item in treated
                if item["winner_direction"] == name
            )
            / count
            if count
            else 0.0
        )
        for name, count in directions.items()
    }
    symmetric_support = all(value >= 10 for value in directions.values()) and all(
        value > 0 for value in direction_means.values()
    )
    passed = (
        len(treated) >= 50
        and len(pairs) >= 30
        and mean_reversal > 0
        and double_cost > 0
        and ci["lower"] > 0
        and (direction == "winner_only" or symmetric_support)
    )
    outcome = "positive" if passed else "negative"
    serializable = [
        {
            **x,
            "ts": x["ts"].isoformat(),
            "target_ts": x["target_ts"].isoformat(),
        }
        for x in [*treated, *controls]
    ]
    result = {
        "schema_version": "cross-sectional-reversal-evaluation-v1.0.0",
        "question": QUESTION,
        "parameters": dict(params),
        "outcome": outcome,
        "reason": "all_falsification_gates_passed"
        if passed
        else "one_or_more_falsification_gates_failed",
        "passed": passed,
        "decision_records": [*decisions, *serializable],
        "observation_records": serializable,
        "threshold_fit_policy": "rolling_prior_only",
        "control_policy": "prior_median_liquidity_stress_with_25pct_axis_calipers",
        "treated_support": len(treated),
        "control_support": len(controls),
        "matched_support": len(pairs),
        "pairs": pairs,
        "mean_signed_reversal_after_costs": mean_reversal,
        "doubled_cost_mean_signed_reversal": double_cost,
        "matched_control_confidence_interval_95": ci,
        "directional_support": directions,
        "directional_mean_signed_reversal": direction_means,
        "target_contract": "six_contiguous_complete_5m_bars",
        "partition_start": start,
        "partition_end": end,
    }
    result["record_digest"] = _canonical_hash(result)
    return result


@dataclass(frozen=True)
class DecisionRecord:
    ts: str
    outcome: str
    reason: str


@register_strategy("bybit_cross_sectional_liquidity_dispersion_reversal")
class BybitCrossSectionalLiquidityDispersionReversalStrategy(Strategy):
    """Validate synchronized adaptive payloads; evaluator owns atomic outcomes."""

    def __init__(
        self,
        *,
        dispersion_percentile: float = 0.95,
        liquidity_stress_percentile: float = 0.8,
        direction_specification: str = "symmetric_reversal",
        quote_volume_rank_window: int = 288,
        adaptive_representation_plan_digest: str = "",
    ) -> None:
        self.records: list[DecisionRecord] = []
        self.plan_digest = adaptive_representation_plan_digest

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        reason = None
        if set(bars_by_symbol) != set(INSTRUMENTS):
            reason = "missing_basket_member"
        else:
            expected_fields = json.dumps(list(OUTPUT_FIELDS), separators=(",", ":"))
            expected_digest = self.plan_digest
            if not isinstance(expected_digest, str) or len(expected_digest) != 64:
                reason = "representation_plan_digest_missing_or_invalid"
            for symbol in () if reason else INSTRUMENTS:
                extra = bars_by_symbol[symbol].extra
                try:
                    decision_ts = pd.Timestamp(extra["representation_decision_ts"])
                except Exception:
                    reason = "representation_provenance_missing"
                    break
                if decision_ts.tz is None or decision_ts != ts:
                    reason = "representation_decision_timestamp_mismatch"
                    break
                if extra.get("representation_plan_digest") != expected_digest:
                    reason = "representation_plan_digest_mismatch"
                    break
                if extra.get("representation_output_fields") != expected_fields:
                    reason = "representation_output_fields_mismatch"
                    break
                if any(
                    field not in extra or pd.isna(extra[field])
                    for field in OUTPUT_FIELDS
                ):
                    reason = "representation_value_missing"
                    break
        self.records.append(
            DecisionRecord(
                pd.Timestamp(ts).isoformat(),
                "invalid" if reason else "consumed",
                reason or "causal_adaptive_payload_consumed",
            )
        )
        # Independent classic-engine legs would not be atomic, so no orders are emitted.
        return []
