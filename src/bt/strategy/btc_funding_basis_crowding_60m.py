"""Point-in-time BTC funding/basis crowding strategy and exact evaluator.

The executable strategy is deliberately only an engine signal adapter.  The
scientific result is produced by :func:`funding_basis_matched_evaluation`,
which retains every decision (including invalid, control, negative and failed
decisions) and constructs the preregistered one-to-one matched comparison.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from math import sqrt
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


QUESTION = "Does point-in-time positive BTCUSDT funding stress combined with a positive completed-bar mark-to-index basis predict a lower BTCUSDT close-to-close return over the next 60m than matched controls?"


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if pd.notna(result) else None


def _quantile(values: list[float], q: float) -> float:
    return float(pd.Series(values).quantile(q, interpolation="lower"))


def _overlap_cluster_standard_error(
    pairs: list[dict[str, Any]], *, horizon_minutes: int = 60
) -> float:
    """Cluster pair differences whose treated or control outcome windows overlap."""
    if len(pairs) <= 1:
        return float("inf")
    parent = list(range(len(pairs)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    intervals: list[tuple[pd.Timestamp, pd.Timestamp, int]] = []
    for index, pair in enumerate(pairs):
        for key in ("treated_decision_ts", "control_decision_ts"):
            start = pd.Timestamp(pair[key])
            intervals.append(
                (start, start + pd.Timedelta(minutes=horizon_minutes), index)
            )
    active: dict[int, pd.Timestamp] = {}
    for start, end, index in sorted(intervals):
        active = {
            active_index: active_end
            for active_index, active_end in active.items()
            if active_end > start
        }
        for active_index in active:
            union(index, active_index)
        active[index] = max(end, active.get(index, end))

    values = [float(pair["difference"]) for pair in pairs]
    mean = sum(values) / len(values)
    scores: dict[int, float] = defaultdict(float)
    for index, value in enumerate(values):
        scores[find(index)] += value - mean
    cluster_count = len(scores)
    if cluster_count <= 1:
        return float("inf")
    variance = (
        cluster_count
        / (cluster_count - 1)
        * sum(score * score for score in scores.values())
        / (len(values) * len(values))
    )
    return sqrt(max(variance, 0.0))


def _funding_cycle_distance(left: int, right: int) -> float:
    difference = abs(left - right)
    return min(difference, 480 - difference) / 480.0


def _maximum_drawdown(returns: list[float]) -> float:
    """Return positive peak-to-trough drawdown magnitude for ordered returns."""
    equity = 1.0
    peak = 1.0
    maximum = 0.0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        maximum = max(maximum, (peak - equity) / peak)
    return float(maximum)


def _complete_decisions(frame: pd.DataFrame) -> pd.DataFrame:
    """Build causal completed-5m rows; a rollover row is never an input."""
    data = frame.copy()
    data["ts"] = pd.to_datetime(data["ts"], utc=True)
    data["funding_source_ts"] = pd.to_datetime(data["funding_source_ts"], utc=True)
    data = data.sort_values("ts", kind="mergesort").reset_index(drop=True)
    data["bucket"] = data["ts"].dt.floor("5min")
    rows: list[dict[str, Any]] = []
    grouped = {bucket: part for bucket, part in data.groupby("bucket", sort=True)}
    buckets = pd.date_range(
        data["bucket"].min(), data["bucket"].max(), freq="5min", tz="UTC"
    )
    for bucket in buckets:
        part = grouped.get(bucket, data.iloc[0:0])
        decision = bucket + pd.Timedelta(minutes=5)
        expected = pd.date_range(bucket, periods=5, freq="1min", tz="UTC")
        complete = len(part) == 5 and list(part["ts"]) == list(expected)
        quote = pd.to_numeric(part["quote_volume"], errors="coerce")
        complete = complete and quote.notna().all()
        prior = data.loc[
            (data["ts"] < decision)
            & (data["funding_source_ts"] <= data["ts"])
        ]
        funding = None
        funding_source = None
        if not prior.empty:
            # Source time, not row order, defines the latest backward join.
            latest_source = prior["funding_source_ts"].max()
            latest = prior.loc[prior["funding_source_ts"] == latest_source].iloc[-1]
            funding, funding_source = _number(latest["funding_rate"]), latest_source
        last = part.iloc[-1] if len(part) else None
        basis = (
            _number(last.get("basis_close_vs_index"))
            if last is not None
            else None
        )
        mark_close = _number(last.get("mark_close")) if last is not None else None
        index_close = _number(last.get("index_close")) if last is not None else None
        rows.append({
            "decision_ts": decision, "bucket_ts": bucket, "complete": bool(complete),
            "close": _number(last["close"]) if last is not None else None,
            "quote_volume_5m": float(quote.sum()) if quote.notna().all() else None,
            "funding_rate": funding, "funding_source_ts": funding_source,
            "basis": basis, "mark_close": mark_close, "index_close": index_close,
        })
    result = pd.DataFrame(rows).set_index("decision_ts", drop=False)
    valid_close = result["close"].where(result["complete"])
    result["trailing_return_60m"] = valid_close.pct_change(12, fill_method=None)
    one_bar = valid_close.pct_change(fill_method=None)
    # Includes the return ending at the current completed decision boundary.
    result["realized_volatility_6h"] = one_bar.rolling(72, min_periods=72).std(ddof=0)
    result["target_return_60m"] = pd.Series(index=result.index, dtype=float)
    if len(result) > 12:
        result.loc[result.index[:-12], "target_return_60m"] = (
            valid_close.iloc[12:].to_numpy()
            / valid_close.iloc[:-12].to_numpy() - 1.0
        )
    # All twelve target buckets must themselves be complete and contiguous.
    result["target_complete"] = [
        bool(index + 12 < len(result) and result["complete"].iloc[index + 1:index + 13].all())
        for index in range(len(result))
    ]
    result.loc[~result["target_complete"], "target_return_60m"] = float("nan")
    return result.reset_index(drop=True)


def funding_basis_matched_evaluation(
    frame: pd.DataFrame, *, params: Mapping[str, Any], start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None, minimum_support: int = 30,
    cost_bps: float = 9.0,
) -> dict[str, Any]:
    """Evaluate the exact question with deterministic no-reuse matching."""
    decisions = _complete_decisions(frame)
    threshold = float(params["funding_percentile_threshold"])
    basis_threshold = float(params["basis_threshold_bps"]) / 10_000.0
    history: deque[float | None] = deque(maxlen=25_920)
    records: list[dict[str, Any]] = []
    for row in decisions.itertuples(index=False):
        funding_rate = _number(row.funding_rate)
        basis = _number(row.basis)
        funding_source_ts = (
            None if pd.isna(row.funding_source_ts) else pd.Timestamp(row.funding_source_ts)
        )
        complete_history = (
            len(history) == history.maxlen
            and all(value is not None for value in history)
        )
        percentile_value = (
            _quantile([float(value) for value in history if value is not None], threshold)
            if complete_history
            else None
        )
        valid = bool(row.complete and row.target_complete and row.quote_volume_5m is not None and row.quote_volume_5m >= 1_000_000 and funding_rate is not None and percentile_value is not None and funding_source_ts is not None and funding_source_ts <= row.decision_ts and basis is not None and _number(row.mark_close) is not None and _number(row.index_close) is not None and row.trailing_return_60m == row.trailing_return_60m and row.realized_volatility_6h == row.realized_volatility_6h and row.target_return_60m == row.target_return_60m)
        stressed = bool(valid and percentile_value is not None and funding_rate > 0 and funding_rate >= percentile_value and basis > basis_threshold)
        records.append({
            "decision_ts": row.decision_ts.isoformat(), "status": "treated" if stressed else "control" if valid else "invalid",
            "valid": valid, "treated": stressed, "funding_rate": funding_rate,
            "funding_source_ts": funding_source_ts.isoformat() if funding_source_ts is not None else None,
            "funding_threshold": percentile_value, "basis": basis,
            "trailing_return_60m": row.trailing_return_60m, "realized_volatility_6h": row.realized_volatility_6h,
            "funding_cycle_position": int(row.decision_ts.hour * 60 + row.decision_ts.minute) % 480,
            "target_return_60m": row.target_return_60m,
        })
        # The feature contract is a decision-row window, not an observation
        # window. Missing point-in-time funding must occupy its row and make
        # the window unavailable until it rolls out.
        history.append(funding_rate)
    partition_records = [
        r for r in records
        if (start is None or pd.Timestamp(r["decision_ts"]) >= pd.Timestamp(start))
        and (end is None or pd.Timestamp(r["decision_ts"]) <= pd.Timestamp(end))
    ]
    eligible = [r for r in partition_records if r["valid"]]
    treated = [r for r in eligible if r["treated"]]
    controls = [r for r in eligible if not r["treated"]]
    used: set[int] = set()
    pairs = []
    for item in treated:
        candidates = [(
            abs(item["trailing_return_60m"] - control["trailing_return_60m"])
            + abs(item["realized_volatility_6h"] - control["realized_volatility_6h"])
            + _funding_cycle_distance(
                item["funding_cycle_position"], control["funding_cycle_position"]
            ),
            index,
            control,
        ) for index, control in enumerate(controls) if index not in used]
        if not candidates:
            item["status"] = "failed_unmatched"
            continue
        _, index, control = min(candidates, key=lambda value: (value[0], value[2]["decision_ts"]))
        used.add(index)
        difference = item["target_return_60m"] - control["target_return_60m"]
        pairs.append({"treated_decision_ts": item["decision_ts"], "control_decision_ts": control["decision_ts"], "difference": difference})
    differences = [p["difference"] for p in pairs]
    mean = float(pd.Series(differences).mean()) if differences else 0.0
    standard_error = _overlap_cluster_standard_error(pairs)
    upper = mean + 1.96 * standard_error
    # A short economic expression pays costs; in treated-minus-control space
    # doubled costs therefore move the difference toward zero (positive).
    doubled_cost_difference = mean + 2.0 * 2.0 * cost_bps / 10_000.0
    directional_support = {
        "positive_trailing_return": sum(r["trailing_return_60m"] > 0 for r in treated),
        "nonpositive_trailing_return": sum(r["trailing_return_60m"] <= 0 for r in treated),
    }
    treated_net_returns = [
        -float(item["target_return_60m"]) - 2.0 * cost_bps / 10_000.0
        for item in sorted(treated, key=lambda value: value["decision_ts"])
    ]
    maximum_drawdown = _maximum_drawdown(treated_net_returns)
    supported = (
        len(pairs) >= minimum_support
        and min(directional_support.values(), default=0) >= 10
    )
    if not eligible:
        outcome = "invalid"
    elif not supported:
        outcome = "failed"
    elif upper < 0 and doubled_cost_difference < 0:
        outcome = "positive"
    else:
        outcome = "negative"
    return {
        "schema_version": "btc-funding-basis-matched-evaluation-v1.0.0", "question": QUESTION,
        "parameters": dict(params), "outcome": outcome,
        "decision_records": partition_records,
        "pairs": pairs, "matched_support": len(pairs), "treated_support": len(treated),
        "control_support": len(controls), "treated_minus_control_mean": mean,
        "confidence_interval_95": {"lower": mean - 1.96 * standard_error, "upper": upper},
        "confidence_interval_method": "overlap_component_cluster_robust_60m",
        "doubled_cost_treated_minus_control": doubled_cost_difference,
        "maximum_drawdown": maximum_drawdown,
        "directional_support": directional_support,
        "passed": outcome == "positive",
    }


@dataclass
class _Bucket:
    start: pd.Timestamp
    rows: list[Bar]


@register_strategy("btc_funding_basis_crowding_60m")
class BtcFundingBasisCrowding60mStrategy(Strategy):
    """Submit a short only after a fully closed, causally joined 5m bucket."""
    def __init__(self, *, funding_percentile_threshold: float = .95, basis_threshold_bps: float = 0.0, r_per_trade: float = .005) -> None:
        self.threshold, self.basis_bps, self.r = float(funding_percentile_threshold), float(basis_threshold_bps), float(r_per_trade)
        self.buckets: dict[str, _Bucket] = {}
        self.funding: dict[str, list[tuple[pd.Timestamp, float]]] = defaultdict(list)
        self.funding_history: dict[str, deque[float | None]] = defaultdict(
            lambda: deque(maxlen=25_920)
        )
        self.completed_buckets: dict[str, deque[tuple[pd.Timestamp, float | None, bool]]] = defaultdict(
            lambda: deque(maxlen=73)
        )
        self.exit_sent: set[str] = set()
        self.last_history_decision: dict[str, pd.Timestamp] = {}

    def _funding_at(self, symbol: str, decision_ts: pd.Timestamp) -> tuple[pd.Timestamp, float] | None:
        available = [(source, rate) for source, rate in self.funding[symbol] if source <= decision_ts]
        return max(available, key=lambda item: item[0]) if available else None

    def _advance_history_to(self, symbol: str, decision_ts: pd.Timestamp) -> tuple[deque[float | None], tuple[pd.Timestamp, float] | None]:
        """Insert every elapsed 5m decision row, including wholly missing rows."""
        history = self.funding_history[symbol]
        previous = self.last_history_decision.get(symbol)
        cursor = decision_ts if previous is None else previous + pd.Timedelta(minutes=5)
        while cursor < decision_ts:
            observation = self._funding_at(symbol, cursor)
            history.append(observation[1] if observation is not None else None)
            cursor += pd.Timedelta(minutes=5)
        latest = self._funding_at(symbol, decision_ts)
        self.last_history_decision[symbol] = decision_ts
        return history, latest

    def _evaluate_bucket(
        self,
        symbol: str,
        closed: _Bucket,
        *,
        signal_ts: pd.Timestamp,
        decision_ts: pd.Timestamp,
        contiguous: bool,
        tradeable: set[str],
        position: Any,
    ) -> list[Signal]:
        rows = closed.rows
        complete = len(rows) == 5 and [r.ts for r in rows] == list(
            pd.date_range(closed.start, periods=5, freq="1min", tz="UTC")
        )
        values = [r.extra for r in rows]
        qv = [_number(v.get("quote_volume")) for v in values]
        last = rows[-1] if rows else None
        close = float(last.close) if last else None
        completed = self.completed_buckets[symbol]
        completed.append((closed.start, close, complete))
        hist, latest = self._advance_history_to(symbol, decision_ts)
        complete_funding_history = len(hist) == hist.maxlen and all(
            value is not None for value in hist
        )
        funding_threshold = (
            _quantile([float(value) for value in hist], self.threshold)
            if complete_funding_history
            else None
        )
        hist.append(latest[1] if latest is not None else None)
        basis = _number(values[-1].get("basis_close_vs_index")) if values else None
        mark_close = _number(values[-1].get("mark_close")) if values else None
        index_close = _number(values[-1].get("index_close")) if values else None
        complete_history = (
            len(completed) == completed.maxlen
            and all(item[1] is not None and item[2] for item in completed)
            and all(
                completed[index][0]
                == completed[index - 1][0] + pd.Timedelta(minutes=5)
                for index in range(1, len(completed))
            )
        )
        ready = (
            contiguous
            and complete
            and complete_history
            and all(v is not None for v in qv)
            and sum(qv) >= 1_000_000
            and funding_threshold is not None
            and latest is not None
            and latest[1] > 0
            and latest[1] >= funding_threshold
            and basis is not None
            and mark_close is not None
            and index_close is not None
            and basis > self.basis_bps / 10_000
            and symbol in tradeable
            and position is None
        )
        if not ready:
            return []
        stop = close * 1.03
        trace = make_decision_trace(
            reason_code="positive_funding_and_basis_crowding",
            setup_class="funding_basis_crowding",
            hypothesis_branch="lower_next_60m",
            conditions_bool_map={
                "complete": complete,
                "funding_stress": True,
                "positive_basis": True,
            },
            blockers_bool_map={"existing_position": False},
            permission_layer_state={"tradeable": True},
            parameter_combination={
                "funding_percentile_threshold": self.threshold,
                "basis_threshold_bps": self.basis_bps,
            },
            gate_values={"funding_rate": latest[1], "basis_close_vs_index": basis},
            gate_thresholds={
                "funding_rate": funding_threshold,
                "basis": self.basis_bps / 10000,
            },
            gate_margins={"funding": latest[1] - funding_threshold},
            most_binding_gate="funding",
        )
        return [Signal(
            ts=signal_ts,
            symbol=symbol,
            side=Side.SELL,
            signal_type="btc_funding_basis_crowding_entry",
            confidence=1.0,
            metadata={
                "strategy": "btc_funding_basis_crowding_60m",
                "strategy_id": "ALPHA-003-BTC-FUNDING-BASIS-CROWDING-60M",
                "decision_trace": trace,
                "stop_price": stop,
                "entry_stop_price": stop,
                "entry_reference_price": close,
                "r_per_trade": self.r,
                "sizing_mode": "risk_at_stop",
                "funding_rate": latest[1],
                "funding_source_ts": latest[0].isoformat(),
                "basis_close_vs_index": basis,
                "mark_close": mark_close,
                "index_close": index_close,
                "funding_percentile_threshold_value": funding_threshold,
                "target_horizon_minutes": 60,
                "target_exit_ts": (decision_ts + pd.Timedelta(minutes=60)).isoformat(),
                "signal_ts": signal_ts.isoformat(),
                "decision_ts": decision_ts.isoformat(),
                "requested_risk_amount": None,
                "risk_utilization_pct": None,
                "under_risked_trade": None,
                "risk_metadata_authority": "bt.risk.risk_engine.RiskEngine",
            },
        )]

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        output: list[Signal] = []
        positions = ctx.get("positions", {}) if isinstance(ctx, Mapping) else {}
        for symbol, bar in sorted(bars_by_symbol.items()):
            position = positions.get(symbol) if isinstance(positions, Mapping) else None
            metadata = position.get("metadata", {}) if isinstance(position, Mapping) else {}
            if position is None:
                self.exit_sent.discard(symbol)
            target = pd.Timestamp(metadata["target_exit_ts"]) if metadata.get("target_exit_ts") else None
            submit_exit_at = target - pd.Timedelta(minutes=1) if target is not None else None
            if submit_exit_at is not None and ts >= submit_exit_at and symbol not in self.exit_sent:
                output.append(Signal(ts, symbol, Side.BUY, "funding_basis_60m_exit", 1.0, {
                    "close_only": True, "is_exit": True,
                    "target_exit_ts": target.isoformat(),
                    "exit_submission_ts": pd.Timestamp(ts).isoformat(),
                    "execution_delay_minutes": 1,
                }))
                self.exit_sent.add(symbol)
            try:
                stop_price = float(metadata.get("entry_stop_price"))
            except (TypeError, ValueError):
                stop_price = None
            if position is not None and stop_price is not None and float(bar.high) >= stop_price and symbol not in self.exit_sent:
                output.append(Signal(ts, symbol, Side.BUY, "funding_basis_fixed_stop_exit", 1.0, {
                    "close_only": True, "is_exit": True,
                    "exit_reason": "fixed_3pct_stop_breached",
                    "stop_detection_policy": "completed_1m_then_next_bar",
                }))
                self.exit_sent.add(symbol)

            extra = bar.extra if isinstance(bar.extra, Mapping) else {}
            source = pd.Timestamp(extra["funding_source_ts"]) if extra.get("funding_source_ts") is not None else None
            rate = _number(extra.get("funding_rate"))
            if source is not None and rate is not None and source <= bar.ts:
                self.funding[symbol].append((source, rate))

            bucket_start = bar.ts.floor("5min")
            current = self.buckets.get(symbol)
            if current is not None and current.start != bucket_start and current.rows:
                output.extend(self._evaluate_bucket(
                    symbol, current, signal_ts=ts, decision_ts=bucket_start,
                    contiguous=False, tradeable=tradeable, position=position,
                ))
            if current is None or current.start != bucket_start:
                current = _Bucket(bucket_start, [])
                self.buckets[symbol] = current
            current.rows.append(bar)
            if bar.ts == bucket_start + pd.Timedelta(minutes=4):
                decision_ts = bucket_start + pd.Timedelta(minutes=5)
                output.extend(self._evaluate_bucket(
                    symbol, current, signal_ts=bar.ts, decision_ts=decision_ts,
                    contiguous=True, tradeable=tradeable, position=position,
                ))
                self.buckets[symbol] = _Bucket(decision_ts, [])
        return output
