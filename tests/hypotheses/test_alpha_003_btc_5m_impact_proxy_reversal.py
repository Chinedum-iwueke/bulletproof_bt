from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt.contracts.research_specs_v2 import canonical_hash
from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import HTFBar
from bt.evaluation.alpha_research import empirical_lower_quantile
from bt.governance.alpha_strategy_pipeline import (
    confirm_card,
    draft_research_card,
    qualify_card,
)
from bt.hypotheses.contract import HypothesisContract
from bt.strategy.btc_5m_impact_proxy_reversal import (
    Btc5mImpactProxyReversalStrategy,
)
from bt.validation.strategy_admission import validate_hypothesis_admission


ROOT = Path(__file__).parents[2]
QUESTION = (
    "Does an extreme point-in-time BTCUSDT 5m absolute return per unit "
    "quote_volume predict opposite-direction close-to-close return over the next 30m?"
)
DIGEST = "4b420fd26d516a1eb4d4109b697c5bebf5a5fcbe810742e06f72aa103a529ff8"
YAML_PATH = ROOT / "research/hypotheses/alpha_003_btc_5m_impact_proxy_reversal.yaml"


def _assignment() -> dict:
    return {
        "question": QUESTION,
        "question_digest": DIGEST,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "dataset_build_id": "fbb81c42-953b-42fb-8fe1-89c75b45e1aa",
        "dataset_digest": "9a211d8818c5ab8ec82ad5a7d38957e63eb387ea83d4b00541922a0eeca4aacb",
        "instrument": "BTCUSDT",
        "timeframe": "1m",
        "venue": "bybit",
        "window_start": "2023-01-01T00:00:00Z",
        "window_end": "2024-01-01T00:00:00Z",
        "max_variants": 8,
        "research_context": {"citations": []},
    }


def _bar(
    ts: pd.Timestamp,
    *,
    quote_volume: float | None = 2_000_000.0,
    close: float = 100.0,
) -> Bar:
    extra = {} if quote_volume is None else {"quote_volume": quote_volume}
    return Bar(ts, "BTCUSDT", close, close + 1.0, close - 1.0, close, 1.0, extra)


def _closed(ts: pd.Timestamp, close: float, *, complete: bool = True) -> HTFBar:
    return HTFBar(
        ts=ts - pd.Timedelta(minutes=5), symbol="BTCUSDT", open=close,
        high=close + 1.0, low=close - 1.0, close=close, volume=5.0,
        timeframe="5m", n_bars=5 if complete else 4, expected_bars=5,
        is_complete=complete,
    )


def _run_returns(strategy, returns: list[float], *, tradeable=True, with_quote=True):
    outputs = []
    close = 100.0
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    for index, value in enumerate(returns):
        bucket_start = start + pd.Timedelta(minutes=5 * index)
        close *= 1.0 + value
        for minute in range(5):
            ts = bucket_start + pd.Timedelta(minutes=minute)
            outputs.extend(strategy.on_bars(
                ts,
                {"BTCUSDT": _bar(
                    ts,
                    quote_volume=400_000.0 if with_quote else None,
                    close=close,
                )},
                {"BTCUSDT"} if tradeable else set(),
                {"positions": {}, "htf": {"5m": {}}},
            ))
    return outputs


def test_strategy_and_evaluator_share_exact_empirical_quantile() -> None:
    assert empirical_lower_quantile([1.0, 2.0, 3.0, 100.0], 0.75) == 3.0


def test_exact_native_card_is_discovered_and_compiles_deterministically() -> None:
    assert canonical_hash({"question": QUESTION}) == DIGEST
    card = draft_research_card(_assignment(), repository_root=str(ROOT))
    assert card["research_question"] == QUESTION
    assert card["claim"] == QUESTION
    assert card["status"] == "draft"
    confirmed = confirm_card(card, actor="founder-operator", confirmed_at="2026-09-18T00:00:00Z")
    first = qualify_card(confirmed, repository_root=str(ROOT))
    second = qualify_card(confirmed, repository_root=str(ROOT))
    assert first["qualified"] is True
    assert first["variant_count"] == 8
    assert first["artifact_bundle"] == second["artifact_bundle"]
    assert first["artifact_bundle"]["compile_readiness"]["status"] == "registry_ready"
    assert first["artifact_bundle"]["run_config"]["strategy"]["name"] == "btc_5m_impact_proxy_reversal"
    generated_evaluation = first["artifact_bundle"]["engine_hypothesis_yaml"][
        "evaluation"
    ]
    assert generated_evaluation["split"]["purge_seconds"] == 1800
    assert generated_evaluation["split"]["embargo_seconds"] == 1800
    assert generated_evaluation["native_implementation"][
        "matched_return_shock_control"
    ] == {
        "path": "src/bt/evaluation/alpha_research.py",
        "function": "impact_proxy_evaluation",
        "decision_time": "bucket_start_plus_5m",
        "gap_policy": "reset_return_normalization_and_atr_state",
    }
    assert first["review"]["gates"]["independent_review_complete"] is False
    assert first["authority"] == {"capital": False, "orders": False, "promotion": False, "self_approval": False}


def test_yaml_grid_and_admission_are_deterministic_and_classic_only() -> None:
    contract = HypothesisContract.from_yaml(YAML_PATH)
    one = contract.materialize_grid()
    two = contract.materialize_grid()
    assert one == two
    assert len(one) == 8
    assert len({item["config_hash"] for item in one}) == 8
    assert contract.schema.execution_semantics["required_extra_columns"] == ["quote_volume"]
    raw_contract = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    assert raw_contract["version"] == "1.6.0"
    assert raw_contract["execution_semantics"]["gap_state_policy"] == (
        "reset_return_normalization_and_atr_state"
    )
    assert (
        raw_contract["evaluation"]["native_implementation"]["split_binding"][
            "split_denominator"
        ]
        == "actual_strict_complete_5m_decision_rows"
    )
    assert raw_contract["costs"]["delay_bars"] == 1
    assert raw_contract["immutable_contract"]["resampling_policy"] == (
        "left_closed_left_labeled_complete_bars"
    )
    assert contract.schema.execution_semantics["resampling_interval"] == (
        "left_closed_right_open"
    )
    assert raw_contract["evaluation"]["split"] == {
        "method": "chronological_decision_row_fraction",
        "train_fraction": 0.6,
        "validation_fraction": 0.2,
        "test_fraction": 0.2,
        "boundary_policy": "next_row_after_prior_partition",
        "purge_seconds": 1800,
        "embargo_seconds": 1800,
    }
    report = validate_hypothesis_admission(YAML_PATH)
    assert report.status == "PASS", report.to_dict()
    raw = YAML_PATH.read_text(encoding="utf-8")
    assert "fast_path_generation: forbidden" in raw


def test_strategy_uses_only_completed_history_and_emits_opposite_direction() -> None:
    strategy = Btc5mImpactProxyReversalStrategy(
        impact_proxy_threshold=0.75, normalization_window=2,
        signal_direction="both", return_shock_control_band=0.1,
    )
    signals = _run_returns(strategy, [0.001] * 20 + [0.10])
    entries = [item for item in signals if not item.metadata.get("is_exit")]
    assert len(entries) == 1
    entry = entries[0]
    assert entry.side == Side.SELL
    assert entry.metadata["signal_return_5m"] == pytest.approx(0.10)
    assert entry.metadata["quote_volume_5m"] == pytest.approx(2_000_000.0)
    assert entry.metadata["target_horizon_minutes"] == 30
    assert pd.Timestamp(entry.metadata["signal_ts"]) == entry.ts + pd.Timedelta(minutes=1)
    assert pd.Timestamp(entry.metadata["signal_emitted_at"]) == entry.ts
    assert entry.metadata["decision_trace"]
    assert entry.metadata["stop_price"] > entry.metadata["entry_reference_price"]


def test_future_mutation_cannot_change_prior_decisions() -> None:
    prefix = [0.001] * 20 + [0.10]
    first = _run_returns(Btc5mImpactProxyReversalStrategy(
        impact_proxy_threshold=0.75, normalization_window=2
    ), prefix + [0.50])
    second = _run_returns(Btc5mImpactProxyReversalStrategy(
        impact_proxy_threshold=0.75, normalization_window=2
    ), prefix + [-0.50])
    cutoff = pd.Timestamp("2023-01-01T01:45:00Z")
    before_first = [(item.ts, item.side, item.metadata) for item in first if item.ts <= cutoff]
    before_second = [(item.ts, item.side, item.metadata) for item in second if item.ts <= cutoff]
    assert before_first == before_second


def test_htf_context_cannot_override_native_completed_bar_history() -> None:
    strategy = Btc5mImpactProxyReversalStrategy(
        impact_proxy_threshold=0.75, normalization_window=2
    )
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    close = 100.0
    latest = None
    signals = []
    # Native reconstruction is authoritative; even a corrupt HTF context cannot
    # alter the decision made on the fifth completed source row.
    for minute in range(110):
        ts = start + pd.Timedelta(minutes=minute)
        if minute % 5 == 0:
            close *= 1.10 if minute == 105 else 1.001
            latest = _closed(ts, close)
        context_bar = latest
        if minute == 109 and latest is not None:
            context_bar = _closed(ts, close * 100, complete=False)
        ctx = {"positions": {}, "htf": {"5m": {}}}
        if context_bar is not None:
            ctx["htf"]["5m"]["BTCUSDT"] = context_bar
        signals.extend(strategy.on_bars(
            ts,
            {"BTCUSDT": _bar(ts, quote_volume=400_000.0, close=close)},
            {"BTCUSDT"} if minute == 109 else set(),
            ctx,
        ))
    entries = [item for item in signals if not item.metadata.get("is_exit")]
    assert len(entries) == 1
    assert entries[0].ts == start + pd.Timedelta(minutes=109)
    assert entries[0].metadata["decision_ts"] == (
        start + pd.Timedelta(minutes=110)
    ).isoformat()
    assert entries[0].metadata["quote_volume_5m"] == pytest.approx(2_000_000.0)
    assert entries[0].metadata["signal_return_5m"] == pytest.approx(0.10)
    assert entries[0].metadata["quote_volume_bucket_ts"] == (
        start + pd.Timedelta(minutes=105)
    ).isoformat()


def test_quote_volume_bucket_must_match_the_exact_closed_price_bucket() -> None:
    strategy = Btc5mImpactProxyReversalStrategy(
        impact_proxy_threshold=0.75, normalization_window=2
    )
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    close = 100.0
    signals = []
    for index in range(21):
        ts = start + pd.Timedelta(minutes=5 * index)
        close *= 1.10 if index == 20 else 1.001
        closed = _closed(ts, close)
        if index == 20:
            closed = _closed(ts - pd.Timedelta(minutes=5), close)
        signals.extend(
            strategy.on_bars(
                ts,
                {"BTCUSDT": _bar(ts)},
                {"BTCUSDT"},
                {"positions": {}, "htf": {"5m": {"BTCUSDT": closed}}},
            )
        )
    assert signals == []


def test_quote_volume_bucket_rejects_a_missing_minute() -> None:
    strategy = Btc5mImpactProxyReversalStrategy(
        impact_proxy_threshold=0.75, normalization_window=2
    )
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    for minute in (0, 1, 3, 4):
        ts = start + pd.Timedelta(minutes=minute)
        strategy.on_bars(
            ts,
            {"BTCUSDT": _bar(ts, quote_volume=500_000.0)},
            {"BTCUSDT"},
            {"positions": {}, "htf": {"5m": {}}},
        )
    rollover = start + pd.Timedelta(minutes=5)
    signals = strategy.on_bars(
        rollover,
        {"BTCUSDT": _bar(rollover, quote_volume=500_000.0)},
        {"BTCUSDT"},
        {"positions": {}, "htf": {"5m": {"BTCUSDT": _closed(rollover, 110.0)}}},
    )
    assert signals == []


def test_missing_bucket_cannot_become_a_multi_bucket_return() -> None:
    strategy = Btc5mImpactProxyReversalStrategy(
        impact_proxy_threshold=0.75, normalization_window=2
    )
    assert _run_returns(strategy, [0.001] * 20) == []
    start = pd.Timestamp("2023-01-01T01:45:00Z")
    signals = []
    for minute in range(5):
        ts = start + pd.Timedelta(minutes=minute)
        signals.extend(strategy.on_bars(
            ts,
            {"BTCUSDT": _bar(ts, quote_volume=400_000.0, close=112.0)},
            {"BTCUSDT"},
            {"positions": {}, "htf": {"5m": {}}},
        ))
    assert signals == []
    assert "BTCUSDT" not in strategy._ratios
    assert "BTCUSDT" not in strategy._ranges


def test_exit_state_begins_only_after_fill_and_lands_on_exact_target() -> None:
    strategy = Btc5mImpactProxyReversalStrategy()
    target = pd.Timestamp("2023-01-01T00:30:00Z")
    before = target - pd.Timedelta(minutes=2)
    submit = target - pd.Timedelta(minutes=1)
    bar = _bar(before)

    # Merely emitting an entry cannot manufacture active-trade state. With no
    # filled position in engine context, no time exit is generated.
    assert strategy.on_bars(
        before,
        {"BTCUSDT": bar},
        {"BTCUSDT"},
        {"positions": {}, "htf": {"5m": {}}},
    ) == []

    position = {
        "BTCUSDT": {
            "side": "sell",
            "metadata": {"target_exit_ts": target.isoformat()},
        }
    }
    assert strategy.on_bars(
        before,
        {"BTCUSDT": bar},
        {"BTCUSDT"},
        {"positions": position, "htf": {"5m": {}}},
    ) == []
    exits = strategy.on_bars(
        submit,
        {"BTCUSDT": _bar(submit)},
        {"BTCUSDT"},
        {"positions": position, "htf": {"5m": {}}},
    )
    assert len(exits) == 1
    assert exits[0].side == Side.BUY
    assert exits[0].metadata["target_exit_ts"] == target.isoformat()
    assert exits[0].metadata["exit_submission_ts"] == submit.isoformat()
    assert exits[0].metadata["execution_delay_minutes"] == 1
    assert strategy.on_bars(
        target,
        {"BTCUSDT": _bar(target)},
        {"BTCUSDT"},
        {"positions": position, "htf": {"5m": {}}},
    ) == []


def test_fixed_at_entry_stop_is_detected_then_exits_on_next_bar() -> None:
    strategy = Btc5mImpactProxyReversalStrategy()
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    position = {"BTCUSDT": {"side": "buy", "metadata": {
        "entry_stop_price": 99.0,
        "target_exit_ts": (start + pd.Timedelta(minutes=30)).isoformat(),
    }}}
    breached = Bar(start, "BTCUSDT", 100.0, 100.5, 98.5, 99.5, 1.0, {"quote_volume": 1_000_000.0})
    exits = strategy.on_bars(start, {"BTCUSDT": breached}, {"BTCUSDT"}, {"positions": position, "htf": {"5m": {}}})
    assert len(exits) == 1
    assert exits[0].ts == start
    assert exits[0].side == Side.SELL
    assert exits[0].metadata["exit_reason"] == "fixed_completed_5m_atr_stop_breached"
    assert exits[0].metadata["stop_detection_policy"] == "completed_1m_then_next_bar"


@pytest.mark.parametrize("case", ["missing_quote", "inactive", "invalid_parameter"])
def test_negative_invalid_and_failed_outcomes_are_retained(case: str) -> None:
    if case == "invalid_parameter":
        with pytest.raises(ValueError, match="impact_proxy_threshold"):
            Btc5mImpactProxyReversalStrategy(impact_proxy_threshold=1.0)
        return
    strategy = Btc5mImpactProxyReversalStrategy(
        impact_proxy_threshold=0.75, normalization_window=2
    )
    signals = _run_returns(
        strategy, [0.001] * 20 + [0.10],
        tradeable=case != "inactive", with_quote=case != "missing_quote",
    )
    assert signals == []


def test_card_binding_failure_is_not_silently_remapped(tmp_path: Path) -> None:
    card = json.loads((ROOT / f"research/hypotheses/cards/{DIGEST}.json").read_text())
    card["research_question"] = "A different question"
    target = tmp_path / "research/hypotheses/cards"
    target.mkdir(parents=True)
    (target / f"{DIGEST}.json").write_text(json.dumps(card), encoding="utf-8")
    with pytest.raises(ValueError, match="engineered_card_question_mismatch"):
        draft_research_card(_assignment(), repository_root=str(tmp_path))
