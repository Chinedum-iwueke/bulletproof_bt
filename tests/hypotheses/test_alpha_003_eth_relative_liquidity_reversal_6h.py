from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from bt.governance.alpha_strategy_pipeline import (
    confirm_card,
    draft_research_card,
    qualify_card,
)
from bt.hypotheses.contract import HypothesisContract
from bt.validation.strategy_admission import validate_hypothesis_admission
from bt.strategy.eth_relative_liquidity_reversal_6h import (
    FROZEN_GRID,
    OUTPUT_FIELDS,
    QUESTION,
    compile_decision_rows,
    relative_liquidity_reversal_grid_evaluation,
)
import bt.strategy.eth_relative_liquidity_reversal_6h as strategy
import scripts.run_alpha_research_assignment as assignment_runner
from scripts.run_alpha_research_assignment import (
    BridgeError,
    heldout_test_consulted,
    verify_trusted_dataset_bindings,
)

ROOT = Path(__file__).parents[2]
YAML_PATH = (
    ROOT / "research/hypotheses/alpha_003_eth_relative_liquidity_reversal_6h.yaml"
)


def _panels(hours: int = 180) -> dict[str, pd.DataFrame]:
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    result = {}
    for symbol, base in (("ETHUSDT", 100.0), ("BTCUSDT", 200.0), ("SOLUSDT", 50.0)):
        rows = []
        for minute in range(hours * 60):
            hour = minute // 60
            close = base * np.exp((-0.002 if symbol == "ETHUSDT" else 0.0002) * hour)
            rows.append(
                {
                    "ts": start + pd.Timedelta(minutes=minute),
                    "symbol": symbol,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": 100.0,
                    "quote_volume": 2_000_000.0
                    - (hour * 1000 if symbol == "ETHUSDT" else 0),
                }
            )
        result[symbol] = pd.DataFrame(rows)
    return result


def _representation_plan() -> dict:
    return yaml.safe_load(YAML_PATH.read_text())["representation_plan"]


def _overlap_receipt(assignment: dict) -> dict:
    document = {
        "schema_version": "alpha-basket-overlap-admission-v1.0.0",
        "authority": "DATA-002/003",
        "dataset_bindings": [
            {
                key: item[key]
                for key in ("instrument", "dataset_build_id", "dataset_digest")
            }
            for item in assignment["dataset_bindings"]
        ],
        "instruments": sorted(assignment["instruments"]),
        "minimum_contiguous_days": 365,
        "admitted_start": assignment["window_start"],
        "admitted_end": assignment["window_end"],
    }
    return {**document, "record_digest": assignment_runner.digest(document)}


def test_exact_contract_is_discovered_without_template_substitution() -> None:
    raw = yaml.safe_load(YAML_PATH.read_text())
    assignment = {
        "question": QUESTION,
        "question_digest": raw["immutable_contract"]["question_digest"],
        "dataset_build_id": raw["immutable_contract"]["dataset_bindings"][0][
            "dataset_build_id"
        ],
        "dataset_digest": raw["immutable_contract"]["dataset_bindings"][0][
            "dataset_digest"
        ],
        "venue": "bybit",
        "instrument": "ETHUSDT",
        "timeframe": "1m",
        "window_start": "2023-01-01T00:00:00Z",
        "window_end": "2024-01-01T00:00:00Z",
    }
    assert (
        draft_research_card(assignment, repository_root=str(ROOT))["research_question"]
        == QUESTION
    )
    assert len(HypothesisContract.from_yaml(YAML_PATH).to_run_specs()) == 4
    card = json.loads(
        (
            ROOT
            / "research/hypotheses/cards"
            / "4454892e0f78b507d772b6e82733007301db4065d6fb8415ab3298bcc1276916.json"
        ).read_text()
    )
    assert card["independent_review_required"]
    assert (
        card["engine_hypothesis_template_digest"]
        == hashlib.sha256(YAML_PATH.read_bytes()).hexdigest()
    )
    assert validate_hypothesis_admission(YAML_PATH).status == "PASS"


def test_grid_opens_test_once_and_retains_negative_invalid_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def score(rows, train, params, partition, plan_digest=""):
        calls.append((partition, dict(params)))
        if partition == "test":
            return strategy._result(
                "negative", "heldout_negative", params, records=[{"x": 1}]
            )
        return strategy._result(
            "positive",
            "pass",
            params,
            records=[{"x": 1}],
            mean_net_residual_return=-float(params["underperformance_threshold"]),
        )

    monkeypatch.setattr(strategy, "_score", score)
    result = relative_liquidity_reversal_grid_evaluation(
        _panels(),
        parameter_grid={key: list(value) for key, value in FROZEN_GRID.items()},
        evaluation_start="2023-01-01T00:00:00Z",
        evaluation_end="2024-01-01T00:00:00Z",
    )
    assert result["test_open_count"] == 1
    assert [part for part, _ in calls].count("test") == 1
    assert result["outcome"] == "negative"
    assert heldout_test_consulted(
        is_liquidity_residual=False, liquidity_grid=None, eth_relative_grid=result
    )
    invalid = relative_liquidity_reversal_grid_evaluation(
        _panels(),
        parameter_grid={"wrong": [1]},
        evaluation_start="2023-01-01T00:00:00Z",
        evaluation_end="2024-01-01T00:00:00Z",
    )
    assert invalid["outcome"] == "invalid" and invalid["test_open_count"] == 0
    monkeypatch.setattr(
        strategy, "_score", lambda *a, **k: strategy._result("failed", "boom", {})
    )
    failed = relative_liquidity_reversal_grid_evaluation(
        _panels(),
        parameter_grid=FROZEN_GRID,
        evaluation_start="2023-01-01T00:00:00Z",
        evaluation_end="2024-01-01T00:00:00Z",
    )
    assert failed["outcome"] == "failed" and failed["test_open_count"] == 0


def test_missing_hour_invalidates_every_crossing_history_and_target() -> None:
    panels = _panels(30)
    missing = pd.Timestamp("2023-01-01T12:00:00Z")
    for symbol, frame in panels.items():
        panels[symbol] = frame.loc[frame["ts"].dt.floor("1h") != missing].reset_index(
            drop=True
        )

    rows = compile_decision_rows(panels).set_index("decision_ts")

    assert not bool(rows.loc[missing + pd.Timedelta(hours=1), "complete"])
    for decision in pd.date_range(
        missing + pd.Timedelta(hours=1),
        missing + pd.Timedelta(hours=7),
        freq="1h",
        tz="UTC",
    ):
        assert pd.isna(rows.loc[decision, "ETHUSDT_return_6h"])
    for decision in pd.date_range(
        missing - pd.Timedelta(hours=5),
        missing + pd.Timedelta(hours=1),
        freq="1h",
        tz="UTC",
    ):
        assert not bool(rows.loc[decision, "target_complete"])


def test_split_purge_and_embargo_are_elapsed_six_hour_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partitions = {}

    def score(rows, train, params, partition, plan_digest=""):
        partitions.setdefault(partition, []).append((rows.copy(), train.copy()))
        return strategy._result(
            "positive",
            "pass",
            params,
            records=[{"record_kind": "test"}],
            mean_net_residual_return=0.01,
        )

    monkeypatch.setattr(strategy, "_score", score)
    result = relative_liquidity_reversal_grid_evaluation(
        _panels(180),
        parameter_grid={key: list(value) for key, value in FROZEN_GRID.items()},
        evaluation_start="2023-01-01T00:00:00Z",
        evaluation_end="2024-01-01T00:00:00Z",
    )

    assert result["test_open_count"] == 1
    validation, train = partitions["validation"][0]
    test, _ = partitions["test"][0]
    assert validation.decision_ts.min() - train.target_exit_ts.max() >= pd.Timedelta(
        hours=6
    )
    assert test.decision_ts.min() - validation.target_exit_ts.max() >= pd.Timedelta(
        hours=6
    )


def test_trusted_btc_receipt_and_all_bindings_are_content_identified(
    tmp_path: Path,
) -> None:
    raw = yaml.safe_load(YAML_PATH.read_text())["immutable_contract"]
    bindings = [dict(item) for item in raw["dataset_bindings"]]
    for item in bindings:
        path = tmp_path / f"{item['instrument']}.parquet"
        path.write_bytes(item["instrument"].encode())
        item["dataset_path"] = str(path)
        item["dataset_digest"] = hashlib.sha256(path.read_bytes()).hexdigest()
        item["partition_digest"] = item["dataset_digest"]
    expected = [dict(item) for item in bindings]
    assignment = {
        "dataset_bindings": bindings,
        "window_start": raw["window"]["start"],
        "window_end": raw["window"]["end"],
    }
    verify_trusted_dataset_bindings(assignment, expected)
    altered = {
        **assignment,
        "dataset_bindings": [dict(item) for item in assignment["dataset_bindings"]],
    }
    next(
        item for item in altered["dataset_bindings"] if item["instrument"] == "BTCUSDT"
    )["producer_receipt_digest"] = "bad"
    with pytest.raises(BridgeError, match="BTCUSDT"):
        verify_trusted_dataset_bindings(altered, expected)
    Path(bindings[0]["dataset_path"]).write_bytes(b"mutated")
    with pytest.raises(BridgeError, match="materialized dataset content mismatch"):
        verify_trusted_dataset_bindings(assignment, expected)
    document = yaml.safe_load(YAML_PATH.read_text())
    assert (
        tuple(document["execution_semantics"]["adaptive_representation_fields"])
        == OUTPUT_FIELDS
    )


def test_execute_registered_retains_reviewed_scientific_bundles_and_rejects_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = yaml.safe_load(YAML_PATH.read_text())
    panels = _panels(120)
    bindings = []
    trusted = {}
    source_hashes = {}
    for source in raw["immutable_contract"]["dataset_bindings"]:
        item = dict(source)
        path = tmp_path / f"{item['instrument']}.parquet"
        panels[item["instrument"]].to_parquet(path, index=False)
        item["dataset_path"] = str(path)
        item["dataset_key"] = item["instrument"].lower()
        source_hashes[path] = hashlib.sha256(path.read_bytes()).hexdigest()
        bindings.append(item)
        trusted[item["instrument"]] = {
            key: item[key]
            for key in (
                "dataset_build_id",
                "dataset_digest",
                "catalog_digest",
                "manifest_digest",
                "producer_receipt_digest",
                "lake_governance_digest",
                "partition_digest",
            )
        }
    original_file_digest = assignment_runner.file_digest

    def admitted_file_digest(path: Path) -> str:
        candidate = Path(path)
        if candidate in source_hashes:
            actual = hashlib.sha256(candidate.read_bytes()).hexdigest()
            if actual == source_hashes[candidate]:
                symbol = candidate.stem
                return trusted[symbol]["dataset_digest"]
        return original_file_digest(candidate)

    monkeypatch.setattr(assignment_runner, "file_digest", admitted_file_digest)
    assignment = {
        "base_ref": "a" * 40,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "campaign_digest": "b" * 64,
        "source_candidate_id": "22222222-2222-4222-8222-222222222222",
        "source_candidate_digest": "c" * 64,
        "question": QUESTION,
        "question_digest": raw["immutable_contract"]["question_digest"],
        "domain_key": "cross-asset-micro-alpha",
        "dataset_build_id": bindings[0]["dataset_build_id"],
        "dataset_digest": bindings[0]["dataset_digest"],
        "dataset_path": bindings[0]["dataset_path"],
        "dataset_key": bindings[0]["dataset_key"],
        "dataset_bindings": bindings,
        "instrument": "ETHUSDT",
        "instruments": list(strategy.INSTRUMENTS),
        "venue": "bybit",
        "timeframe": "1m",
        "research_timeframe": "1h",
        "window_start": raw["immutable_contract"]["window"]["start"],
        "window_end": raw["immutable_contract"]["window"]["end"],
        "tier": "Tier2B",
        "max_variants": 4,
        "representation_plan": _representation_plan(),
        "bundle_root": str(tmp_path / "retained"),
        "memory_database": str(tmp_path / "memory.sqlite3"),
        "research_context": {
            "corpus_digest": "d" * 64,
            "abstained": True,
            "citations": [],
        },
    }
    assignment["overlap_admission_receipt"] = _overlap_receipt(assignment)
    card = confirm_card(
        draft_research_card(assignment, repository_root=str(ROOT)),
        actor="founder-operator",
        confirmed_at="2026-09-27T00:00:00Z",
    )
    qualification = qualify_card(card, repository_root=str(ROOT))
    qualification["qualified"] = True
    qualification["review"]["gates"]["independent_review_complete"] = True
    assignment["qualification"] = qualification
    monkeypatch.setattr(assignment_runner, "governed_review_verified", lambda *_: True)
    monkeypatch.setattr(
        assignment_runner, "complete_independent_review", lambda _a, item: item
    )

    result = assignment_runner.execute_registered(
        assignment, ROOT, tmp_path / "output", max_workers=1
    )

    assert result["disposition"] == "native_execution_complete"
    assert result["alpha_campaign_attempt"]["outcome"] in {
        "positive",
        "negative",
        "invalid",
        "failed",
    }
    assert (
        result["alpha_campaign_attempt"]["gate_report"]["independent_review_complete"]
        is True
    )
    manifests = list(
        (tmp_path / "output").glob("run-bundles-*/bundles/*/run_bundle_manifest.json")
    )
    assert len(manifests) == 4
    assert (
        len(
            list(
                (tmp_path / "output").glob(
                    "run-bundles-*/bundles/*/artifacts/eth_relative_liquidity_reversal_validation_evaluation.json"
                )
            )
        )
        == 4
    )
    assert (
        len(
            list(
                (tmp_path / "output").glob(
                    "run-bundles-*/bundles/*/artifacts/required_trade_logging_evaluation.json"
                )
            )
        )
        == 4
    )

    path = Path(bindings[0]["dataset_path"])
    changed = pd.read_parquet(path)
    changed.loc[0, "close"] += 1.0
    changed.to_parquet(path, index=False)
    with pytest.raises(BridgeError, match="materialized dataset content mismatch"):
        assignment_runner.execute_registered(
            assignment, ROOT, tmp_path / "mutated-output", max_workers=1
        )
