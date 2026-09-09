from pathlib import Path

from scripts.run_alpha_research_assignment import (
    AUTHORITY,
    engineering_required,
    held_out_evaluation,
    hypothesis_identity,
    record_alpha_memory,
    retain_bundle,
)


def assignment():
    return {
        "base_ref": "a" * 40,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "campaign_digest": "b" * 64,
        "source_candidate_id": "22222222-2222-4222-8222-222222222222",
        "source_candidate_digest": "c" * 64,
        "question": "Does a new weekend interaction survive costs?",
        "question_digest": "d" * 64,
        "domain_key": "market-microstructure",
        "dataset_build_id": "33333333-3333-4333-8333-333333333333",
        "dataset_digest": "e" * 64,
        "instrument": "BTCUSDT",
        "timeframe": "1m",
        "research_context": {
            "corpus_digest": "f" * 64,
            "abstained": False,
            "citations": [
                {
                    "object_id": "44444444-4444-4444-8444-444444444444",
                    "content_digest": "1" * 64,
                    "coordinates": {"page": 4, "line_start": 2, "line_end": 9},
                    "text": "Weekend liquidity is structurally distinct.",
                    "confidence": 0.8,
                }
            ],
        },
    }


def test_explicit_registered_identity_is_only_taken_from_typed_marker() -> None:
    assert hypothesis_identity("hypothesis_id: L1-H9B") == "L1-H9B"
    prose = "Run something similar to L1-H9B but with a weekend interaction"
    assert hypothesis_identity(prose) == prose


def test_unsupported_question_retains_citations_and_zero_trials(tmp_path: Path) -> None:
    result = engineering_required(assignment(), tmp_path, "not registered")
    attempt = result["alpha_campaign_attempt"]
    assert result["disposition"] == "engineering_required"
    assert attempt["trial_count"] == 0
    assert attempt["outcome"] == "failed"
    assert attempt["failure_stage"] == "strategy_generation"
    assert attempt["gate_report"]["shadow_eligible"] is False
    assert result["hypothesis_card"]["research_context"]["citations"]
    assert set(AUTHORITY.values()) == {False}
    assert (tmp_path / "strategy-engineering-requirement.json").is_file()


def test_held_out_evaluation_is_temporal_and_doubles_observed_costs(
    tmp_path: Path,
) -> None:
    import pandas as pd

    pd.DataFrame(
        {
            "entry_ts": [
                "2026-01-01T00:00:00Z",
                "2026-02-01T00:00:00Z",
                "2026-02-02T00:00:00Z",
            ],
            "r_net": [9.0, 0.5, 0.25],
            "cost_drag_r": [0.1, 0.1, 0.1],
        }
    ).to_csv(tmp_path / "trades.csv", index=False)
    report = held_out_evaluation(tmp_path, "2026-02-01T00:00:00Z")
    assert report["trade_count"] == 2
    assert report["mean_net_r"] == 0.375
    assert report["double_cost_mean_net_r"] == 0.275
    assert report["adequate_support"] is False


def test_durable_bundle_and_native_memory_are_idempotent(tmp_path: Path) -> None:
    import json

    source = tmp_path / "source"
    source.mkdir()
    bundle = {"bundle_digest": "9" * 64, "manifest_digest": "8" * 64}
    (source / "run_bundle_manifest.json").write_text(
        json.dumps({"manifest_digest": bundle["manifest_digest"]}),
        encoding="utf-8",
    )
    destination = retain_bundle(source, tmp_path / "bundles", bundle)
    assert retain_bundle(source, tmp_path / "bundles", bundle) == destination

    first = record_alpha_memory(
        tmp_path / "memory.sqlite", assignment=assignment(), bundle=bundle
    )
    second = record_alpha_memory(
        tmp_path / "memory.sqlite", assignment=assignment(), bundle=bundle
    )
    assert first["disposition"] == "created"
    assert second["disposition"] == "existing"
    assert first["memory_database_digest"] == second["memory_database_digest"]
