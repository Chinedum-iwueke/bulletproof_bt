from pathlib import Path

from bt.institutional.strategy_catalog import build_strategy_capability_catalog


def test_catalog_exposes_only_native_registered_contracts_without_authority():
    root = Path(__file__).parents[1]
    result = build_strategy_capability_catalog(root, source_commit="a" * 40)
    assert result["schema_version"] == "alpha-strategy-capability-catalog-v1.0.0"
    assert result["capital_or_order_authority"] is False
    assert len(result["catalog_digest"]) == 64
    assert result["capabilities"]
    assert all(
        item["contract_path"].startswith("research/hypotheses/")
        for item in result["capabilities"]
    )
    reusable = {
        item["hypothesis_id"]
        for item in result["capabilities"]
        if item["bounded_weekly_reuse_eligible"]
    }
    assert "ALPHA-WEEKEND-MOMENTUM" in reusable
    assert len(reusable) >= 2
    assert all(
        item["logging_requirements"]
        for item in result["capabilities"]
        if item["bounded_weekly_reuse_eligible"]
    )
    smoke = next(
        item
        for item in result["capabilities"]
        if item["hypothesis_id"] == "SAMPLE-PIPELINE-SMOKE"
    )
    assert smoke["bounded_weekly_reuse_eligible"] is False
    assert "non_research_fixture" in smoke["reuse_blockers"]
