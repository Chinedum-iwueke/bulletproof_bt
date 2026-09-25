from pathlib import Path

from bt.institutional.strategy_catalog import build_strategy_capability_catalog


def test_catalog_exposes_only_native_registered_contracts_without_authority():
    root = Path(__file__).parents[1]
    result = build_strategy_capability_catalog(root, source_commit="a" * 40)
    assert result["schema_version"] == "alpha-strategy-capability-catalog-v1.1.0"
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
    l2_h3 = next(
        item for item in result["capabilities"] if item["hypothesis_id"] == "L2-H3"
    )
    assert l2_h3["research_contract"]["parameter_grid"]["T_hold"] == [12]
    assert l2_h3["research_contract"]["entry"]["order_timing"] == (
        "bar_close_submit_next_bar_execution"
    )
    assert l2_h3["research_contract"]["truth_contract"]["htf_completeness"] == (
        "closed_only"
    )
    assert len(l2_h3["research_contract_digest"]) == 64
    smoke = next(
        item
        for item in result["capabilities"]
        if item["hypothesis_id"] == "SAMPLE-PIPELINE-SMOKE"
    )
    assert smoke["bounded_weekly_reuse_eligible"] is False
    assert "non_research_fixture" in smoke["reuse_blockers"]

    cross_sectional = next(
        item
        for item in result["capabilities"]
        if item["hypothesis_id"]
        == "ALPHA-003-BYBIT-CROSS-SECTIONAL-LIQUIDITY-DISPERSION-REVERSAL"
    )
    assert cross_sectional["input_mode"] == "aligned_basket"
    assert cross_sectional["maximum_instruments"] == 3

    cross_asset = next(
        item
        for item in result["capabilities"]
        if item["hypothesis_id"]
        == "ALPHA-003-ETH-LIQUIDITY-DISPLACEMENT-BTC-RESIDUAL-60M"
    )
    assert cross_asset["input_mode"] == "aligned_basket"
    assert cross_asset["maximum_instruments"] == 2
