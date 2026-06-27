import json
from pathlib import Path

import pytest

from bt.contracts.artifact_queries import catalog_artifact, interpret_query_result, query_artifacts
from bt.contracts.pine_bridge import compare_signals, compile_pine_v6, evaluate_portable_signals, parse_restricted_pine, pine_compatibility
from bt.contracts.qualification import build_backtest_demo_qualification, normalize_portable_ir
from bt.contracts.research_specs_v2 import build_artifact_bundle, canonical_hash


def portable_ir():
    card = json.loads(Path("tests/fixtures/contracts/csi_hypothesis_card_v1.json").read_text())
    card.pop("engine_strategy_name", None)
    card.pop("engine_hypothesis_template", None)
    card["features"] = [{"id": "ret", "source": "ohlcv", "source_field": "close", "transform": "return", "lag": 1}]
    card["gates"] = [{"left": "ret", "op": ">=", "right_param": "threshold"}]
    card["exit"] = {"type": "fixed_stop_time_exit", "stop_param": "stop_atr_multiple", "max_hold_param": "max_hold_bars"}
    card["parameters"] = {"threshold": [0.01], "stop_atr_multiple": [2.0], "max_hold_bars": [48], "r_per_trade": [0.005]}
    card["data_requirements"] = ["ohlcv"]
    return normalize_portable_ir(build_artifact_bundle(card, available_datasets={"ohlcv"})["strategy_spec"])


def complete_evidence(ir):
    return {"qualification_id": "q1", "program_id": "p1", "code_hash": "code", "data_snapshot_id": "data", "config_hash": "config", "truth_certification": "PASS", "blocking_contract_errors": False, "fees_bps": 4, "slippage_bps": 3, "spread_bps": 2, "timing_model": "next_bar", "leverage": 1, "liquidation_model": "engine", "trade_count": 100, "coverage_days": 365, "holdout_required": True, "holdout_status": "PASS", "cost_survival": "PASS", "risk_of_ruin": 0.01, "symbols": ["BTCUSDT"], "exchange_product_type": "perpetual", "unresolved_critical_verdicts": 0, "experiment_run_id": "run1"}


def test_c2_qualification_requires_exact_hash_approval_and_all_evidence():
    ir = portable_ir()
    evidence = complete_evidence(ir)
    blocked = build_backtest_demo_qualification(portable_ir=ir, evidence=evidence, approval={})
    assert blocked["status"] == "blocked" and "exact_hash_approval" in blocked["blockers"]
    approval = {"strategy_spec_hash": ir["strategy_spec_hash"], "risk_policy_hash": canonical_hash(ir["risk_policy"]), "config_hash": "config", "approved_by": "user", "approved_at": "2026-06-23T00:00:00Z"}
    qualified = build_backtest_demo_qualification(portable_ir=ir, evidence=evidence, approval=approval)
    assert qualified["status"] == "qualified" and qualified["snapshot_hash"]
    evidence["cost_survival"] = "FAIL"
    assert "cost_survival" in build_backtest_demo_qualification(portable_ir=ir, evidence=evidence, approval=approval)["blockers"]


def test_c2_25_queries_are_typed_exact_bounded_and_tenant_scoped():
    own = catalog_artifact({"catalog_id":"c1","account_id":"a1","program_id":"p1","artifact_type":"metrics","object_id":"run1","sensitivity":"program_private","content_hash":"h1","lineage":{"run_id":"run1"},"summary":"metrics","searchable_text":"sharpe","anchors":[{"row":1}],"schema":{"type":"object"},"query_payload":{"metrics":{"sharpe":1.25,"trades":100},"sample":100,"tier":"Tier2"},"units":{"sharpe":"ratio","trades":"count"}})
    foreign = catalog_artifact({**own,"catalog_id":"c2","account_id":"a2","object_id":"run2","content_hash":"h2"})
    result = query_artifacts([own,foreign],{"query_type":"run_metrics"},account_id="a1",program_id="p1",limit=1)
    assert result["result_count"] == 1 and result["rows"][0]["value"] == 1.25 and result["rows"][0]["unit"] == "ratio"
    assert all(item["object_id"] != "run2" for item in result["citations"])
    answer = interpret_query_result(result,"Does it show promise?")
    assert answer["facts"] and answer["canonical_query_hash"] == result["result_hash"]
    with pytest.raises(ValueError, match="unsupported"):
        query_artifacts([own],{"query_type":"invent_numbers"},account_id="a1",program_id="p1")


def test_c2_5_pine_compiler_is_deterministic_and_fail_closed():
    ir = portable_ir()
    one = compile_pine_v6(ir,export_id="e1",program_id="p1",approved=True,generated_at="2026-06-23T00:00:00Z")
    two = compile_pine_v6(ir,export_id="e1",program_id="p1",approved=True,generated_at="2026-06-23T00:00:00Z")
    assert one["bundle_hash"] == two["bundle_hash"]
    assert "barstate.isconfirmed" in one["source"] and "lookahead_on" not in one["source"]
    assert "Long invalidation" in one["source"]
    assert 'message="{\\"idempotency_key\\"' in one["source"]
    assert one["simulation_source"] and "strategy_simulation.pine" in one["manifest"]["files"]
    assert 'strategy.exit("Long stop"' in one["simulation_source"] and "strategy.close_all" in one["simulation_source"]
    assert one["parity"]["verdict"] == "provisional"
    bars = [{"timestamp":"t0","open":100,"close":100},{"timestamp":"t1","open":100,"close":102},{"timestamp":"t2","open":102,"close":104}]
    assert evaluate_portable_signals(ir, bars) == [{"timestamp":"t2","side":"long"}]
    private = dict(ir)
    private["features"] = [{"id":"funding","source":"funding","transform":"identity"}]
    private["data_requirements"]=["funding"]
    assert pine_compatibility(private)["status"] == "unsupported"
    with pytest.raises(ValueError, match="unsupported"):
        compile_pine_v6(private,export_id="e2",program_id="p1",approved=True,generated_at="2026-06-23T00:00:00Z")
    injected = {**ir, "strategy_spec_id": 'bad"\nalert("x")'}
    assert 'alert("x")' not in compile_pine_v6(injected,export_id="e3",program_id="p1",approved=True,generated_at="2026-06-23T00:00:00Z")["source"]
    bad_gate = {**ir, "gates": [{"left":"ret","op":"; alert(1)","right":0}]}
    with pytest.raises(ValueError, match="pine_gate_operator_unsupported"):
        compile_pine_v6(bad_gate,export_id="e4",program_id="p1",approved=True,generated_at="2026-06-23T00:00:00Z")


def test_c2_5_parity_and_restricted_import_report_truthfully():
    context={"symbol":"BTCUSDT","timeframe":"15m","window_start":"2026-01-01","window_end":"2026-02-01","timezone":"UTC","session":"24x7","parameter_hash":"p"}
    signal={"timestamp":"2026-01-02T00:00:00Z","side":"long"}
    assert compare_signals(engine=[signal],tradingview=[signal],context=context)["verdict"] == "verified"
    assert compare_signals(engine=[signal],tradingview=[],context=context)["verdict"] == "divergent"
    assert len(compare_signals(engine=[signal],tradingview=[{"timestamp":signal["timestamp"],"side":"short"}],context=context)["direction_mismatches"]) == 1
    safe=parse_restricted_pine('//@version=6\nindicator("x")\nx=input.float(1.0,"x")\n')
    assert safe["draft_spec_status"] == "draft"
    blocked=parse_restricted_pine('//@version=6\nstrategy("x")\nstrategy.entry("L", strategy.long)\n')
    assert blocked["draft_spec_status"] == "blocked" and "strategy.entry" in blocked["rejected_constructs"]


def test_c2_5_shared_manifest_fixture_matches_contract():
    fixture = json.loads(Path("tests/fixtures/contracts/pine_export_manifest_v1.json").read_text())
    schema = json.loads(Path("schemas/pine_export_manifest_v1.schema.json").read_text())
    assert not set(schema["required"]) - set(fixture)
    assert fixture["schema_version"] == schema["properties"]["schema_version"]["const"]
    assert fixture["source_sharing"] == "account_private"
    assert canonical_hash(fixture) == "8f310ec792d4d8df3aaa0c1d35af657517976be3b85c65d2eff46a9393804af2"
