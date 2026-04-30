from pathlib import Path
from orchestrator.validate_hypothesis_logging import load_strategy_registry, find_strategy_name, audit_strategy_file
from bt.logging.decision_trace import flatten_decision_trace, StrategyDecisionTrace, make_decision_trace
from bt.logging.trade_enrichment import enrich_trade_row


def test_audit_finds_hypotheses():
    files = sorted(Path('research/hypotheses').glob('*.yaml'))
    assert files


def test_registry_maps_strategies():
    reg = load_strategy_registry()
    assert 'l1_h1_vol_floor_trend' in reg


def test_detects_missing_decision_trace_fixture(tmp_path: Path):
    p = tmp_path / 's.py'
    p.write_text('@register_strategy("x")\nclass X: pass\n', encoding='utf-8')
    flags = audit_strategy_file(p)
    assert flags['has_decision_trace'] is False


def test_decision_trace_json_safe():
    tr = StrategyDecisionTrace(reason_code='r', conditions_bool_map={'a': True})
    flat = flatten_decision_trace(tr)
    assert isinstance(flat['entry_decision_conditions_json'], str)


def test_trade_enrichment_preserves_state_and_decision_fields():
    row = {'entry_state_vol_regime': 'vol_mid', 'entry_decision_setup_class': 'trend', 'r_net': 1.0, 'path_mfe_r': 2.0, 'path_mae_r': 0.5}
    out = enrich_trade_row(row)
    assert out['entry_state_vol_regime'] == 'vol_mid'
    assert out['entry_decision_setup_class'] == 'trend'


def test_find_strategy_name_from_entry_field():
    name = find_strategy_name({'entry': {'strategy': 'abc'}})
    assert name == 'abc'


def test_make_decision_trace_json_safe():
    trace = make_decision_trace(reason_code="x", setup_class="y", parameter_combination={"a": 1})
    assert trace["reason_code"] == "x"
    assert isinstance(trace["parameter_combination"], str)
