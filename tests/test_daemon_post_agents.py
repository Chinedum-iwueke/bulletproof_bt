from __future__ import annotations

from pathlib import Path

from orchestrator import research_daemon as rd
from orchestrator.analysis.llm_packet import build_llm_packet


def test_post_agent_command_order_and_state_dir_in_interpreter(tmp_path: Path) -> None:
    payload = {
        "name": "demo",
        "hypothesis": "research/hypotheses/demo.yaml",
        "outputs_root": str(tmp_path / "outputs"),
    }
    config = {
        "state_discovery_output_dir": "research/state_findings",
        "state_discovery_min_trades": 30,
        "state_discovery_min_bucket_trades": 10,
        "state_discovery_top_n": 25,
        "state_discovery_write_db": True,
        "include_state_discovery_in_verdict": True,
    }
    db_path = tmp_path / "r.sqlite"
    sd = rd.build_state_discovery_command(db_path, payload, config, mode="combined")
    interp = rd.build_interpret_command(db_path, payload, config)
    assert "state_discovery.py" in " ".join(sd)
    assert "--stable-root" in sd and "--vol-root" in sd
    assert "--state-discovery-dir" in interp
    assert "research/state_findings/tier2b" in interp
    assert "research/state_findings/tier2b" in sd
    assert "research/verdicts/tier2b" in interp


def test_interpreter_fallback_on_llm_timeout(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.interpret_experiment_results as ier

    class Ctx:
        hypothesis_path = tmp_path / "h.yaml"
        hypothesis_text = "h"
        class D:
            summary_rows = []
            summary_path = None
            runs_dataset_path = None
            trades_dataset_path = None
            strategy_summary_paths = []
            structural_summary_rows = []
        stable = D()
        volatile = D()

    monkeypatch.setattr(ier, "load_experiment_context", lambda **_: Ctx())
    monkeypatch.setattr(ier, "score_runs", lambda rows: [])
    monkeypatch.setattr(ier, "compute_diagnostics", lambda *a, **k: {"failure_mode": None})
    monkeypatch.setattr(ier, "compute_preliminary_verdict", lambda *a, **k: {"preliminary_verdict": "INCONCLUSIVE_NEEDS_MORE_DATA", "preliminary_reason": "r", "allowed_verdicts": ["INCONCLUSIVE_NEEDS_MORE_DATA"]})
    monkeypatch.setattr(ier, "build_llm_packet", lambda **_: {})
    monkeypatch.setattr(ier, "build_llm_prompt", lambda p: "prompt")
    monkeypatch.setattr(ier, "write_packet_files", lambda out, name, packet, prompt: (tmp_path / "p.json", tmp_path / "p.txt"))
    monkeypatch.setattr(ier, "write_markdown_verdict", lambda *a, **k: None)
    monkeypatch.setattr(ier, "call_llm_json", lambda **_: (_ for _ in ()).throw(TimeoutError("ollama timeout")))

    monkeypatch.setattr("sys.argv", [
        "x", "--name", "demo", "--hypothesis", "h.yaml", "--stable-root", "s", "--vol-root", "v", "--output-dir", str(tmp_path)
    ])
    rc = ier.main()
    assert rc == 0
    verdict_dir = tmp_path / "tier2"
    verdict = (verdict_dir / "demo_verdict.json").read_text(encoding="utf-8")
    assert "rule_based_fallback" in verdict
    assert (verdict_dir / "demo_llm_error.txt").exists()


def test_llm_packet_compacts_run_rows() -> None:
    rows = [
        {
            "run_id": "r1",
            "ev_r_net": 0.2,
            "robustness_score": 1.0,
            "num_trades": 20,
            "entry_decision_conditions_json": "{\"large\":\"payload\"}",
            "output_dir": "outputs/demo",
        }
    ]
    packet = build_llm_packet(
        name="demo",
        hypothesis_text="h",
        input_files={},
        stable_rows=rows,
        vol_rows=[],
        diagnostics={},
        preliminary={"preliminary_verdict": "INCONCLUSIVE_NEEDS_MORE_DATA", "allowed_verdicts": []},
        max_top_runs=1,
        max_bottom_runs=0,
    )
    assert packet["top_runs"] == [{"run_id": "r1", "ev_r_net": 0.2, "robustness_score": 1.0, "num_trades": 20}]
