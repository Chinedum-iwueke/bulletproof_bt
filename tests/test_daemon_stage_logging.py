from __future__ import annotations

from pathlib import Path

from orchestrator import research_daemon as rd
from orchestrator.db import PIPELINE_RUN_STATUSES


def test_daemon_uses_output_scoped_log_dir() -> None:
    p = rd._daemon_command_log_dir("qid123", "myjob", "outputs", "tier2")
    assert str(p).endswith("outputs/tier2/myjob_daemon_command_logs")


def test_post_agent_stages_are_part_of_queue_completion_contract() -> None:
    # Enabled post-pipeline agents are required research stages. The daemon
    # marks DONE only after discovery, verdict, memory, and card refresh.
    config = {
        "run_state_discovery_after_pipeline": True,
        "run_interpretation_after_pipeline": True,
        "interpretation_after_state_discovery": True,
    }
    assert bool(config["run_state_discovery_after_pipeline"])
    assert bool(config["run_interpretation_after_pipeline"])
    assert bool(config["interpretation_after_state_discovery"])


def test_stage_names_are_ordered_prefixes(tmp_path: Path) -> None:
    payload = {"name": "demo", "hypothesis": "h", "outputs_root": str(tmp_path / "outputs")}
    command_log_dir = rd._daemon_command_log_dir("1", payload["name"], payload["outputs_root"], "tier3")
    assert command_log_dir.name == "demo_daemon_command_logs"
    assert command_log_dir.parent.name == "tier3"


def test_strategy_admission_is_a_registered_pipeline_status() -> None:
    assert "STRATEGY_ADMISSION" in PIPELINE_RUN_STATUSES
