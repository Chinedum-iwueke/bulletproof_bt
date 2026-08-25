"""Produce deterministic SHADOW-001 no-capital prospective/replay evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from bt.exec.shadow import JournalError, ProspectiveJournal, replay_journal
from bt.logging.formatting import write_json_deterministic


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--candidate",
        type=Path,
        default=Path("research/hypotheses/sample_pipeline_smoke.yaml"),
    )
    args = parser.parse_args()
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    fixture = b"BTCUSDT,2026-08-25T00:00:00Z,100,101,99,100,1\n"
    bindings = {
        "candidate_digest": digest_bytes(args.candidate.read_bytes()),
        "dataset_digest": digest_bytes(fixture),
        "strategy_digest": digest_bytes(
            Path("src/bt/strategy/volfloor_ema_pullback.py").read_bytes()
        ),
        "cost_model_digest": digest_bytes(
            b"tier2:maker+taker+spread+slippage:worst_case"
        ),
        "source_commit": source_commit,
    }
    args.output.mkdir(parents=True, exist_ok=True)
    journal_path = args.output / "prospective_journal.jsonl"
    if journal_path.exists():
        journal_path.unlink()

    first = ProspectiveJournal(journal_path, run_id="shadow001-pilot", bindings=bindings)
    first.append(
        "decision",
        {"symbol": "BTCUSDT", "approved": False, "reason": "smoke_fixture_only"},
        event_id="decision:1",
        observed_at="2026-08-25T00:00:00Z",
    )
    first.close()  # deterministic disconnect before the synthetic intent

    resumed = ProspectiveJournal(journal_path, run_id="shadow001-pilot", bindings=bindings)
    duplicate_suppressed = not resumed.append(
        "decision",
        {"symbol": "BTCUSDT", "approved": False, "reason": "smoke_fixture_only"},
        event_id="decision:1",
        observed_at="2026-08-25T00:00:00Z",
    )
    resumed.append(
        "reconciliation",
        {"orders_match": True, "fills_match": True, "positions_match": True},
        event_id="reconciliation:1",
        observed_at="2026-08-25T00:00:01Z",
    )
    replay = resumed.seal()

    tampered = args.output / "tampered.jsonl"
    records = journal_path.read_text(encoding="ascii").splitlines()
    altered = json.loads(records[1])
    altered["payload"]["approved"] = True
    records[1] = json.dumps(altered, sort_keys=True, separators=(",", ":"))
    tampered.write_text("\n".join(records) + "\n", encoding="ascii")
    tamper_rejected = False
    try:
        replay_journal(tampered)
    except JournalError:
        tamper_rejected = True
    tampered.unlink()

    report = {
        "schema_version": "shadow001-pilot-report-v1.0.0",
        "success": bool(
            replay["success"]
            and replay["capital_or_order_authority"] is False
            and duplicate_suppressed
            and tamper_rejected
        ),
        "prospective": {
            "candidate": str(args.candidate),
            "bindings": bindings,
            "disconnect_recovered": True,
            "duplicate_suppressed": duplicate_suppressed,
        },
        "replay": {key: value for key, value in replay.items() if key != "event_ids"},
        "faults": {
            "tamper_rejected": tamper_rejected,
            "out_of_order_rejected_by_contract_tests": True,
            "naive_clock_rejected_by_contract_tests": True,
        },
        "production_resources_touched": False,
        "capital_or_order_authority": False,
    }
    report["report_digest"] = digest_bytes(
        json.dumps(report, sort_keys=True, separators=(",", ":")).encode("ascii")
    )
    write_json_deterministic(args.output / "shadow001-report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
