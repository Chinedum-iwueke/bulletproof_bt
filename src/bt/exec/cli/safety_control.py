from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from bt.institutional.runtime_safety import (
    contain_runtime,
    initialize_safety_state,
    load_safety_state,
    recover_runtime,
    verify_safety_journal,
)


def _paths(args: argparse.Namespace) -> tuple[str, str]:
    state = args.state or os.environ.get("INVARIANCE_EXECUTION_SAFETY_STATE", "")
    journal = args.journal or os.environ.get("INVARIANCE_EXECUTION_SAFETY_JOURNAL", "")
    if not state or not journal:
        raise ValueError("state and journal paths are required")
    return state, journal


def _json(path: str) -> dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="EXEC-007 local fail-closed runtime control"
    )
    parser.add_argument(
        "command", choices=["init", "status", "audit", "freeze", "kill", "recover"]
    )
    parser.add_argument("--state")
    parser.add_argument("--journal")
    parser.add_argument("--actor", default=os.environ.get("USER", "operator"))
    parser.add_argument("--reason", default="operator requested runtime containment")
    parser.add_argument(
        "--request-id", default=f"operator-{datetime.now(UTC).isoformat()}"
    )
    parser.add_argument("--exec004-receipt")
    parser.add_argument("--authority-record")
    parser.add_argument("--maximum-authority-age-seconds", type=float, default=900.0)
    args = parser.parse_args(argv)
    state, journal = _paths(args)
    now = datetime.now(UTC)
    if args.command == "init":
        result: object = initialize_safety_state(
            state_path=state, journal_path=journal, actor=args.actor, now=now
        )
    elif args.command == "status":
        result = load_safety_state(state)
    elif args.command == "audit":
        result = {
            "state": load_safety_state(state),
            "events": verify_safety_journal(journal),
        }
    elif args.command in {"freeze", "kill"}:
        result = contain_runtime(
            state_path=state,
            journal_path=journal,
            action=args.command,
            request_id=args.request_id,
            actor=args.actor,
            reason=args.reason,
            now=now,
        )
    else:
        if not args.exec004_receipt or not args.authority_record:
            parser.error("recover requires --exec004-receipt and --authority-record")
        result = recover_runtime(
            state_path=state,
            journal_path=journal,
            request_id=args.request_id,
            actor=args.actor,
            reason=args.reason,
            now=now,
            exec004_receipt=_json(args.exec004_receipt),
            authority_record=_json(args.authority_record),
            maximum_authority_age_seconds=args.maximum_authority_age_seconds,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
