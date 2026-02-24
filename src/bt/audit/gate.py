from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

EXIT_OK = 0
EXIT_VIOLATIONS = 2
EXIT_MISSING_REQUIRED = 3
EXIT_COVERAGE_ERROR = 4


def _parse_required(required_raw: str | None) -> list[str]:
    if not required_raw:
        return []
    return [item.strip() for item in required_raw.split(",") if item.strip()]


def evaluate_gate(run_dir: Path, *, required_override: list[str] | None = None, strict: bool = True) -> tuple[int, dict[str, Any]]:
    coverage_path = run_dir / "audit" / "coverage.json"
    source = "coverage"
    payload: dict[str, Any] | None = None

    if coverage_path.exists():
        try:
            loaded = json.loads(coverage_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = loaded
        except (json.JSONDecodeError, OSError):
            payload = None
    elif not strict:
        stability_path = run_dir / "audit" / "stability_report.json"
        if stability_path.exists():
            loaded = json.loads(stability_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = {
                    "violations": loaded.get("violations", {}),
                    "executed_layers": list((loaded.get("counts") or {}).keys()),
                    "required_layers": [],
                }
                source = "stability_report"

    if payload is None:
        return EXIT_COVERAGE_ERROR, {
            "status": "fail",
            "reason": "coverage_missing_or_unreadable",
            "run_dir": str(run_dir),
            "source": source,
        }

    required_layers = required_override if required_override is not None else payload.get("required_layers", [])
    required_layers = [str(layer) for layer in required_layers]

    executed_layers_raw = payload.get("executed_layers", [])
    executed_layers = {str(layer) for layer in executed_layers_raw}

    violations_map = payload.get("violations", {})
    violating_layers = sorted(
        str(layer)
        for layer, count in violations_map.items()
        if int(count) > 0
    )

    missing_required = sorted(layer for layer in required_layers if layer not in executed_layers)

    if violating_layers:
        return EXIT_VIOLATIONS, {
            "status": "fail",
            "reason": "violations",
            "violating_layers": violating_layers,
            "missing_required_layers": missing_required,
            "source": source,
        }

    if missing_required:
        return EXIT_MISSING_REQUIRED, {
            "status": "fail",
            "reason": "missing_required_layers",
            "violating_layers": violating_layers,
            "missing_required_layers": missing_required,
            "source": source,
        }

    return EXIT_OK, {
        "status": "pass",
        "reason": "ok",
        "violating_layers": [],
        "missing_required_layers": [],
        "source": source,
    }


def _print_text_summary(code: int, details: dict[str, Any]) -> None:
    if code == EXIT_OK:
        print("PASS: 0 violations, coverage OK")
    elif code == EXIT_VIOLATIONS:
        print(f"FAIL: violations in layers: {details.get('violating_layers', [])}")
    elif code == EXIT_MISSING_REQUIRED:
        print(f"FAIL: missing required layers: {details.get('missing_required_layers', [])}")
    else:
        print("FAIL: coverage.json missing or unreadable")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit stability gate")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--required", help="Comma-separated required layers override")
    parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    parser.add_argument("--no-strict", dest="strict", action="store_false")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    code, details = evaluate_gate(
        Path(args.run_dir),
        required_override=_parse_required(args.required) if args.required is not None else None,
        strict=bool(args.strict),
    )
    if args.as_json:
        print(json.dumps(details, sort_keys=True))
    else:
        _print_text_summary(code, details)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
