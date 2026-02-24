"""CLI entrypoint for v1 backtests."""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import yaml

from bt.api import run_backtest
from bt.audit.gate import evaluate_gate
from bt.config import load_yaml
from bt.logging.artifacts_manifest import write_artifacts_manifest
from bt.logging.cli_footer import print_run_footer
from bt.logging.run_contract import validate_run_artifacts
from bt.logging.run_manifest import write_run_manifest
from bt.logging.summary import write_summary_txt


def main() -> None:
    parser = argparse.ArgumentParser(description="Run backtest (v1).")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--data", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--override", action="append", default=[])
    parser.add_argument("--local-config")
    parser.add_argument("--audit-enabled", action="store_true")
    parser.add_argument("--audit-level", choices=("basic", "full"))
    parser.add_argument("--audit-gate", action="store_true")
    parser.add_argument("--audit-required", help="Comma-separated required audit layers")
    args = parser.parse_args()

    override_paths = list(args.override)
    if args.local_config:
        override_paths.append(args.local_config)

    required_layers = [layer.strip() for layer in (args.audit_required or "").split(",") if layer.strip()]
    audit_override_path: Path | None = None
    if args.audit_enabled or args.audit_level or required_layers:
        payload: dict[str, object] = {"audit": {}}
        audit_cfg = payload["audit"]
        assert isinstance(audit_cfg, dict)
        if args.audit_enabled:
            audit_cfg["enabled"] = True
        if args.audit_level:
            audit_cfg["level"] = args.audit_level
        if required_layers:
            audit_cfg["required_layers"] = required_layers
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            yaml.safe_dump(payload, tmp, sort_keys=True)
            audit_override_path = Path(tmp.name)
        override_paths.append(str(audit_override_path))

    resolved_run_dir: Path | None = None
    config: dict | None = None
    try:
        run_dir = run_backtest(
            config_path=args.config,
            data_path=args.data,
            out_dir="outputs/runs",
            override_paths=override_paths or None,
            run_name=args.run_id,
        )
        resolved_run_dir = Path(run_dir)

        validate_run_artifacts(resolved_run_dir)

        config_path = resolved_run_dir / "config_used.yaml"
        try:
            loaded_config = load_yaml(config_path)
        except Exception as exc:  # pragma: no cover - defensive user-facing guard
            raise ValueError(f"Unable to read config_used.yaml from run_dir={run_dir}: {exc}") from exc
        if not isinstance(loaded_config, dict):
            raise ValueError(f"Invalid config_used.yaml format in run_dir={run_dir}; expected mapping.")

        config = loaded_config
        write_summary_txt(resolved_run_dir)
        write_run_manifest(resolved_run_dir, config=config, data_path=args.data)
        write_artifacts_manifest(resolved_run_dir, config=config)
        print_run_footer(resolved_run_dir)

        if args.audit_gate:
            code, details = evaluate_gate(
                resolved_run_dir,
                required_override=required_layers if required_layers else None,
                strict=True,
            )
            if code == 0:
                print("PASS: 0 violations, coverage OK")
            elif code == 2:
                print(f"FAIL: violations in layers: {details.get('violating_layers', [])}")
            elif code == 3:
                print(f"FAIL: missing required layers: {details.get('missing_required_layers', [])}")
            else:
                print("FAIL: coverage.json missing or unreadable")
            if code != 0:
                raise SystemExit(code)
    finally:
        if audit_override_path is not None and audit_override_path.exists():
            audit_override_path.unlink()
        if resolved_run_dir is not None and config is not None:
            write_artifacts_manifest(resolved_run_dir, config=config)


if __name__ == "__main__":
    main()
