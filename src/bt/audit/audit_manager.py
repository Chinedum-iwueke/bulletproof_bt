from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bt.logging.formatting import write_json_deterministic


@dataclass(frozen=True)
class AuditContext:
    run_id: str
    config_hash: str


class AuditManager:
    ALL_LAYERS = (
        "order_audit",
        "fill_audit",
        "position_audit",
        "portfolio_audit",
        "alignment_audit",
        "resample_audit",
        "signal_audit",
        "order_normalization_check",
    )

    def __init__(self, *, run_dir: Path, config: dict[str, Any], run_id: str) -> None:
        audit_cfg = config.get("audit") if isinstance(config.get("audit"), dict) else {}
        data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
        self.enabled = bool(audit_cfg.get("enabled", False))
        self.level = str(audit_cfg.get("level", "basic"))
        self.max_events = int(audit_cfg.get("max_events_per_file", 5000))
        self.audit_dir = run_dir / "audit"
        self._counts: dict[str, int] = {}
        self._violations: dict[str, int] = {}
        self._executed_layers: set[str] = set()
        self._skipped_layers: dict[str, str] = {}
        required_layers_raw = audit_cfg.get("required_layers", [])
        if isinstance(required_layers_raw, list):
            self.required_layers = [str(layer) for layer in required_layers_raw if str(layer)]
        else:
            self.required_layers = []
        self._resampling_active = bool(data_cfg.get("engine_timeframe") or data_cfg.get("timeframe")) if isinstance(data_cfg, dict) else False
        if self.enabled:
            config_hash = hashlib.sha256(
                json.dumps(config, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            self.audit_dir.mkdir(parents=True, exist_ok=True)
        else:
            config_hash = ""
        self._context = AuditContext(run_id=run_id, config_hash=config_hash)

    @property
    def context(self) -> AuditContext:
        return self._context

    def record_event(self, name: str, payload: dict[str, Any], *, violation: bool = False) -> None:
        if not self.enabled:
            return
        self.mark_layer_executed(name)
        current = self._counts.get(name, 0)
        self._counts[name] = current + 1
        if violation:
            self._violations[name] = self._violations.get(name, 0) + 1
        if current >= self.max_events:
            return
        line = {
            "run_id": self._context.run_id,
            "config_hash": self._context.config_hash,
            **payload,
            "violation": violation,
        }
        path = self.audit_dir / f"{name}.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(line, default=str, sort_keys=True) + "\n")

    def write_json(self, name: str, payload: dict[str, Any]) -> None:
        if not self.enabled:
            return
        write_json_deterministic(self.audit_dir / name, payload)

    def mark_layer_executed(self, layer: str) -> None:
        if not self.enabled:
            return
        self._executed_layers.add(layer)
        self._skipped_layers.pop(layer, None)

    def mark_layer_skipped(self, layer: str, reason: str) -> None:
        if not self.enabled or layer in self._executed_layers:
            return
        self._skipped_layers[layer] = reason

    def _build_skipped_layers(self) -> dict[str, str]:
        skipped = dict(self._skipped_layers)
        for layer in self.ALL_LAYERS:
            if layer in self._executed_layers or layer in skipped:
                continue
            if layer == "resample_audit" and not self._resampling_active:
                skipped[layer] = "no_resampling_active"
            elif layer in {"alignment_audit", "signal_audit"} and self.level != "full":
                skipped[layer] = "level_not_full_or_not_triggered"
            else:
                skipped[layer] = "not_configured_or_not_hooked_for_this_run"
        return skipped

    def write_coverage_json(self) -> None:
        if not self.enabled:
            return
        executed_layers = sorted(self._executed_layers)
        skipped_layers = self._build_skipped_layers()
        missing_required = [layer for layer in self.required_layers if layer not in self._executed_layers]
        has_violations = any(count > 0 for count in self._violations.values())
        status = "fail" if has_violations or missing_required else "pass"
        coverage = {
            "run_id": self._context.run_id,
            "enabled": self.enabled,
            "level": self.level,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "expected_layers": list(self.ALL_LAYERS),
            "required_layers": list(self.required_layers),
            "executed_layers": executed_layers,
            "skipped_layers": skipped_layers,
            "event_counts": {layer: int(self._counts.get(layer, 0)) for layer in self.ALL_LAYERS},
            "violations": {layer: int(self._violations.get(layer, 0)) for layer in self.ALL_LAYERS},
            "status": status,
        }
        self.write_json("coverage.json", coverage)

    def write_summary(self) -> None:
        if not self.enabled:
            return
        self.write_coverage_json()
        summary = {
            "run_id": self._context.run_id,
            "counts": self._counts,
            "violations": self._violations,
            "status_by_layer": {
                key: ("fail" if self._violations.get(key, 0) > 0 else "pass")
                for key in self._counts
            },
        }
        self.write_json("stability_report.json", summary)
        print("Stability Report")
        for key in sorted(summary["status_by_layer"]):
            print(f"- {key}: {summary['status_by_layer'][key]}")
