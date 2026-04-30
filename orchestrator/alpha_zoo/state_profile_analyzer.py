from __future__ import annotations
import json
from pathlib import Path


def enrich_state_profiles(candidates: list[dict], findings_dir: Path) -> list[dict]:
    indexed = {}
    for p in findings_dir.glob("*_state_findings.json"):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        findings = payload.get("findings") if isinstance(payload, dict) else None
        if isinstance(findings, list):
            indexed[p.stem.replace("_state_findings","")] = findings
    for c in candidates:
        name = c["identity"].get("hypothesis_name")
        rows = indexed.get(name, [])
        pos = [f"{r.get('state_variable')}__{r.get('bucket')}" for r in rows if (r.get("ev_r_net") or 0) > 0]
        neg = [f"{r.get('state_variable')}__{r.get('bucket')}" for r in rows if (r.get("ev_r_net") or 0) < 0]
        sp = c.setdefault("state_profile", {})
        sp["positive_state_conditions"] = pos[:8]
        sp["avoid_state_conditions"] = neg[:8]
        sp.setdefault("best_state_buckets", pos[:5])
        sp.setdefault("worst_state_buckets", neg[:5])
        sp["state_profile_status"] = "ok" if rows else "under_instrumented"
        sp.setdefault("setup_class", "unknown")
    return candidates
