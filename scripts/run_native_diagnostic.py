"""Read one bounded JSON request from stdin; emit a diagnostic, never approval."""
import json
from pathlib import Path
import subprocess
import sys

from bt.governance.diagnostic_tools import run_diagnostic


def main():
    root = Path(__file__).resolve().parents[1]
    source = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(["git", "-C", str(root), "status", "--porcelain", "--untracked-files=all",
        "--", "src", "scripts/run_native_diagnostic.py"], text=True)
    if dirty:
        raise ValueError("native diagnostic requires a clean pinned source tree")
    raw = sys.stdin.buffer.read(262145)
    if len(raw) > 262144:
        raise ValueError("diagnostic input exceeds bound")
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result
    envelope = json.loads(raw, object_pairs_hook=unique_pairs)
    if set(envelope) != {"request", "evidence_catalog"}:
        raise ValueError("invalid diagnostic envelope")
    receipt = run_diagnostic(envelope["request"], runtime_source_commit=source,
                             evidence_catalog=envelope["evidence_catalog"])
    print(json.dumps(receipt, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
