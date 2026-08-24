#!/usr/bin/env python3
"""Create the explicit classic-engine market-model artifact for a run."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.execution.model_registry import declared_classic_bundle


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("tier1", "tier2", "tier3", "custom"), required=True)
    parser.add_argument("--taker-fee-bps", type=float, required=True)
    parser.add_argument("--slippage-bps", type=float, required=True)
    parser.add_argument("--spread-bps", type=float, required=True)
    parser.add_argument("--delay-bars", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    bundle = declared_classic_bundle(
        profile=args.profile,
        parameters={
            "taker_fee_bps": args.taker_fee_bps,
            "slippage_bps": args.slippage_bps,
            "spread_bps": args.spread_bps,
            "delay_bars": args.delay_bars,
        },
    ).document()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "bundle_digest": bundle["bundle_digest"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
