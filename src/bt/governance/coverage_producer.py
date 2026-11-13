"""Produce eligible research clocks from admitted bytes, never model assertions."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import numpy as np
import pandas as pd

from bt.experiments.adaptive_representation import materialize_adaptive_representation
from bt.data.resample import timeframe_minutes
from bt.governance.data_requirements import coverage_receipt, fingerprint, instant


def produce_coverage(
    *,
    plan: dict,
    window: dict,
    receipts: list[dict],
    required_fields: dict[str, list[str]],
    maximum_source_rows: int = 2_000_000,
) -> dict:
    """Read only content-addressed recovery copies within the authorized window.

    These are predictor-availability clocks, not evidence of event support or
    profitability. Final DATA registry bindings are attached by the control plane.
    """
    start, end = instant(window["start"]), instant(window["end"])
    if start >= end:
        raise ValueError("coverage window must be nonempty")
    symbols = plan["instruments"]
    if type(maximum_source_rows) is not int or maximum_source_rows < 1:
        raise ValueError("coverage needs a positive source-row resource budget")
    if (end - start).total_seconds() / 60 * len(symbols) > maximum_source_rows:
        raise ValueError(
            "coverage source-row resource budget exceeded before reading panels"
        )
    if set(required_fields) != set(symbols) or len(receipts) != len(symbols):
        raise ValueError("coverage requires the exact selected basket")
    panels, identities = {}, {}
    for receipt in receipts:
        result = receipt["result"]
        symbol = result["instrument"]
        if symbol not in symbols or symbol in panels:
            raise ValueError("coverage receipt basket mismatch")
        if (
            result.get("admitted") is not True
            or not result.get("checks")
            or any(value is not True for value in result["checks"].values())
        ):
            raise ValueError("coverage requires successful native admission")
        recovery = result["recovery_copy"]
        uri = recovery["uri"]
        if (
            not uri.startswith("file://")
            or recovery.get("integrity_verified") is not True
        ):
            raise ValueError("coverage requires an immutable local recovery copy")
        path = Path(uri.removeprefix("file://"))
        if path.is_symlink():
            raise ValueError("coverage recovery copy must not be a symlink")
        digest = sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        if (
            digest.hexdigest() != result["panel_sha256"]
            or recovery["content_digest"] != digest.hexdigest()
        ):
            raise ValueError("coverage recovery bytes differ from admission")
        fields = sorted(
            set(required_fields[symbol])
            | {"ts", "symbol", "open", "high", "low", "close", "volume"}
        )
        if set(fields) - {
            "ts",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
        }:
            raise ValueError(
                "coverage needs a field-specific availability verifier for non-OHLCV fields"
            )
        panel = pd.read_parquet(
            path, columns=fields, filters=[("ts", ">=", start), ("ts", "<", end)]
        )
        timestamps = pd.to_datetime(panel["ts"], utc=True, errors="raise")
        if not timestamps.is_monotonic_increasing or timestamps.duplicated().any():
            raise ValueError("coverage source clock must be unique and ordered")
        if not timestamps.eq(timestamps.dt.floor("min")).all():
            raise ValueError("coverage requires exact minute boundaries")
        numeric = [field for field in fields if field not in {"ts", "symbol"}]
        values = panel[numeric].apply(pd.to_numeric, errors="coerce")
        valid = np.isfinite(values.to_numpy()).all(axis=1)
        valid &= (panel[["open", "high", "low", "close"]] > 0).all(axis=1)
        valid &= panel["volume"].ge(0)
        valid &= panel["high"].ge(panel[["open", "close", "low"]].max(axis=1))
        valid &= panel["low"].le(panel[["open", "close"]].min(axis=1))
        if "quote_volume" in panel:
            valid &= panel["quote_volume"].ge(0)
        panels[symbol] = panel.loc[valid].copy()
        identities[symbol] = {
            "panel_digest": result["panel_sha256"],
            "admission_digest": fingerprint(receipt),
            "required_fields": required_fields[symbol],
            "source_rows": len(panel),
            "eligible_source_rows": int(valid.sum()),
        }
    materialized = materialize_adaptive_representation(plan, panels)
    fields = materialized.receipt["output_fields"]
    frame = materialized.frame
    eligible = np.isfinite(frame[fields].to_numpy(dtype=float)).all(axis=1)
    eligible &= frame["decision_at"].ge(pd.Timestamp(start))
    eligible &= frame["decision_at"].lt(pd.Timestamp(end))
    decisions = (
        frame.loc[eligible, "decision_at"].map(lambda value: value.isoformat()).tolist()
    )
    evidence = {
        "schema_version": "admitted-research-clock-v1",
        "representation_digest": fingerprint(plan),
        "window": window,
        "assets": identities,
        "representation_receipt": materialized.receipt,
        "eligible_decisions_count": len(decisions),
        "point_in_time_membership_verified": False,
        "claim_boundary": "OHLCV predictor availability only; membership and reviewed requirements remain separate gates",
        "capital_or_order_authority": False,
    }
    # The same clock is used for every member: basket coverage is an intersection.
    intervals = coverage_receipt(
        instrument=symbols[0],
        binding={},
        bar_minutes=timeframe_minutes(plan["research_timeframe"]),
        eligible_decisions=decisions,
        eligibility_evidence_digest=fingerprint(evidence),
    )["intervals"]
    evidence["intervals"] = intervals
    evidence["research_bar_minutes"] = timeframe_minutes(plan["research_timeframe"])
    return evidence | {"evidence_digest": fingerprint(evidence)}
