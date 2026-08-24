"""Bounded, deterministic BT-009 walking-skeleton qualification run."""
from __future__ import annotations

import hashlib
import json
import math
import platform
from pathlib import Path
import subprocess
from typing import Any
import uuid

import pandas as pd
import yaml  # type: ignore[import-untyped]

from bt.execution.model_registry import declared_classic_bundle
from bt.experiments.hypothesis_runner import execute_hypothesis_variant
from bt.experiments.representation_contract import (
    EvaluationSplit,
    FieldContract,
    RepresentationContract,
    certify_representation_frame,
)
from bt.experiments.search_plan import SearchBudget, StoppingRule, compile_hypothesis_search_plan
from bt.governance.research_bridge import BridgeError, materialize_approved_contract
from bt.hypotheses.contract import HypothesisContract
from bt.logging.run_bundle import finalize_run_bundle
from bt.validation.experiment_truth import validate_experiment_root, write_truth_report


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(value if isinstance(value, bytes) else _canonical(value)).hexdigest()


def build_qualification_snapshot(root: Path, *, rows: int = 720) -> dict[str, Any]:
    """Create one causal synthetic perpetual panel; no performance is implied."""
    if rows < 600:
        raise BridgeError("qualification snapshot requires at least 600 one-minute rows")
    root.mkdir(parents=True, exist_ok=True)
    path = root / "BTCUSDT.parquet"
    manifest_path = root / "manifest.yaml"
    if path.exists() or manifest_path.exists():
        raise BridgeError("qualification snapshot destination must be empty")
    timestamps = pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC")
    price = 100.0
    records: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        shock = 4.0 if index % 120 == 90 else (-3.5 if index % 120 == 30 else 0.0)
        drift = 0.015 * math.sin(index / 13)
        open_price = price
        close = max(1.0, open_price + drift + shock)
        records.append({
            "ts": timestamp, "symbol": "BTCUSDT", "open": open_price,
            "high": max(open_price, close) + (0.8 if shock else 0.15),
            "low": min(open_price, close) - (0.8 if shock else 0.15),
            "close": close, "volume": 1000 + index % 100 + (1500 if shock else 0),
            "mark_close": close * 1.001, "index_close": close,
            "basis_close_vs_index": 0.001,
            "funding_rate": 0.0001 + (index % 120) / 1_000_000,
            "funding_available_at": timestamp,
            "open_interest": 1_000_000 + index * 100 + (50_000 if shock else 0),
            "oi_available_at": timestamp,
        })
        price = close
    frame = pd.DataFrame(records)
    frame.to_parquet(path, index=False)
    manifest = {"format": "per_symbol_parquet", "symbols": ["BTCUSDT"], "path": "{symbol}.parquet"}
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    digest = _digest(path.read_bytes())
    snapshot_id = str(uuid.uuid5(uuid.UUID("fdfc386c-7360-4197-ad69-07948630bccb"), digest))
    receipt = {
        "schema_version": "bt009-qualification-snapshot-v1.0.0",
        "snapshot_id": snapshot_id,
        "content_digest": digest,
        "rows": rows,
        "start": timestamps[0].isoformat(),
        "end": timestamps[-1].isoformat(),
        "fields": sorted(frame.columns),
        "purpose": "contract qualification only; no market-performance claim",
    }
    (root / "snapshot.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt


def _representation(snapshot: dict[str, Any], repository_commit: str, code_digest: str, frame: pd.DataFrame):
    decisions = pd.to_datetime(frame["ts"], utc=True)
    boundary_one = decisions.iloc[len(decisions) // 2]
    boundary_two = decisions.iloc[(len(decisions) * 3) // 4]
    audit_frame = frame.assign(
        decision_at=decisions,
        membership_known_at=decisions.iloc[0],
        membership_valid_from=decisions.iloc[0],
        close_feature=frame["close"],
        observed_at=decisions,
        available_at=decisions,
    )
    contract = RepresentationContract(
        contract_id="bt009-csi-qualification-v1",
        dataset_snapshot_id=snapshot["snapshot_id"],
        dataset_digest=snapshot["content_digest"],
        repository_commit=repository_commit,
        code_digest=code_digest,
        decision_time_column="decision_at",
        entity_columns=("symbol",),
        membership_known_at_column="membership_known_at",
        membership_valid_from_column="membership_valid_from",
        membership_valid_to_column=None,
        fields=(FieldContract(
            name="close_feature", kind="feature", source_columns=("close",),
            transformation="bt009:identity-close", transformation_version="1.0.0",
            implementation_digest=_digest(b"bt009:identity-close:v1"),
            observation_time_column="observed_at", availability_time_column="available_at",
            warmup_observations=0, missing_policy="error", fit_policy="stateless",
        ),),
        split=EvaluationSplit(
            train_start=decisions.iloc[0].isoformat(), train_end=boundary_one.isoformat(),
            validation_start=decisions.iloc[len(decisions) // 2 + 1].isoformat(), validation_end=boundary_two.isoformat(),
            test_start=decisions.iloc[(len(decisions) * 3) // 4 + 1].isoformat(), test_end=decisions.iloc[-1].isoformat(),
            fit_start=decisions.iloc[0].isoformat(), fit_end=boundary_one.isoformat(),
            purge_seconds=0, embargo_seconds=0,
        ),
    )
    return contract, certify_representation_frame(contract, audit_frame)


def execute_qualification(
    *, proposal: dict[str, Any], repository_root: Path, data_root: Path,
    output_root: Path, config_path: Path,
) -> dict[str, Any]:
    if output_root.exists():
        raise BridgeError("qualification output root must not already exist")
    output_root.mkdir(parents=True)
    snapshot = json.loads((data_root / "snapshot.json").read_text(encoding="utf-8"))
    actual_dataset_digest = _digest((data_root / "BTCUSDT.parquet").read_bytes())
    if actual_dataset_digest != snapshot["content_digest"] or actual_dataset_digest != proposal["dataset"]["digest"]:
        raise BridgeError("qualification dataset differs from the approved proposal")
    repository_commit = subprocess.run(
        ["git", "-C", str(repository_root), "rev-parse", "HEAD"], check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    if repository_commit != proposal["resolution"]["repository_commit"]:
        raise BridgeError("repository changed after approval")
    code_digest = _digest(repository_commit.encode())
    approved_path = output_root / "approved-hypothesis.yaml"
    contract_receipt = materialize_approved_contract(
        proposal, repository_root=repository_root, output=approved_path,
    )
    contract = HypothesisContract.from_yaml(approved_path)
    frame = pd.read_parquet(data_root / "BTCUSDT.parquet")
    representation, leakage_report = _representation(snapshot, repository_commit, code_digest, frame)
    model = declared_classic_bundle(
        profile="tier2",
        parameters={"taker_fee_bps": 6.0, "slippage_bps": 2.0, "spread_bps": 1.0, "delay_bars": 1},
    )
    search = compile_hypothesis_search_plan(
        contract=contract,
        family_id=f"bt009-csi-{proposal['proposal_digest'][:12]}",
        hypothesis_digest=contract_receipt["content_digest"],
        dataset_snapshot_id=snapshot["snapshot_id"],
        dataset_digest=snapshot["content_digest"],
        repository_commit=repository_commit,
        code_digest=code_digest,
        market_model_bundle_digest=model.digest,
        representation_contract_digest=representation.digest,
        tiers=("Tier2",), seeds=(7,), resources={"max_workers": 1},
        budget=SearchBudget(16, 1, 3600, 1), stopping_rule=StoppingRule(kind="exhaustive"),
    )
    search_document = search.document()
    (output_root / "search-plan.json").write_text(json.dumps(search_document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    experiment_root = output_root / "experiment"
    runs_root = experiment_root / "runs"
    runs_root.mkdir(parents=True)
    variants = contract.to_run_specs()
    trials = search.trials()
    if len(variants) != 16 or len(trials) != 16:
        raise BridgeError("qualification must contain exactly 16 registered variants")
    bundles: list[dict[str, Any]] = []
    for index, (spec, trial) in enumerate(zip(variants, trials, strict=True), start=1):
        run_slug = f"row_{index:05d}"
        result = execute_hypothesis_variant(
            contract=contract, spec=spec, tier="Tier2",
            config_path=str(config_path), data_path=str(data_root),
            out_root=str(runs_root), run_slug=run_slug, phase="tier2b",
        )
        run_dir = Path(result["run_dir"])
        (run_dir / "market_model_bundle.json").write_text(json.dumps(model.document(), sort_keys=True) + "\n", encoding="utf-8")
        (run_dir / "representation_contract.json").write_text(json.dumps(representation.document(), sort_keys=True) + "\n", encoding="utf-8")
        (run_dir / "representation_leakage_report.json").write_text(json.dumps(leakage_report, sort_keys=True) + "\n", encoding="utf-8")
        (run_dir / "search_plan.json").write_text(json.dumps(search_document, sort_keys=True) + "\n", encoding="utf-8")
        bundles.append({"run_dir": str(run_dir), "trial": trial, "result": result})
    truth = validate_experiment_root(experiment_root)
    write_truth_report(truth, experiment_root / "summaries")
    if truth.status != "PASS":
        raise BridgeError(f"native truth validation failed with {truth.hard_failures} hard failures")
    bundle_root = output_root / "run-bundles"
    environment_digest = _digest({"python": platform.python_version(), "platform": platform.platform()})
    finalized: list[dict[str, Any]] = []
    for item in bundles:
        trial = item["trial"]
        receipt = finalize_run_bundle(
            Path(item["run_dir"]), bundle_root,
            lineage={
                "repository_commit": repository_commit, "code_digest": code_digest,
                "dataset_snapshot_id": snapshot["snapshot_id"], "dataset_digest": snapshot["content_digest"],
                "specification_digest": proposal["proposal_digest"], "environment_digest": environment_digest,
                "market_model_bundle_digest": model.digest,
                "representation_contract_digest": representation.digest,
                "search_plan_digest": search.digest, "search_family_id": search.family_id,
                "trial_id": trial["trial_id"], "attempt": 1,
            },
        )
        finalized.append({"trial": trial, "bundle": receipt, "result": item["result"]})
    receipt = {
        "schema_version": "bt009-qualification-execution-v1.0.0",
        "proposal_digest": proposal["proposal_digest"],
        "repository_commit": repository_commit,
        "dataset_digest": snapshot["content_digest"],
        "search_plan_digest": search.digest,
        "representation_contract_digest": representation.digest,
        "market_model_bundle_digest": model.digest,
        "truth": truth.to_dict(),
        "runs": finalized,
        "production_eligible": False,
    }
    (output_root / "qualification-receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt
