#!/usr/bin/env python3
"""Execute one immutable Hermes ALPHA-002 assignment in Bulletproof."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import shutil
import sqlite3
import subprocess
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.data.resample import timeframe_minutes
from bt.evaluation.alpha_research import (
    complete_timeframe_bars,
    held_out_trade_evaluation,
    impact_proxy_evaluation,
    required_trade_logging_evaluation,
    trade_decision_timestamps,
)
from bt.execution.model_registry import declared_classic_bundle
from bt.experiments.hypothesis_runner import execute_hypothesis_variant
from bt.experiments.adaptive_representation import (
    materialize_adaptive_representation,
)
from bt.experiments.representation_contract import (
    EvaluationSplit,
    FieldContract,
    RepresentationContract,
    certify_representation_frame,
)
from bt.experiments.search_plan import (
    SearchBudget,
    StoppingRule,
    compile_hypothesis_search_plan,
)
from bt.governance.research_bridge import (
    BridgeError,
    DatasetBinding,
    HypothesisSubmission,
    compile_submission,
    materialize_approved_contract,
)
from bt.governance.alpha_strategy_pipeline import (
    canonical_hash,
    complete_independent_review,
    confirm_card,
    draft_research_card,
    governed_review_verified,
    qualify_card,
)
from bt.hypotheses.contract import HypothesisContract
from bt.logging.run_bundle import finalize_run_bundle
from bt.validation.experiment_truth import validate_experiment_root, write_truth_report
from bt.strategy.btc_funding_basis_crowding_60m import (
    funding_basis_matched_evaluation,
)
from bt.strategy.eth_liquidity_displacement_btc_residual_60m import (
    liquidity_displacement_grid_evaluation,
)

AUTHORITY = {
    "capital": False,
    "orders": False,
    "promotion": False,
    "self_approval": False,
}
_EXPLICIT_HYPOTHESIS = re.compile(
    r"(?:hypothesis(?:_id)?|strategy)\s*[:=]\s*([A-Za-z0-9][A-Za-z0-9._-]*)",
    re.IGNORECASE,
)


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def digest(value: Any) -> str:
    return hashlib.sha256(
        value if isinstance(value, bytes) else canonical(value)
    ).hexdigest()


def file_digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def materialize_execution_panel(
    assignment: dict[str, Any],
    output: Path,
    *,
    warmup_bars: int = 0,
    warmup_timeframe: str = "1m",
) -> tuple[Path, str, dict[str, pd.DataFrame]]:
    """Bind every admitted basket member into one immutable engine panel."""
    bindings = assignment.get("dataset_bindings") or [
        {
            "dataset_build_id": assignment["dataset_build_id"],
            "dataset_digest": assignment["dataset_digest"],
            "dataset_path": assignment["dataset_path"],
            "dataset_key": assignment.get("dataset_key", "legacy-single-panel"),
            "instrument": assignment["instrument"],
            "venue": assignment.get("venue"),
        }
    ]
    start = (
        pd.Timestamp(assignment["window_start"])
        if assignment.get("window_start")
        else None
    )
    end = (
        pd.Timestamp(assignment["window_end"]) if assignment.get("window_end") else None
    )
    warmup_start = start
    if start is not None and warmup_bars:
        warmup_start = start - (
            pd.Timedelta(minutes=timeframe_minutes(warmup_timeframe)) * warmup_bars
        )
    frames: list[pd.DataFrame] = []
    panels: dict[str, pd.DataFrame] = {}
    for binding in bindings:
        path = Path(binding["dataset_path"])
        filters = None
        if warmup_start is not None and end is not None:
            filters = [
                ("ts", ">=", warmup_start.to_pydatetime()),
                ("ts", "<", end.to_pydatetime()),
            ]
        selected = pd.read_parquet(path, filters=filters)
        if selected.empty:
            raise BridgeError(
                f"immutable execution window contains no rows for {binding['instrument']}"
            )
        instrument = binding["instrument"]
        if "symbol" not in selected:
            selected["symbol"] = instrument
        elif set(selected["symbol"].astype(str)) != {instrument}:
            raise BridgeError("basket panel symbol differs from its admitted binding")
        panels[instrument] = selected.copy()
        frames.append(selected)
    combined = pd.concat(frames, ignore_index=True).sort_values(
        ["ts", "symbol"], kind="stable"
    )
    if combined.duplicated(["ts", "symbol"]).any():
        raise BridgeError("combined basket contains duplicate symbol timestamps")
    destination = output / "execution-window.parquet"
    combined.to_parquet(destination, index=False)
    aggregate_digest = digest(
        {
            "dataset_bindings": [
                {
                    "dataset_build_id": item["dataset_build_id"],
                    "dataset_digest": item["dataset_digest"],
                    "instrument": item["instrument"],
                    "venue": item.get("venue"),
                }
                for item in bindings
            ],
            "window_start": assignment.get("window_start"),
            "window_end": assignment.get("window_end"),
            "warmup_start": warmup_start.isoformat() if warmup_start is not None else None,
            "warmup_bars": warmup_bars,
            "warmup_timeframe": warmup_timeframe,
            "materialized_file_digest": file_digest(destination),
        }
    )
    return destination, aggregate_digest, panels


def attach_adaptive_features(
    execution_data_path: Path,
    materialized: Any,
    *,
    output: Path,
    declared_fields: list[str],
) -> Path:
    """Expose reviewed adaptive fields to strategies only at causal decision times."""
    output_fields = materialized.receipt["output_fields"]
    if declared_fields != output_fields:
        raise BridgeError(
            "strategy adaptive_representation_fields differ from the frozen plan"
        )
    frame = pd.read_parquet(execution_data_path)
    provenance_fields = {
        "representation_plan_digest",
        "representation_output_fields",
        "representation_decision_ts",
        "prior_only_volatility_source_end_ts",
    }
    overlap = (set(output_fields) | provenance_fields) & set(frame.columns)
    if overlap:
        raise BridgeError(
            f"adaptive fields collide with source data: {sorted(overlap)}"
        )
    features = materialized.frame[["decision_at", *output_fields]].rename(
        columns={"decision_at": "ts"}
    )
    features["ts"] = pd.to_datetime(features["ts"], utc=True, errors="raise")
    # The frozen ETH->BTC experiment requires a one-completed-bar lag.  Bind
    # that semantic here instead of relabelling compiler output that includes
    # the signal bar as "prior-only".
    prior_volatility = "btc_15m_realized_volatility_96"
    if prior_volatility in output_fields:
        features[prior_volatility] = features[prior_volatility].shift(1)
        features["prior_only_volatility_source_end_ts"] = (
            features["ts"] - pd.Timedelta(minutes=15)
        ).map(lambda value: value.isoformat())
    plan_digest = materialized.receipt.get("plan_digest")
    if plan_digest is not None:
        features["representation_plan_digest"] = plan_digest
        features["representation_output_fields"] = json.dumps(
            output_fields, separators=(",", ":")
        )
        features["representation_decision_ts"] = features["ts"].map(
            lambda value: value.isoformat()
        )
    if features["ts"].duplicated().any():
        raise BridgeError("adaptive representation has duplicate decision timestamps")
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="raise")
    enriched = frame.merge(features, on="ts", how="left", validate="many_to_one")
    destination = output / "execution-panel-with-adaptive-features.parquet"
    enriched.to_parquet(destination, index=False)
    return destination


def causal_warmup_receipt(
    materialized: Any,
    *,
    official_start: Any,
    warmup_bars: int,
    warmup_timeframe: str,
) -> dict[str, Any]:
    """Prove enough complete pre-evaluation decisions exist to seed online state."""
    start = pd.Timestamp(official_start)
    warmup_rows = int(
        (
            pd.to_datetime(
                materialized.frame["decision_at"], utc=True, errors="raise"
            )
            <= start
        ).sum()
    )
    if warmup_rows < warmup_bars:
        raise BridgeError(
            "materialized representation lacks the declared causal warmup: "
            f"{warmup_rows} < {warmup_bars} complete {warmup_timeframe} bars"
        )
    receipt = {
        "schema_version": "alpha-causal-warmup-receipt-v1.0.0",
        "authority": "causal_feature_state_only",
        "official_evaluation_start": start.isoformat(),
        "warmup_start": (
            start
            - pd.Timedelta(minutes=timeframe_minutes(warmup_timeframe)) * warmup_bars
        ).isoformat(),
        "warmup_timeframe": warmup_timeframe,
        "required_complete_bars": warmup_bars,
        "materialized_complete_bars": warmup_rows,
        "orders_permitted_during_warmup": False,
        "performance_attribution_during_warmup": False,
    }
    receipt["record_digest"] = digest(receipt)
    return receipt


def _execution_identity(assignment: dict[str, Any]) -> dict[str, Any]:
    return {
        "campaign_digest": assignment["campaign_digest"],
        "question_digest": assignment["question_digest"],
        "dataset_digest": assignment["dataset_digest"],
        "source_commit": assignment["base_ref"],
        "execution_class": assignment.get("execution_class", "qualification"),
        "window_start": assignment.get("window_start"),
        "window_end": assignment.get("window_end"),
        "qualification_digest": digest(assignment.get("qualification")),
        "dataset_bindings_digest": digest(assignment.get("dataset_bindings", [])),
        "representation_plan_digest": digest(assignment.get("representation_plan")),
    }


def prepare_execution_output(
    output: Path, assignment: dict[str, Any]
) -> dict[str, Any] | None:
    """Recover an interrupted immutable attempt or return its completed result."""
    identity = _execution_identity(assignment)
    identity_digest = digest(identity)
    marker_name = "execution-identity.json"
    completion_name = "execution-result.json"
    if output.exists():
        marker_path = output / marker_name
        if not marker_path.is_file():
            raise BridgeError(
                "execution output exists without an immutable identity marker"
            )
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        if marker.get("identity_digest") != identity_digest:
            raise BridgeError("execution output is bound to a different assignment")
        completion_path = output / completion_name
        if completion_path.is_file():
            envelope = json.loads(completion_path.read_text(encoding="utf-8"))
            expected = digest(
                {
                    "identity_digest": identity_digest,
                    "result": envelope.get("result"),
                }
            )
            if envelope.get("record_digest") != expected:
                raise BridgeError("completed execution result digest is invalid")
            return envelope["result"]
        counter = 1
        while True:
            partial = output.with_name(
                f".{output.name}.partial-{identity_digest[:12]}-{counter:04d}"
            )
            if not partial.exists():
                output.rename(partial)
                break
            counter += 1
    output.mkdir(parents=True, exist_ok=False)
    marker = {
        "schema_version": "alpha-execution-identity-v1.0.0",
        "identity": identity,
        "identity_digest": identity_digest,
    }
    marker["record_digest"] = digest(marker)
    (output / marker_name).write_bytes(canonical(marker) + b"\n")
    return None


def finalize_execution_output(
    output: Path, assignment: dict[str, Any], result: dict[str, Any]
) -> None:
    identity_digest = digest(_execution_identity(assignment))
    document = {"identity_digest": identity_digest, "result": result}
    envelope = {**document, "record_digest": digest(document)}
    temporary = output / ".execution-result.json.new"
    temporary.write_bytes(canonical(envelope) + b"\n")
    temporary.replace(output / "execution-result.json")


def downstream_reuse_manifest(
    run_dir: Path,
    *,
    assignment: dict[str, Any],
    representation_digest: str,
    search_plan_digest: str,
    evaluation_artifact: str,
    variant_index: int,
    selected_for_holdout: bool,
) -> dict[str, Any]:
    artifact_names = [
        "decisions.jsonl",
        "fills.jsonl",
        "trades.csv",
        "equity.csv",
        "performance.json",
        "representation_contract.json",
        "representation_leakage_report.json",
        "adaptive-representation-receipt.json",
        "search_plan.json",
        "selection_bias_audit.json",
        evaluation_artifact,
        "required_trade_logging_evaluation.json",
    ]
    artifacts = {
        name: file_digest(run_dir / name)
        for name in artifact_names
        if (run_dir / name).is_file()
    }
    document = {
        "schema_version": "alpha-downstream-reuse-v1.0.0",
        "dataset_digest": assignment["dataset_digest"],
        "question_digest": assignment["question_digest"],
        "source_commit": assignment["base_ref"],
        "representation_contract_digest": representation_digest,
        "search_plan_digest": search_plan_digest,
        "variant_index": variant_index,
        "selected_for_holdout": selected_for_holdout,
        "artifacts": artifacts,
        "readiness": {
            "institutional_learning": {
                "state": "ready",
                "uses": ["novelty", "failure_memory", "abductive_replenishment"],
            },
            "ml002": {
                "state": "materialization_candidate",
                "uses": ["causal_features", "labels", "purged_embargoed_splits"],
                "required_before_training": [
                    "DISC-003 producer receipt",
                    "independent materialization rebuild",
                ],
            },
            "rl001": {
                "state": "not_ready",
                "reason": (
                    "Backtests do not supply sealed shadow behavior propensities, "
                    "action support, or independently rebuilt rewards."
                ),
                "required_before_use": ["SHADOW-001 receipt", "RL-001 receipt"],
            },
        },
        "authority": AUTHORITY,
    }
    document["record_digest"] = digest(document)
    return document


def hypothesis_identity(question: str) -> str:
    match = _EXPLICIT_HYPOTHESIS.search(question)
    return match.group(1) if match else question.strip()


def question_card(assignment: dict[str, Any], *, disposition: str) -> dict[str, Any]:
    context = assignment["research_context"]
    return {
        "schema_version": "alpha002-hypothesis-card-v1.0.0",
        "question": assignment["question"],
        "question_digest": assignment["question_digest"],
        "domain_key": assignment["domain_key"],
        "claim": assignment["question"],
        "dataset": {
            "build_id": assignment["dataset_build_id"],
            "digest": assignment["dataset_digest"],
            "instrument": assignment["instrument"],
            "timeframe": assignment["timeframe"],
            "research_timeframe": assignment.get("research_timeframe", "1m"),
            "resampling_policy": assignment.get(
                "resampling_policy", "right_closed_left_labeled_complete_bars"
            ),
            "instruments": assignment.get("instruments", [assignment["instrument"]]),
            "bindings": assignment.get("dataset_bindings", []),
        },
        "research_context": context,
        "disposition": disposition,
        "falsification": (
            "Execute only a prospectively registered finite grid and retain the full "
            "result when net performance, robustness, or truth gates fail."
        ),
        "authority": AUTHORITY,
    }


def base_attempt(
    assignment: dict[str, Any], *, hypothesis_id: str, hypothesis_digest: str
) -> dict[str, Any]:
    return {
        "attempt_key": (
            f"alpha002-{assignment['source_candidate_id'][:8]}-"
            f"{assignment['dataset_digest'][:12]}"
        ),
        "expected_campaign_digest": assignment["campaign_digest"],
        "question": assignment["question"],
        "question_digest": assignment["question_digest"],
        "source_candidate_id": assignment["source_candidate_id"],
        "source_candidate_digest": assignment["source_candidate_digest"],
        "hypothesis_id": hypothesis_id[:180],
        "hypothesis_digest": hypothesis_digest,
        "dataset_build_id": assignment["dataset_build_id"],
        "dataset_digest": assignment["dataset_digest"],
        "governed_bridge_id": None,
        "produced_by": "bulletproof_bt",
        "source_commit": assignment["base_ref"],
    }


def engineering_required(
    assignment: dict[str, Any], output: Path, reason: str
) -> dict[str, Any]:
    card = question_card(
        assignment, disposition="bounded_strategy_engineering_required"
    )
    artifact = {
        **card,
        "reason": reason,
        "required_work": [
            "author an exact hypothesis contract and strategy implementation",
            "declare a finite prospective parameter grid and required logging",
            "pass causal, unit, integration, truth and independent-review gates",
            "obtain digest-bound approval before native execution",
        ],
    }
    output.mkdir(parents=True, exist_ok=True)
    path = output / "strategy-engineering-requirement.json"
    path.write_bytes(canonical(artifact) + b"\n")
    evidence_digest = file_digest(path)
    attempt = base_attempt(
        assignment,
        hypothesis_id=f"ENGINEERING-{assignment['question_digest'][:16]}",
        hypothesis_digest=digest(card),
    ) | {
        "trial_count": 0,
        "outcome": "failed",
        "failure_stage": "strategy_generation",
        "gate_report": {
            "truth_certified": False,
            "point_in_time_valid": False,
            "reproducible": False,
            "out_of_sample_evaluated": False,
            "cost_stress_evaluated": False,
            "selection_bias_audited": False,
            "independent_review_complete": False,
            "shadow_eligible": False,
            "production_eligible": False,
            "capital_authority": False,
            "failed_gates": ["exact_registered_strategy_unavailable"],
        },
        "evidence_digests": [evidence_digest],
    }
    return {
        "disposition": "engineering_required",
        "hypothesis_card": card,
        "alpha_campaign_attempt": attempt,
        "evidence": {"engineering_requirement_digest": evidence_digest},
    }


def record_alpha_memory(
    database: Path, *, assignment: dict[str, Any], bundle: dict[str, Any]
) -> dict[str, Any]:
    """Precommit one immutable native result to Bulletproof research memory."""
    database.parent.mkdir(parents=True, exist_ok=True)
    publication_key = f"alpha002:{bundle['bundle_digest']}"
    document = {
        "publication_key": publication_key,
        "bundle_digest": bundle["bundle_digest"],
        "campaign_digest": assignment["campaign_digest"],
        "question_digest": assignment["question_digest"],
        "dataset_digest": assignment["dataset_digest"],
    }
    record_digest = digest(document)
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_research_publications (
                publication_key TEXT PRIMARY KEY,
                bundle_digest TEXT NOT NULL UNIQUE,
                record_json TEXT NOT NULL,
                record_digest TEXT NOT NULL UNIQUE
            )
            """
        )
        existing = connection.execute(
            "SELECT record_digest FROM alpha_research_publications "
            "WHERE publication_key = ?",
            (publication_key,),
        ).fetchone()
        if existing is not None and existing[0] != record_digest:
            raise BridgeError("alpha research memory identity is immutable")
        disposition = "existing" if existing is not None else "created"
        if existing is None:
            connection.execute(
                "INSERT INTO alpha_research_publications VALUES (?, ?, ?, ?)",
                (
                    publication_key,
                    bundle["bundle_digest"],
                    canonical(document).decode("ascii"),
                    record_digest,
                ),
            )
            connection.commit()
        return {
            "schema_version": "bulletproof-memory-publication-receipt-v1.0.0",
            "bundle_digest": bundle["bundle_digest"],
            "memory_database_digest": record_digest,
            "publication_key": publication_key,
            "disposition": disposition,
        }
    finally:
        connection.close()


def retain_bundle(source: Path, root: Path, bundle: dict[str, Any]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    destination = root / bundle["bundle_digest"]
    if destination.exists():
        manifest = json.loads(
            (destination / "run_bundle_manifest.json").read_text(encoding="utf-8")
        )
        if manifest.get("manifest_digest") != bundle["manifest_digest"]:
            raise BridgeError("durable bundle identity is immutable")
        return destination
    temporary = root / f".{bundle['bundle_digest']}.new"
    if temporary.exists():
        shutil.rmtree(temporary)
    shutil.copytree(source, temporary)
    temporary.rename(destination)
    return destination


def available_fields(path: Path) -> tuple[str, ...]:
    columns = set(pd.read_parquet(path, engine="pyarrow", columns=[]).columns)
    # PyArrow returns no names for an empty projection; inspect metadata without loading rows.
    if not columns:
        import pyarrow.parquet as pq

        columns = set(pq.ParquetFile(path).schema_arrow.names)
    fields = (
        {"ohlcv"} if {"open", "high", "low", "close", "volume"} <= columns else set()
    )
    if "funding_rate" in columns:
        fields.add("funding")
    if "open_interest" in columns:
        fields.add("open_interest")
    if "volume" in columns:
        fields.add("volume")
    return tuple(sorted(fields))


def representation(
    assignment: dict[str, Any],
    frame: pd.DataFrame,
    code_digest: str,
    *,
    purge_seconds: int = 60,
    embargo_seconds: int = 60,
    decision_timeframe: str = "1m",
) -> tuple[RepresentationContract, dict[str, Any]]:
    ordered = frame.copy()
    ordered["ts"] = pd.to_datetime(ordered["ts"], utc=True, errors="raise")
    ordered = ordered.sort_values(["ts", "symbol"], kind="stable").reset_index(
        drop=True
    )
    if ordered.duplicated(["symbol", "ts"]).any():
        raise BridgeError("representation rejects duplicate instrument timestamps")
    # Source rows are left-labeled and become observable after their interval.
    # Split boundaries use decision opportunities, never entry fills.
    if decision_timeframe == "1m":
        decisions = ordered["ts"] + pd.Timedelta(minutes=1)
        audit_rows = ordered
        audit_decisions = decisions
    else:
        complete_rows = complete_timeframe_bars(ordered, decision_timeframe)
        if complete_rows.empty:
            raise BridgeError("representation has no complete decision rows")
        interval = pd.Timedelta(minutes=timeframe_minutes(decision_timeframe))
        # The registered split contract is a fraction of actual decision rows.
        # Missing or incomplete buckets are not decision opportunities and must
        # not enter the train/validation/test denominator.
        decisions = (
            complete_rows["ts"].drop_duplicates().sort_values().reset_index(drop=True)
            + interval
        )
        audit_rows = complete_rows
        audit_decisions = complete_rows["ts"] + interval
    first, last = decisions.iloc[0], decisions.iloc[-1]
    split_one = decisions.iloc[len(decisions) * 6 // 10]
    split_two = decisions.iloc[len(decisions) * 8 // 10]
    validation_index = decisions.searchsorted(
        split_one + pd.Timedelta(seconds=purge_seconds), side="right"
    )
    test_index = decisions.searchsorted(
        split_two + pd.Timedelta(seconds=embargo_seconds), side="right"
    )
    if validation_index >= len(decisions) or test_index >= len(decisions):
        raise BridgeError(
            "evaluation window is too short for its purge/embargo contract"
        )
    audit = audit_rows.assign(
        decision_at=audit_decisions,
        membership_known_at=first,
        membership_valid_from=first,
        close_feature=audit_rows["close"],
        observed_at=audit_decisions,
        available_at=audit_decisions,
    )
    contract = RepresentationContract(
        contract_id=f"alpha002-{assignment['question_digest'][:16]}",
        dataset_snapshot_id=assignment["dataset_build_id"],
        dataset_digest=assignment["dataset_digest"],
        repository_commit=assignment["base_ref"],
        code_digest=code_digest,
        decision_time_column="decision_at",
        entity_columns=("symbol",),
        membership_known_at_column="membership_known_at",
        membership_valid_from_column="membership_valid_from",
        membership_valid_to_column=None,
        fields=(
            FieldContract(
                name="close_feature",
                kind="feature",
                source_columns=("close",),
                transformation="alpha002:identity-close",
                transformation_version="1.0.0",
                implementation_digest=digest(b"alpha002:identity-close:v1"),
                observation_time_column="observed_at",
                availability_time_column="available_at",
                warmup_observations=0,
                missing_policy="error",
                fit_policy="stateless",
            ),
        ),
        split=EvaluationSplit(
            train_start=first.isoformat(),
            train_end=split_one.isoformat(),
            validation_start=decisions.iloc[validation_index].isoformat(),
            validation_end=split_two.isoformat(),
            test_start=decisions.iloc[test_index].isoformat(),
            test_end=last.isoformat(),
            fit_start=first.isoformat(),
            fit_end=split_one.isoformat(),
            purge_seconds=purge_seconds,
            embargo_seconds=embargo_seconds,
        ),
    )
    return contract, certify_representation_frame(contract, audit)


def period_evaluation(run_dir: Path, start: str, end: str) -> dict[str, Any]:
    trades_path = run_dir / "trades.csv"
    try:
        trades = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame()
    if trades.empty:
        return {
            "trade_count": 0,
            "mean_net_r": 0.0,
            "maximum_drawdown": 0.0,
        }
    decisions = trade_decision_timestamps(trades)
    sample = trades.loc[
        (decisions >= pd.Timestamp(start)) & (decisions <= pd.Timestamp(end))
    ]
    net_column = "r_net" if "r_net" in sample else "r_multiple_net"
    net = pd.to_numeric(sample[net_column], errors="coerce").dropna()
    cumulative = pd.concat(
        [pd.Series([0.0], dtype=float), net.reset_index(drop=True)],
        ignore_index=True,
    ).cumsum()
    maximum_drawdown = float((cumulative.cummax() - cumulative).max())
    return {
        "trade_count": int(len(net)),
        "mean_net_r": float(net.mean()) if len(net) else 0.0,
        "maximum_drawdown": maximum_drawdown,
    }


def select_funding_basis_variant(evaluations: list[dict[str, Any]]) -> int | None:
    """Select only among validation variants with adequate scientific support."""
    supported = [
        index
        for index, evaluation in enumerate(evaluations)
        if evaluation.get("outcome") in {"positive", "negative"}
    ]
    if not supported:
        return None
    return min(
        supported,
        key=lambda item: (
            evaluations[item]["treated_minus_control_mean"],
            -evaluations[item]["matched_support"],
            item,
        ),
    )


def heldout_not_evaluated_evidence(
    *,
    question: str,
    parameters: dict[str, Any],
    validation: list[dict[str, Any]],
    liquidity_grid: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Retain why validation did not authorize opening the held-out test."""
    if liquidity_grid is not None:
        outcome = str(liquidity_grid["outcome"])
        reason = (
            "no_valid_validation_variant"
            if outcome == "invalid"
            else "validation_edge_nonpositive_test_not_opened"
        )
    else:
        outcome = (
            "invalid"
            if all(item["outcome"] == "invalid" for item in validation)
            else "failed"
        )
        reason = (
            "no_valid_validation_variant"
            if outcome == "invalid"
            else "no_supported_validation_variant"
        )
    result = {
        "schema_version": "scientific-heldout-not-evaluated-v1.0.0",
        "question": question,
        "parameters": parameters,
        "outcome": outcome,
        "reason": reason,
        "evaluation_partition": "test",
        "held_out_evaluated": False,
        "decision_records": [],
        "pairs": [],
        "matched_support": 0,
        "treated_support": 0,
        "control_support": 0,
        "treated_minus_control_mean": 0.0,
        "confidence_interval_95": {"lower": 0.0, "upper": 0.0},
        "confidence_interval_method": "not_evaluated",
        "doubled_cost_treated_minus_control": 0.0,
        "directional_support": {
            "positive_trailing_return": 0,
            "nonpositive_trailing_return": 0,
        },
        "passed": False,
    }
    if liquidity_grid is not None:
        result.update(
            {
                "validation_directional_effect": max(
                    (
                        float(item.get("validation_directional_effect", 0.0))
                        for item in validation
                        if item.get("outcome") in {"positive", "negative"}
                    ),
                    default=0.0,
                ),
                "test_directional_effect": 0.0,
                "doubled_cost_directional_effect": 0.0,
                "extreme_support": 0,
                "maximum_drawdown": 0.0,
            }
        )
    result["record_digest"] = digest(result)
    return result


def heldout_test_consulted(
    *, is_liquidity_residual: bool, liquidity_grid: dict[str, Any] | None
) -> bool:
    """Report whether validation opened the single permitted held-out test."""
    return bool(
        is_liquidity_residual
        and liquidity_grid is not None
        and liquidity_grid.get("test_open_count") == 1
    )


def weekend_regime_comparison(
    frame: pd.DataFrame, *, lookback: int = 60
) -> dict[str, Any]:
    """Measure the registered predictive association without treating it as PnL."""
    ordered = frame.sort_values(["symbol", "ts"]).copy()
    grouped = ordered.groupby("symbol", sort=False)["close"]
    ordered["lagged_return"] = grouped.pct_change(lookback)
    ordered["next_return"] = grouped.transform(
        lambda series: series.pct_change().shift(-1)
    )
    ordered["signed_next_return"] = (
        ordered["lagged_return"].apply(lambda value: 1.0 if value > 0 else -1.0)
        * ordered["next_return"]
    )
    timestamps = pd.to_datetime(ordered["ts"], utc=True)
    ordered["regime"] = timestamps.dt.dayofweek.map(
        lambda day: "weekend" if day >= 5 else "weekday"
    )
    usable = ordered.dropna(subset=["lagged_return", "next_return"])
    groups = {}
    for regime in ("weekend", "weekday"):
        sample = usable.loc[usable["regime"] == regime, "signed_next_return"]
        groups[regime] = {
            "observations": int(len(sample)),
            "mean_signed_next_return": float(sample.mean()) if len(sample) else 0.0,
        }
    return {
        "schema_version": "alpha-weekend-regime-comparison-v1.0.0",
        "measurement": "causal predictive association; not executable PnL",
        "feature": f"trailing_{lookback}_bar_return_sign",
        "target": "next_bar_return",
        "groups": groups,
        "weekend_minus_weekday": (
            groups["weekend"]["mean_signed_next_return"]
            - groups["weekday"]["mean_signed_next_return"]
        ),
    }


def execute_variant_grid(
    jobs: list[dict[str, Any]], max_workers: int
) -> list[dict[str, Any]]:
    if not 1 <= len(jobs) <= 8 or not 1 <= max_workers <= 8:
        raise BridgeError("Alpha execution requires 1-8 variants and 1-8 worker slots")
    if max_workers == 1:
        return [_execute_variant_job(job) for job in jobs]
    with ProcessPoolExecutor(
        max_workers=min(max_workers, len(jobs)), mp_context=get_context("spawn")
    ) as pool:
        return list(pool.map(_execute_variant_job, jobs))


def _execute_variant_job(job: dict[str, Any]) -> dict[str, Any]:
    return execute_hypothesis_variant(**job)


def execution_scope(
    assignment: dict[str, Any], qualification: dict[str, Any] | None
) -> dict[str, Any]:
    """Validate whether this run can contribute qualification evidence."""
    execution_class = assignment.get("execution_class", "qualification")
    if execution_class not in {"qualification", "commissioning"}:
        raise BridgeError("unknown alpha execution class")
    if not isinstance(qualification, dict):
        if execution_class == "commissioning":
            raise BridgeError("commissioning requires a reviewed qualification")
        return {
            "execution_class": execution_class,
            "qualification_authority": True,
        }
    reviewed = qualification.get("window")
    if not isinstance(reviewed, dict):
        raise BridgeError("qualified strategy lacks its reviewed execution window")
    try:
        start = pd.Timestamp(assignment["window_start"])
        end = pd.Timestamp(assignment["window_end"])
        reviewed_start = pd.Timestamp(reviewed["start"])
        reviewed_end = pd.Timestamp(reviewed["end"])
    except (KeyError, TypeError, ValueError) as exc:
        raise BridgeError("execution window is incomplete or invalid") from exc
    if any(item.tzinfo is None for item in (start, end, reviewed_start, reviewed_end)):
        raise BridgeError("execution windows must include an explicit timezone")
    if end <= start:
        raise BridgeError("execution window must be positive")
    if execution_class == "qualification":
        if start != reviewed_start or end != reviewed_end:
            raise BridgeError("qualification execution window differs from review")
        return {
            "execution_class": execution_class,
            "qualification_authority": True,
        }
    if start < reviewed_start or end > reviewed_end:
        raise BridgeError("commissioning window is outside the reviewed window")
    if end - start > pd.Timedelta(days=31):
        raise BridgeError("commissioning window exceeds 31 days")
    if int(assignment.get("max_variants", 0)) > 8:
        raise BridgeError("commissioning variant budget exceeds eight")
    return {
        "execution_class": execution_class,
        "qualification_authority": False,
        "reviewed_window": reviewed,
        "commissioning_window": {
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    }


def independent_review_required(
    assignment: dict[str, Any], output: Path, reason: str
) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    evidence = {
        "schema_version": "alpha-independent-review-failure-v1.0.0",
        "campaign_digest": assignment["campaign_digest"],
        "question_digest": assignment["question_digest"],
        "source_commit": assignment["base_ref"],
        "reason": reason,
        "trial_count": 0,
        "authority": AUTHORITY,
    }
    path = output / "independent-review-failure.json"
    path.write_bytes(canonical(evidence) + b"\n")
    attempt = base_attempt(
        assignment,
        hypothesis_id=f"REVIEW-{assignment['question_digest'][:16]}",
        hypothesis_digest=digest(assignment.get("qualification", {})),
    ) | {
        "trial_count": 0,
        "outcome": "failed",
        "failure_stage": "independent_evaluation",
        "gate_report": {
            **{
                key: False
                for key in (
                    "truth_certified",
                    "point_in_time_valid",
                    "reproducible",
                    "out_of_sample_evaluated",
                    "cost_stress_evaluated",
                    "selection_bias_audited",
                    "independent_review_complete",
                    "shadow_eligible",
                    "production_eligible",
                    "capital_authority",
                )
            },
            "failed_gates": ["independent_specification_review"],
        },
        "evidence_digests": [file_digest(path)],
    }
    return {
        "disposition": "independent_review_required",
        "alpha_campaign_attempt": attempt,
    }


def execute_registered(
    assignment: dict[str, Any], repository: Path, output: Path, *, max_workers: int = 1
) -> dict[str, Any]:
    identity = hypothesis_identity(assignment["question"])
    source = next(
        (
            path
            for path in sorted((repository / "research/hypotheses").glob("*.yaml"))
            if identity.casefold()
            in {
                HypothesisContract.from_yaml(
                    path
                ).schema.metadata.hypothesis_id.casefold(),
                HypothesisContract.from_yaml(path).schema.metadata.title.casefold(),
                path.stem.casefold(),
            }
        ),
        None,
    )
    qualification = assignment.get("qualification")
    if source is None and not isinstance(qualification, dict):
        raise BridgeError(
            "hypothesis is not registered; bounded engineering generation is required"
        )
    if not governed_review_verified(assignment, qualification):
        raise BridgeError(
            "independent specification review is missing or unbound; no compute started"
        )
    qualification_card = qualification.get("artifact_bundle", {}).get("card", {})
    if qualification_card.get("independent_review_required", False):
        qualification = complete_independent_review(assignment, qualification)
    scope = execution_scope(assignment, qualification)
    completed = prepare_execution_output(output, assignment)
    if completed is not None:
        return completed
    if isinstance(qualification, dict):
        if qualification.get("qualified") is not True:
            raise BridgeError("unqualified strategy cannot execute")
        card = qualification["card"]
        if canonical_hash(card) != qualification["card_digest"]:
            raise BridgeError("qualified card digest changed before execution")
        contract_document = qualification["artifact_bundle"]["engine_hypothesis_yaml"]
        contract = HypothesisContract.from_dict(contract_document)
        contract_digest = digest(
            qualification["artifact_bundle"]["engine_hypothesis_yaml"]
        )
        proposal = {
            "proposal_digest": canonical_hash(
                qualification["artifact_bundle"]["strategy_spec"]
            ),
            "search": {"variant_count": qualification["variant_count"]},
        }
        contract_receipt = {"content_digest": contract_digest}
        (output / "hypothesis-card.json").write_bytes(canonical(card) + b"\n")
        (output / "approved-hypothesis.json").write_bytes(
            canonical(qualification["artifact_bundle"]["engine_hypothesis_yaml"])
            + b"\n"
        )
    else:
        contract_document = yaml.safe_load(source.read_text(encoding="utf-8"))
        registered = HypothesisContract.from_yaml(source)
        grid = {
            name: (values[0],)
            for name, values in registered.schema.parameter_grid.items()
        }
        proposal = compile_submission(
            HypothesisSubmission(
                original_text=assignment["question"],
                hypothesis=identity,
                tier=assignment["tier"],
                grid=grid,
                dataset=DatasetBinding(
                    snapshot_id=assignment["dataset_build_id"],
                    digest=assignment["dataset_digest"],
                    available_fields=available_fields(Path(assignment["dataset_path"])),
                    universe=assignment["instrument"],
                    timeframe=assignment["timeframe"],
                ),
            ),
            repository_root=repository,
            repository_commit=assignment["base_ref"],
            max_variants=assignment["max_variants"],
        )
        card = question_card(assignment, disposition="reuse_registered_strategy") | {
            "hypothesis_id": registered.schema.metadata.hypothesis_id,
            "hypothesis_title": registered.schema.metadata.title,
            "proposal_digest": proposal["proposal_digest"],
        }
        (output / "hypothesis-card.json").write_bytes(canonical(card) + b"\n")
        approved = proposal | {"state": "approved"}
        contract_path = output / "approved-hypothesis.yaml"
        contract_receipt = materialize_approved_contract(
            approved, repository_root=repository, output=contract_path
        )
        contract = HypothesisContract.from_yaml(contract_path)
    variants = contract.to_run_specs()
    if len(variants) > assignment["max_variants"]:
        raise BridgeError("qualified strategy exceeds the immutable variant budget")

    execution_semantics = contract.schema.execution_semantics
    warmup_bars = int(execution_semantics.get("warmup_observations", 0))
    warmup_timeframe = str(
        execution_semantics.get(
            "warmup_timeframe",
            execution_semantics.get("signal_timeframe", "1m"),
        )
    )
    execution_data_path, window_digest, source_panels = materialize_execution_panel(
        assignment,
        output,
        warmup_bars=warmup_bars,
        warmup_timeframe=warmup_timeframe,
    )
    adaptive_receipt = None
    if assignment.get("representation_plan") is not None:
        materialized = materialize_adaptive_representation(
            assignment["representation_plan"], source_panels
        )
        if warmup_bars:
            warmup_receipt = causal_warmup_receipt(
                materialized,
                official_start=assignment["window_start"],
                warmup_bars=warmup_bars,
                warmup_timeframe=warmup_timeframe,
            )
            (output / "causal-warmup-receipt.json").write_bytes(
                canonical(warmup_receipt) + b"\n"
            )
        materialized.frame.to_parquet(
            output / "adaptive-representation.parquet", index=False
        )
        adaptive_receipt = materialized.receipt
        (output / "adaptive-representation-receipt.json").write_bytes(
            canonical(adaptive_receipt) + b"\n"
        )
        declared_adaptive_fields = contract.schema.execution_semantics.get(
            "adaptive_representation_fields", []
        )
        if not isinstance(declared_adaptive_fields, list) or not all(
            isinstance(item, str) and item for item in declared_adaptive_fields
        ):
            raise BridgeError(
                "adaptive_representation_fields must be a non-empty string list"
            )
        execution_data_path = attach_adaptive_features(
            execution_data_path,
            materialized,
            output=output,
            declared_fields=declared_adaptive_fields,
        )

    lightweight_columns = ["ts", "symbol", "close"]
    for column in contract.schema.execution_semantics.get("required_extra_columns", []):
        if column not in lightweight_columns:
            lightweight_columns.append(column)
    lightweight = pd.read_parquet(execution_data_path, columns=lightweight_columns)
    if assignment.get("window_start") and assignment.get("window_end"):
        lightweight_ts = pd.to_datetime(lightweight["ts"], utc=True, errors="raise")
        lightweight = lightweight.loc[
            (lightweight_ts >= pd.Timestamp(assignment["window_start"]))
            & (lightweight_ts < pd.Timestamp(assignment["window_end"]))
        ].copy()
    code_digest = digest(assignment["base_ref"].encode())
    split_contract = contract_document.get("evaluation", {}).get("split", {})
    rep, leakage = representation(
        assignment,
        lightweight,
        code_digest,
        purge_seconds=int(split_contract.get("purge_seconds", 60)),
        embargo_seconds=int(split_contract.get("embargo_seconds", 60)),
        decision_timeframe=str(
            contract.schema.execution_semantics.get("signal_timeframe", "1m")
        ),
    )
    execution_delay_bars = int(contract_document.get("costs", {}).get("delay_bars", 1))
    model = declared_classic_bundle(
        profile="tier2",
        parameters={
            "taker_fee_bps": 6.0,
            "slippage_bps": 2.0,
            "spread_bps": 1.0,
            "delay_bars": execution_delay_bars,
        },
    )
    search = compile_hypothesis_search_plan(
        contract=contract,
        family_id=f"alpha002-{assignment['question_digest'][:12]}",
        hypothesis_digest=contract_receipt["content_digest"],
        dataset_snapshot_id=assignment["dataset_build_id"],
        dataset_digest=window_digest,
        repository_commit=assignment["base_ref"],
        code_digest=code_digest,
        market_model_bundle_digest=model.digest,
        representation_contract_digest=rep.digest,
        tiers=("Tier2",),
        seeds=(7,),
        resources={"max_workers": min(max_workers, len(variants))},
        budget=SearchBudget(
            len(variants), len(variants), 86400, min(max_workers, len(variants))
        ),
        stopping_rule=StoppingRule(kind="exhaustive"),
    )
    experiment = output / "experiment"
    runs = experiment / "runs"
    runs.mkdir(parents=True)
    phase = assignment["tier"].lower()
    results = []
    run_dirs = []
    required_extra_columns = contract.schema.execution_semantics.get(
        "required_extra_columns", []
    )
    if adaptive_receipt is not None:
        expected_prefix = adaptive_receipt["output_fields"]
        required_provenance = [
            "representation_plan_digest",
            "representation_output_fields",
            "representation_decision_ts",
        ]
        legacy_contract = required_extra_columns == expected_prefix
        provenance_contract = required_extra_columns[
            : len(expected_prefix)
        ] == expected_prefix and all(
            item in required_extra_columns for item in required_provenance
        )
        if not (legacy_contract or provenance_contract):
            raise BridgeError(
                "required_extra_columns must preserve ordered adaptive outputs and provenance"
            )
    execution_overrides: list[str] = []
    if warmup_bars:
        if not assignment.get("window_start") or not assignment.get("window_end"):
            raise BridgeError("causal warmup requires an immutable execution window")
        warmup_start = pd.Timestamp(assignment["window_start"]) - (
            pd.Timedelta(minutes=timeframe_minutes(warmup_timeframe)) * warmup_bars
        )
        warmup_override_path = output / "causal-warmup-window.yaml"
        warmup_override_path.write_text(
            yaml.safe_dump(
                {
                    "data": {
                        "date_range": {
                            "start": assignment["window_start"],
                            "end": assignment["window_end"],
                        },
                        "warmup_start": warmup_start.isoformat(),
                    }
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        execution_overrides.append(str(warmup_override_path))
    if required_extra_columns:
        if not isinstance(required_extra_columns, list) or not all(
            isinstance(column, str) and column for column in required_extra_columns
        ):
            raise BridgeError("required_extra_columns must be a non-empty string list")
        data_override_path = output / "required-data-columns.yaml"
        data_override_path.write_text(
            yaml.safe_dump(
                {"data": {"extra_columns": required_extra_columns}},
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        execution_overrides.append(str(data_override_path))
    is_funding_basis = (
        contract.schema.metadata.hypothesis_family == "funding_basis_matched_control"
    )
    is_liquidity_residual = (
        contract.schema.metadata.hypothesis_family
        == "cross_asset_liquidity_transmission"
    )
    grid_data_path = execution_data_path
    if is_funding_basis or is_liquidity_residual:
        validation_end = pd.Timestamp(rep.split.validation_end)
        validation_frame = pd.read_parquet(execution_data_path)
        validation_frame = validation_frame[
            pd.to_datetime(validation_frame["ts"], utc=True) <= validation_end
        ]
        if validation_frame.empty:
            raise BridgeError("scientific validation partition contains no rows")
        grid_data_path = output / "validation-window.parquet"
        validation_frame.to_parquet(grid_data_path, index=False)
    jobs = [
        dict(
            contract=contract,
            spec=spec,
            tier="Tier3" if assignment["tier"] == "Tier3" else "Tier2",
            config_path=str(repository / "configs/engine.yaml"),
            data_path=str(grid_data_path),
            out_root=str(runs),
            override_paths=execution_overrides,
            run_slug=f"row_{index:04d}",
            phase=phase,
        )
        for index, spec in enumerate(variants, start=1)
    ]
    for result in execute_variant_grid(jobs, max_workers):
        run_dir = Path(result["run_dir"])
        for name, document in (
            ("market_model_bundle.json", model.document()),
            ("representation_contract.json", rep.document()),
            ("representation_leakage_report.json", leakage),
            ("search_plan.json", search.document()),
        ):
            (run_dir / name).write_bytes(canonical(document) + b"\n")
        if adaptive_receipt is not None:
            (run_dir / "adaptive-representation-receipt.json").write_bytes(
                canonical(adaptive_receipt) + b"\n"
            )
        results.append(result)
        run_dirs.append(run_dir)
    trials = search.trials()
    liquidity_grid = None
    if is_liquidity_residual:
        liquidity_grid = liquidity_displacement_grid_evaluation(
            source_panels,
            parameter_grid=contract.schema.parameter_grid,
            evaluation_start=assignment["window_start"],
            evaluation_end=assignment["window_end"],
        )
        validation = liquidity_grid["selection_candidates"]
        for item in validation:
            item["record_digest"] = digest(item)
        selected_parameters = liquidity_grid.get("selected_parameters")
        selected_index = next(
            (
                index
                for index, variant in enumerate(variants)
                if variant["params"] == selected_parameters
            ),
            None,
        )
        selection_metric = "validation_directional_effect"
    elif is_funding_basis:
        validation = [
            funding_basis_matched_evaluation(
                lightweight,
                params=variant["params"],
                start=rep.split.validation_start,
                end=rep.split.validation_end,
            )
            for variant in variants
        ]
        for index, item in enumerate(validation):
            partition = period_evaluation(
                run_dirs[index],
                rep.split.validation_start,
                rep.split.validation_end,
            )
            item["evaluation_partition"] = "validation"
            item["maximum_drawdown"] = partition["maximum_drawdown"]
            item["maximum_drawdown_authority"] = "classic_engine_trade_log_partition_R"
            item["record_digest"] = digest(item)
        selected_index = select_funding_basis_variant(validation)
        selection_metric = "validation_treated_minus_control_mean"
    else:
        validation = [
            period_evaluation(
                path, rep.split.validation_start, rep.split.validation_end
            )
            for path in run_dirs
        ]
        selected_index = max(
            range(len(validation)),
            key=lambda item: (
                validation[item]["mean_net_r"],
                validation[item]["trade_count"],
                -item,
            ),
        )
        selection_metric = "mean_net_r"
    execution_index = (
        selected_index
        if selected_index is not None
        else max(
            range(len(validation)),
            key=lambda item: (validation[item].get("matched_support", 0), -item),
        )
    )
    if (is_funding_basis or is_liquidity_residual) and selected_index is not None:
        heldout_root = runs
        heldout_job = dict(
            contract=contract,
            spec=variants[selected_index],
            tier="Tier3" if assignment["tier"] == "Tier3" else "Tier2",
            config_path=str(repository / "configs/engine.yaml"),
            data_path=str(execution_data_path),
            out_root=str(heldout_root),
            override_paths=execution_overrides,
            run_slug=f"selected_{selected_index:04d}",
            phase=phase,
        )
        heldout_result = execute_variant_grid([heldout_job], 1)[0]
        heldout_run_dir = Path(heldout_result["run_dir"])
        for name, document in (
            ("market_model_bundle.json", model.document()),
            ("representation_contract.json", rep.document()),
            ("representation_leakage_report.json", leakage),
            ("search_plan.json", search.document()),
        ):
            (heldout_run_dir / name).write_bytes(canonical(document) + b"\n")
        if adaptive_receipt is not None:
            (heldout_run_dir / "adaptive-representation-receipt.json").write_bytes(
                canonical(adaptive_receipt) + b"\n"
            )
        results[selected_index] = heldout_result
        run_dirs[selected_index] = heldout_run_dir
    selection_audit = {
        "schema_version": "alpha-selection-bias-audit-v1.0.0",
        "declared_variant_count": len(variants),
        "evaluated_variant_count": len(validation),
        "selection_partition": "validation",
        "selection_metric": selection_metric,
        "held_out_test_consulted": heldout_test_consulted(
            is_liquidity_residual=is_liquidity_residual,
            liquidity_grid=liquidity_grid,
        ),
        "stopping_rule": "exhaustive",
        "selected_variant_index": selected_index,
        "validation_results": validation,
        "search_plan_digest": search.digest,
    }
    selection_audit["record_digest"] = digest(selection_audit)
    is_impact_proxy = (
        contract.schema.metadata.hypothesis_family == "impact_proxy_reversal"
    )
    per_variant_evaluations = (
        validation if (is_funding_basis or is_liquidity_residual) else []
    )
    heldout_evaluation = (
        funding_basis_matched_evaluation(
            lightweight,
            start=rep.split.test_start,
            params=variants[selected_index]["params"],
        )
        if is_funding_basis and selected_index is not None
        else liquidity_grid
        if is_liquidity_residual and selected_index is not None
        else None
    )
    if heldout_evaluation is not None:
        heldout_partition = period_evaluation(
            run_dirs[selected_index],
            rep.split.test_start,
            rep.split.test_end,
        )
        heldout_evaluation["evaluation_partition"] = "test"
        heldout_evaluation["maximum_drawdown"] = heldout_partition["maximum_drawdown"]
        heldout_evaluation["maximum_drawdown_authority"] = (
            "classic_engine_trade_log_partition_R"
        )
        heldout_evaluation["record_digest"] = digest(heldout_evaluation)
    unsupported_validation = None
    if (is_funding_basis or is_liquidity_residual) and selected_index is None:
        unsupported_validation = heldout_not_evaluated_evidence(
            question=contract_document["immutable_contract"]["question"],
            parameters=variants[execution_index]["params"],
            validation=validation,
            liquidity_grid=liquidity_grid if is_liquidity_residual else None,
        )
    evaluation_artifact = (
        impact_proxy_evaluation(
            lightweight,
            test_start=rep.split.test_start,
            params=variants[execution_index]["params"],
        )
        if is_impact_proxy
        else heldout_evaluation or unsupported_validation
        if (is_funding_basis or is_liquidity_residual)
        else weekend_regime_comparison(lightweight)
    )
    if "record_digest" not in evaluation_artifact:
        evaluation_artifact["record_digest"] = digest(evaluation_artifact)
    evaluation_artifact_name = (
        "impact_proxy_evaluation.json"
        if is_impact_proxy
        else "funding_basis_heldout_not_evaluated.json"
        if is_funding_basis and selected_index is None
        else "eth_liquidity_residual_heldout_not_evaluated.json"
        if is_liquidity_residual and selected_index is None
        else "eth_liquidity_residual_evaluation.json"
        if is_liquidity_residual
        else "funding_basis_matched_evaluation.json"
        if is_funding_basis
        else "weekend_regime_comparison.json"
    )
    validation_evaluation_name = (
        "eth_liquidity_residual_validation_evaluation.json"
        if is_liquidity_residual
        else "funding_basis_validation_evaluation.json"
    )
    logging_reports = [
        required_trade_logging_evaluation(path, card["logging_requirements"])
        for path in run_dirs
    ]
    failed_logging = [
        index for index, report in enumerate(logging_reports) if not report["passed"]
    ]
    if failed_logging:
        raise BridgeError(
            f"required trade logging is incomplete for variants: {failed_logging}"
        )
    for index, candidate_run in enumerate(run_dirs):
        (candidate_run / "selection_bias_audit.json").write_bytes(
            canonical(selection_audit) + b"\n"
        )
        if is_funding_basis or is_liquidity_residual:
            (candidate_run / validation_evaluation_name).write_bytes(
                canonical(per_variant_evaluations[index]) + b"\n"
            )
            if index == selected_index or (
                selected_index is None and index == execution_index
            ):
                (candidate_run / evaluation_artifact_name).write_bytes(
                    canonical(evaluation_artifact) + b"\n"
                )
        else:
            (candidate_run / evaluation_artifact_name).write_bytes(
                canonical(evaluation_artifact) + b"\n"
            )
    for candidate_run, logging_report in zip(run_dirs, logging_reports, strict=True):
        (candidate_run / "required_trade_logging_evaluation.json").write_bytes(
            canonical(logging_report) + b"\n"
        )
    for index, candidate_run in enumerate(run_dirs):
        reuse = downstream_reuse_manifest(
            candidate_run,
            assignment=assignment,
            representation_digest=rep.digest,
            search_plan_digest=search.digest,
            evaluation_artifact=(
                evaluation_artifact_name
                if not (is_funding_basis or is_liquidity_residual)
                or index == selected_index
                else validation_evaluation_name
            ),
            variant_index=index,
            selected_for_holdout=selected_index is not None and index == selected_index,
        )
        (candidate_run / "downstream_reuse_manifest.json").write_bytes(
            canonical(reuse) + b"\n"
        )
    truth = validate_experiment_root(experiment)
    write_truth_report(truth, experiment / "summaries")
    if truth.status != "PASS":
        raise BridgeError(f"native truth validation failed: {truth.hard_failures}")
    trial = trials[execution_index]
    result = results[execution_index]
    run_dir = run_dirs[execution_index]
    lineage = {
        "repository_commit": assignment["base_ref"],
        "code_digest": code_digest,
        "dataset_snapshot_id": assignment["dataset_build_id"],
        "dataset_digest": assignment["dataset_digest"],
        "specification_digest": proposal["proposal_digest"],
        "environment_digest": digest(
            {"python": platform.python_version(), "platform": platform.platform()}
        ),
        "market_model_bundle_digest": model.digest,
        "representation_contract_digest": rep.digest,
        "search_plan_digest": search.digest,
        "search_family_id": search.family_id,
        "trial_id": trial["trial_id"],
        "attempt": 1,
        "parent_dataset_digest": assignment["dataset_digest"],
        "execution_window_digest": window_digest,
    }
    bundles = []
    retained = []
    for index, candidate_run in enumerate(run_dirs):
        candidate_lineage = {**lineage, "trial_id": trials[index]["trial_id"]}
        item = finalize_run_bundle(
            candidate_run,
            output / f"run-bundles-{index:04d}",
            lineage=candidate_lineage,
        )
        bundles.append(item)
        retained.append(
            retain_bundle(
                output / f"run-bundles-{index:04d}" / "bundles" / item["bundle_digest"],
                Path(assignment["bundle_root"]),
                item,
            )
        )
    bundle = bundles[execution_index]
    retained_bundle = retained[execution_index]
    holdout = (
        None
        if (is_funding_basis or is_liquidity_residual) and selected_index is None
        else held_out_trade_evaluation(run_dir, rep.split.test_start)
    )
    logging_report = logging_reports[execution_index]
    manifest = json.loads(
        (retained_bundle / "run_bundle_manifest.json").read_text(encoding="utf-8")
    )
    memory_receipts = [
        record_alpha_memory(
            Path(assignment["memory_database"]), assignment=assignment, bundle=item
        )
        for item in bundles
    ]
    memory = memory_receipts[execution_index]
    metrics = {
        key: value
        for key, value in result.items()
        if isinstance(value, (int, float, bool))
    }
    metrics.update(
        {
            "declared_variant_count": len(variants),
            "selected_variant_index": selected_index,
            "execution_artifact_index": execution_index,
            "selection_basis": selection_metric,
        }
    )
    if holdout is not None:
        metrics.update(
            {
                "oos_trade_count": holdout["trade_count"],
                "oos_mean_net_r": holdout["mean_net_r"],
                "double_cost_oos_mean_net_r": holdout["double_cost_mean_net_r"],
            }
        )
    metrics["maximum_drawdown"] = float(result.get("max_drawdown_r", 0.0))
    if is_impact_proxy:
        metrics["direction_balance"] = evaluation_artifact["direction_balance"]
        metrics["matched_return_shock_control"] = evaluation_artifact[
            "matched_return_shock_control"
        ]
    elif is_funding_basis:
        metrics.update(
            {
                key: evaluation_artifact[key]
                for key in (
                    "matched_support",
                    "treated_support",
                    "control_support",
                    "treated_minus_control_mean",
                    "confidence_interval_95",
                    "doubled_cost_treated_minus_control",
                )
            }
        )
        metrics["maximum_drawdown"] = (
            evaluation_artifact["maximum_drawdown"]
            if selected_index is not None
            else validation[execution_index]["maximum_drawdown"]
        )
    elif is_liquidity_residual:
        metrics.update(
            {
                key: evaluation_artifact[key]
                for key in (
                    "validation_directional_effect",
                    "test_directional_effect",
                    "confidence_interval_95",
                    "doubled_cost_directional_effect",
                    "extreme_support",
                    "matched_support",
                    "maximum_drawdown",
                )
            }
        )
    else:
        metrics["selection_bias_audit"] = selection_audit
    required_metrics = tuple(contract_document.get("evaluation", {}).get("metrics", ()))
    missing_metrics = sorted(set(required_metrics) - set(metrics))
    if missing_metrics:
        raise BridgeError(
            f"declared evaluation metrics were not produced: {missing_metrics}"
        )
    started_at = datetime.fromtimestamp(run_dir.stat().st_mtime, tz=UTC)
    ended_at = datetime.now(UTC)
    independent_review_complete = governed_review_verified(assignment, qualification)
    passed_edge = bool(
        (
            (is_funding_basis or is_liquidity_residual)
            or (
                holdout is not None
                and holdout["adequate_support"]
                and holdout["positive_net_edge"]
                and holdout["cost_stress_passed"]
            )
        )
        and independent_review_complete
        and logging_report["passed"]
        and scope["qualification_authority"]
        and (
            not is_impact_proxy
            or (
                evaluation_artifact["matched_return_shock_control"][
                    "statistically_outperformed_control"
                ]
                and evaluation_artifact["direction_balance"][
                    "balanced_positive_reversal"
                ]
            )
        )
        and (
            not (is_funding_basis or is_liquidity_residual)
            or evaluation_artifact["passed"]
        )
    )
    classic_pnl_gates = (
        ()
        if (is_funding_basis or is_liquidity_residual)
        else (
            ("oos_trade_support", holdout["adequate_support"]),
            ("positive_oos_net_edge", holdout["positive_net_edge"]),
            ("double_cost_oos_edge", holdout["cost_stress_passed"]),
        )
    )
    failed_gates = [name for name, passed in classic_pnl_gates if not passed]
    if not independent_review_complete:
        failed_gates.append("independent_specification_review")
    if not logging_report["passed"]:
        failed_gates.append("required_trade_logging")
    if not scope["qualification_authority"]:
        failed_gates.append("commissioning_run_has_no_qualification_authority")
    if is_impact_proxy:
        if not evaluation_artifact["matched_return_shock_control"][
            "statistically_outperformed_control"
        ]:
            failed_gates.append("matched_return_shock_control_95pct_lower_bound")
        if not evaluation_artifact["direction_balance"]["balanced_positive_reversal"]:
            failed_gates.append("positive_reversal_in_both_directions")
    scientific_outcome = (
        evaluation_artifact["outcome"]
        if (is_funding_basis or is_liquidity_residual)
        else None
    )
    heldout_scientific_evaluated = bool(
        evaluation_artifact.get("held_out_evaluated", True)
    )
    if is_funding_basis or is_liquidity_residual:
        if scientific_outcome == "invalid":
            failed_gates.append("point_in_time_scientific_sample_invalid")
        elif scientific_outcome == "failed":
            failed_gates.append("matched_control_support_inadequate")
        elif is_funding_basis and not evaluation_artifact["passed"]:
            if evaluation_artifact["confidence_interval_95"]["upper"] >= 0:
                failed_gates.append("matched_funding_basis_95pct_upper_bound")
            if evaluation_artifact["doubled_cost_treated_minus_control"] >= 0:
                failed_gates.append("matched_funding_basis_double_cost_stress")
        elif is_liquidity_residual and not heldout_scientific_evaluated:
            failed_gates.append("validation_edge_nonpositive_test_not_opened")
        elif is_liquidity_residual and not evaluation_artifact["passed"]:
            if evaluation_artifact["confidence_interval_95"][0] <= 0:
                failed_gates.append("eth_btc_residual_95pct_lower_bound")
            if evaluation_artifact["doubled_cost_directional_effect"] <= 0:
                failed_gates.append("eth_btc_residual_double_cost_stress")
    scientific_valid = (
        not (is_funding_basis or is_liquidity_residual)
        or scientific_outcome != "invalid"
    )
    scientific_supported = not (is_funding_basis or is_liquidity_residual) or (
        heldout_scientific_evaluated and scientific_outcome in {"positive", "negative"}
    )
    evaluation_evidence_digests = (
        [
            *[item["record_digest"] for item in per_variant_evaluations],
            evaluation_artifact["record_digest"],
        ]
        if (is_funding_basis or is_liquidity_residual)
        else [evaluation_artifact["record_digest"]]
    )
    gate_report = {
        "truth_certified": scientific_valid,
        "point_in_time_valid": scientific_valid,
        "reproducible": True,
        "out_of_sample_evaluated": scientific_supported,
        "cost_stress_evaluated": scientific_supported,
        "selection_bias_audited": True,
        "required_trade_logging_complete": logging_report["passed"],
        "independent_review_complete": independent_review_complete,
        "shadow_eligible": False,
        "production_eligible": False,
        "capital_authority": False,
        "qualification_authority": scope["qualification_authority"],
        "execution_class": scope["execution_class"],
        "failed_gates": failed_gates,
    }
    attempt = base_attempt(
        assignment,
        hypothesis_id=contract.schema.metadata.hypothesis_id,
        hypothesis_digest=contract_receipt["content_digest"],
    ) | {
        "trial_count": len(variants),
        "outcome": (
            "failed"
            if not independent_review_complete
            else scientific_outcome
            if (is_funding_basis or is_liquidity_residual)
            else "candidate"
            if passed_edge
            else "negative"
        ),
        "failure_stage": (
            "independent_evaluation"
            if not independent_review_complete
            else "truth_gate"
            if (is_funding_basis or is_liquidity_residual)
            and scientific_outcome == "failed"
            else None
        ),
        "gate_report": gate_report,
        "evidence_digests": [
            *[item["bundle_digest"] for item in bundles],
            selection_audit["record_digest"],
            *evaluation_evidence_digests,
            logging_report["record_digest"],
            truth.to_dict().get("report_digest", digest(truth.to_dict())),
            *(
                [adaptive_receipt["receipt_digest"]]
                if adaptive_receipt is not None
                else []
            ),
        ],
    }
    proposal_source = {
        "question": assignment["question"],
        "question_digest": assignment["question_digest"],
        "source_candidate_id": assignment["source_candidate_id"],
        "source_candidate_digest": assignment["source_candidate_digest"],
        "research_context": assignment["research_context"],
    }
    proposal_without_digest = {
        "schema_version": "governed-research-bridge-v1.0.0",
        "authority": {
            "capital": "prohibited",
            "live_orders": "prohibited",
            "self_approval": "prohibited",
            "production_promotion": "prohibited",
        },
        "source": proposal_source,
        "resolution": {
            "hypothesis_id": contract.schema.metadata.hypothesis_id,
            "hypothesis_digest": contract_receipt["content_digest"],
            "strategy": contract.schema.entry["strategy"],
        },
        "dataset": {
            "dataset_build_id": assignment["dataset_build_id"],
            "dataset_digest": assignment["dataset_digest"],
            "execution_dataset_digest": window_digest,
            "instrument": assignment["instrument"],
            "instruments": assignment.get("instruments", [assignment["instrument"]]),
            "timeframe": assignment["timeframe"],
            "rows": len(lightweight),
            "start": pd.to_datetime(lightweight["ts"], utc=True).iloc[0].isoformat(),
            "end": pd.to_datetime(lightweight["ts"], utc=True).iloc[-1].isoformat(),
        },
        "search": {
            "stopping_rule": "exhaustive",
            "variant_count": len(variants),
            "max_variants": assignment["max_variants"],
            "search_plan_digest": search.digest,
            "selection_basis": selection_metric,
            "selected_variant_index": selected_index,
        },
        "required_gates": [
            "truth",
            "point_in_time",
            "reproducibility",
            "out_of_sample",
            "cost_stress",
            "selection_bias",
            "independent_review",
        ],
    }
    bridge_proposal = {
        **proposal_without_digest,
        "state": "awaiting_approval",
        "proposal_digest": digest(proposal_without_digest),
    }
    publication_envelope = {
        "schema_version": (
            "alpha003-publication-envelope-v1.0.0"
            if isinstance(qualification, dict)
            else "alpha002-publication-envelope-v1.0.0"
        ),
        "bridge_proposal": bridge_proposal,
        "hypothesis_card": card,
        "experiment": {
            "features": sorted(available_fields(Path(assignment["dataset_path"]))),
            "target": "net portfolio outcome under the registered strategy",
            "fees_bps": 6.0,
            "slippage_bps": 2.0,
            "delay_bars": execution_delay_bars,
            "sample_range": (
                f"{proposal_without_digest['dataset']['start']}.."
                f"{proposal_without_digest['dataset']['end']}"
            ),
        },
        "trial": {
            "trial_id": trial["trial_id"],
            "code_digest": code_digest,
            "metrics": metrics,
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "truth": truth.to_dict(),
            "bundle_digest": bundle["bundle_digest"],
            "bundle_manifest_digest": manifest["manifest_digest"],
            "market_model_bundle_digest": model.digest,
            "representation_contract_digest": rep.digest,
            "adaptive_representation_receipt": adaptive_receipt,
            "search_plan_digest": search.digest,
            "result_disposition": "accepted" if passed_edge else "rejected",
            "held_out_evaluation": holdout,
            "selection_bias_audit": selection_audit,
            "hypothesis_evaluation": evaluation_artifact,
            "required_trade_logging_evaluation": logging_report,
        },
        "memory_receipt": memory,
        "durable_bundle_path": str(retained_bundle),
        "producer_gate_report": gate_report,
        "execution_class": scope["execution_class"],
        "qualification_authority": scope["qualification_authority"],
    }
    result_document = {
        "disposition": "native_execution_complete",
        "hypothesis_card": card,
        "proposal": proposal,
        "truth": truth.to_dict(),
        "bundle": bundle,
        "metrics": metrics,
        "publication_envelope": publication_envelope,
        "adaptive_representation_receipt": adaptive_receipt,
    }
    if scope["qualification_authority"]:
        result_document["alpha_campaign_attempt"] = attempt
    else:
        result_document["disposition"] = "commissioning_complete"
        result_document["commissioning_receipt"] = {
            "schema_version": "alpha-commissioning-receipt-v1.1.0",
            "execution_class": "commissioning",
            "qualification_authority": False,
            "campaign_digest": assignment["campaign_digest"],
            "question_digest": assignment["question_digest"],
            "source_commit": assignment["base_ref"],
            "dataset_digest": assignment["dataset_digest"],
            "execution_window_digest": window_digest,
            "window": scope["commissioning_window"],
            "qualification_reviewed_window": scope["reviewed_window"],
            "variant_count": len(variants),
            "selected_variant_index": selected_index,
            "bundle_digests": [item["bundle_digest"] for item in bundles],
            "truth_report": truth.to_dict(),
            "gate_report": gate_report,
            "memory_receipts": memory_receipts,
        }
        result_document["commissioning_receipt"]["record_digest"] = digest(
            result_document["commissioning_receipt"]
        )
    finalize_execution_output(output, assignment, result_document)
    return result_document


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--assignment", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--max-workers", type=int, default=1, choices=range(1, 9))
    args = parser.parse_args()
    assignment = json.loads(args.assignment.read_text(encoding="utf-8"))
    repository = args.repository_root.resolve(strict=True)
    commit = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if commit != assignment["base_ref"]:
        raise SystemExit("repository commit differs from immutable assignment")
    panel = Path(assignment["dataset_path"])
    if file_digest(panel) != assignment["dataset_digest"]:
        raise SystemExit("dataset bytes differ from immutable assignment")
    for binding in assignment.get("dataset_bindings", []):
        bound_panel = Path(binding["dataset_path"])
        if file_digest(bound_panel) != binding["dataset_digest"]:
            raise SystemExit("basket dataset bytes differ from immutable assignment")
    if (
        digest({"question": " ".join(assignment["question"].split())})
        != assignment["question_digest"]
    ):
        raise SystemExit("question digest differs from immutable assignment")
    stage = assignment.get("stage", "execute")
    if stage == "draft":
        try:
            card = draft_research_card(assignment, repository_root=str(repository))
            result = {"disposition": "hypothesis_draft_ready", "hypothesis_card": card}
        except ValueError as exc:
            result = {
                "disposition": "strategy_engineering_required",
                "engineering_requirement": {
                    "schema_version": "alpha-strategy-engineering-requirement-v1.0.0",
                    "question": assignment["question"],
                    "question_digest": assignment["question_digest"],
                    "reason": str(exc),
                    "required_deliverables": [
                        "hypothesis_card_v1",
                        "hypothesis YAML",
                        "strategy.py or reviewed research_graph_v1 mapping",
                        "causality and leakage tests",
                        "classic-engine integration test",
                    ],
                    "authority": AUTHORITY,
                },
            }
    elif stage == "qualify":
        card = confirm_card(
            assignment["hypothesis_card"],
            actor=assignment["card_approval"]["actor"],
            confirmed_at=assignment["card_approval"]["approved_at"],
        )
        qualification = qualify_card(card, repository_root=str(repository))
        result = {
            "disposition": (
                "strategy_compiled"
                if qualification["qualified"]
                else "strategy_engineering_required"
            ),
            "qualification": qualification,
        }
    else:
        try:
            result = execute_registered(
                assignment, repository, args.output, max_workers=args.max_workers
            )
        except BridgeError as exc:
            if "independent specification review" in str(exc):
                result = independent_review_required(assignment, args.output, str(exc))
            elif "not registered" in str(exc):
                result = engineering_required(assignment, args.output, str(exc))
            else:
                raise
    receipt = {
        "schema_version": "alpha003-governed-receipt-v1.0.0",
        "stage": stage,
        "execution_class": assignment.get("execution_class", "qualification"),
        "qualification_authority": assignment.get("execution_class", "qualification")
        == "qualification",
        "campaign_digest": assignment["campaign_digest"],
        "question_digest": assignment["question_digest"],
        "dataset_digest": assignment["dataset_digest"],
        "source_commit": commit,
        "authority": AUTHORITY,
        **result,
    }
    receipt["receipt_digest"] = digest(receipt)
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_bytes(canonical(receipt) + b"\n")
    print(
        json.dumps(
            {
                "disposition": receipt["disposition"],
                "receipt_digest": receipt["receipt_digest"],
                "trial_count": receipt.get("alpha_campaign_attempt", {}).get(
                    "trial_count", 0
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
