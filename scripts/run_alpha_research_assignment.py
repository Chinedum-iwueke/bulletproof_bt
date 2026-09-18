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

from bt.execution.model_registry import declared_classic_bundle
from bt.experiments.hypothesis_runner import execute_hypothesis_variant
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
    confirm_card,
    draft_research_card,
    governed_review_verified,
    qualify_card,
)
from bt.hypotheses.contract import HypothesisContract
from bt.logging.run_bundle import finalize_run_bundle
from bt.validation.experiment_truth import validate_experiment_root, write_truth_report

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
            "instruments": assignment.get(
                "instruments", [assignment["instrument"]]
            ),
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
    assignment: dict[str, Any], frame: pd.DataFrame, code_digest: str
) -> tuple[RepresentationContract, dict[str, Any]]:
    decisions = pd.to_datetime(frame["ts"], utc=True)
    first, last = decisions.iloc[0], decisions.iloc[-1]
    split_one = decisions.iloc[len(decisions) * 6 // 10]
    split_two = decisions.iloc[len(decisions) * 8 // 10]
    audit = frame.assign(
        decision_at=decisions,
        membership_known_at=first,
        membership_valid_from=first,
        close_feature=frame["close"],
        observed_at=decisions,
        available_at=decisions,
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
            validation_start=decisions.iloc[len(decisions) * 6 // 10 + 1].isoformat(),
            validation_end=split_two.isoformat(),
            test_start=decisions.iloc[len(decisions) * 8 // 10 + 1].isoformat(),
            test_end=last.isoformat(),
            fit_start=first.isoformat(),
            fit_end=split_one.isoformat(),
            purge_seconds=60,
            embargo_seconds=60,
        ),
    )
    return contract, certify_representation_frame(contract, audit)


def held_out_evaluation(run_dir: Path, test_start: str) -> dict[str, Any]:
    trades_path = run_dir / "trades.csv"
    try:
        trades = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame()
    if trades.empty:
        return {
            "test_start": test_start,
            "trade_count": 0,
            "mean_net_r": 0.0,
            "double_cost_mean_net_r": 0.0,
            "adequate_support": False,
            "positive_net_edge": False,
            "cost_stress_passed": False,
        }
    entry = pd.to_datetime(trades["entry_ts"], utc=True, errors="coerce")
    sample = trades.loc[entry >= pd.Timestamp(test_start)]
    net_column = "r_net" if "r_net" in sample else "r_multiple_net"
    cost_column = "cost_drag_r" if "cost_drag_r" in sample else None
    net = pd.to_numeric(sample[net_column], errors="coerce").dropna()
    costs = (
        pd.to_numeric(sample.loc[net.index, cost_column], errors="coerce").fillna(0.0)
        if cost_column
        else pd.Series(0.0, index=net.index)
    )
    stressed = net - costs.abs()
    return {
        "test_start": test_start,
        "trade_count": int(len(net)),
        "mean_net_r": float(net.mean()) if len(net) else 0.0,
        "double_cost_mean_net_r": float(stressed.mean()) if len(stressed) else 0.0,
        "adequate_support": len(net) >= 50,
        "positive_net_edge": bool(len(net) and net.mean() > 0),
        "cost_stress_passed": bool(len(stressed) and stressed.mean() > 0),
    }


def period_evaluation(run_dir: Path, start: str, end: str) -> dict[str, Any]:
    trades_path = run_dir / "trades.csv"
    try:
        trades = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame()
    if trades.empty:
        return {"trade_count": 0, "mean_net_r": 0.0}
    entry = pd.to_datetime(trades["entry_ts"], utc=True, errors="coerce")
    sample = trades.loc[(entry >= pd.Timestamp(start)) & (entry <= pd.Timestamp(end))]
    net_column = "r_net" if "r_net" in sample else "r_multiple_net"
    net = pd.to_numeric(sample[net_column], errors="coerce").dropna()
    return {
        "trade_count": int(len(net)),
        "mean_net_r": float(net.mean()) if len(net) else 0.0,
    }


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


def complete_five_minute_bars(frame: pd.DataFrame) -> pd.DataFrame:
    """Build strict left-labeled 5m bars from complete, unique 1m observations."""
    required = {"ts", "symbol", "close", "quote_volume"}
    missing = required - set(frame.columns)
    if missing:
        raise BridgeError(
            f"impact-proxy evaluation is missing source fields: {sorted(missing)}"
        )
    ordered = frame.loc[:, sorted(required)].copy()
    ordered["ts"] = pd.to_datetime(ordered["ts"], utc=True, errors="raise")
    ordered = ordered.sort_values(["symbol", "ts"])
    if ordered.duplicated(["symbol", "ts"]).any():
        raise BridgeError("impact-proxy evaluation rejects duplicate minute bars")
    if (ordered["ts"].dt.second != 0).any() or (
        ordered["ts"].dt.microsecond != 0
    ).any():
        raise BridgeError("impact-proxy evaluation requires minute-aligned source bars")
    ordered["bucket"] = ordered["ts"].dt.floor("5min")
    grouped = ordered.groupby(["symbol", "bucket"], sort=True)
    complete = grouped.filter(
        lambda sample: len(sample) == 5
        and sample["ts"].nunique() == 5
        and (
            sample["ts"].max() - sample["ts"].min()
            == pd.Timedelta(minutes=4)
        )
    )
    if complete.empty:
        return pd.DataFrame(
            columns=["symbol", "ts", "close", "quote_volume"]
        )
    return (
        complete.groupby(["symbol", "bucket"], sort=True)
        .agg(close=("close", "last"), quote_volume=("quote_volume", "sum"))
        .reset_index()
        .rename(columns={"bucket": "ts"})
    )


def impact_proxy_evaluation(
    frame: pd.DataFrame,
    *,
    test_start: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate the preregistered held-out impact proxy against matched shocks."""
    bars = complete_five_minute_bars(frame)
    if bars.empty:
        raise BridgeError("impact-proxy evaluation has no complete 5m bars")
    threshold = float(params["impact_proxy_threshold"])
    window = int(params["normalization_window"])
    band = float(params["return_shock_control_band"])
    parts: list[pd.DataFrame] = []
    for _, sample in bars.groupby("symbol", sort=False):
        sample = sample.sort_values("ts").copy()
        sample["signal_return"] = sample["close"].pct_change()
        sample["impact_proxy"] = (
            sample["signal_return"].abs() / sample["quote_volume"]
        )
        sample["threshold_value"] = sample["impact_proxy"].shift(1).rolling(
            window=window, min_periods=window
        ).quantile(threshold)
        sample["next_30m_return"] = sample["close"].shift(-6) / sample["close"] - 1.0
        sample["signed_reversal"] = (
            -sample["signal_return"].apply(
                lambda value: 1.0 if value > 0 else -1.0
            )
            * sample["next_30m_return"]
        )
        parts.append(sample)
    evaluated = pd.concat(parts, ignore_index=True)
    evaluated = evaluated.loc[
        (evaluated["ts"] >= pd.Timestamp(test_start))
        & evaluated["signal_return"].notna()
        & evaluated["next_30m_return"].notna()
        & evaluated["threshold_value"].notna()
        & (evaluated["quote_volume"] >= 1_000_000.0)
    ].copy()
    extreme = evaluated.loc[
        evaluated["impact_proxy"] >= evaluated["threshold_value"]
    ]
    controls: list[float] = []
    for row in extreme.itertuples(index=False):
        magnitude = abs(float(row.signal_return))
        low, high = magnitude * (1.0 - band), magnitude * (1.0 + band)
        candidates = evaluated.loc[
            (evaluated["symbol"] == row.symbol)
            & (evaluated["ts"] != row.ts)
            & (evaluated["impact_proxy"] < evaluated["threshold_value"])
            & (evaluated["signal_return"].abs().between(low, high))
            & ((evaluated["signal_return"] > 0) == (row.signal_return > 0))
        ]
        if not candidates.empty:
            distance = (candidates["signal_return"].abs() - magnitude).abs()
            controls.append(float(candidates.loc[distance.idxmin(), "signed_reversal"]))
    extreme_reversal = pd.to_numeric(
        extreme["signed_reversal"], errors="coerce"
    ).dropna()
    control_mean = float(pd.Series(controls, dtype=float).mean()) if controls else 0.0
    extreme_mean = float(extreme_reversal.mean()) if len(extreme_reversal) else 0.0
    direction = {
        "long": int((extreme["signal_return"] < 0).sum()),
        "short": int((extreme["signal_return"] > 0).sum()),
    }
    matched = {
        "extreme_observations": int(len(extreme_reversal)),
        "matched_control_observations": len(controls),
        "extreme_mean_signed_30m_return": extreme_mean,
        "control_mean_signed_30m_return": control_mean,
        "extreme_minus_control": extreme_mean - control_mean,
        "outperformed_control": bool(controls and extreme_mean > control_mean),
    }
    report = {
        "schema_version": "alpha-impact-proxy-evaluation-v1.0.0",
        "measurement": "held-out causal predictive association; not executable PnL",
        "test_start": pd.Timestamp(test_start).isoformat(),
        "resampling": "strict complete left-labeled 5m bars from unique 1m rows",
        "parameters": {
            "impact_proxy_threshold": threshold,
            "normalization_window": window,
            "return_shock_control_band": band,
        },
        "direction_balance": direction,
        "matched_return_shock_control": matched,
    }
    report["record_digest"] = digest(report)
    return report


def execute_variant_grid(jobs: list[dict[str, Any]], max_workers: int) -> list[dict[str, Any]]:
    if not 1 <= len(jobs) <= 8 or not 1 <= max_workers <= 8:
        raise BridgeError("Alpha execution requires 1-8 variants and 1-8 worker slots")
    if max_workers == 1:
        return [_execute_variant_job(job) for job in jobs]
    with ProcessPoolExecutor(max_workers=min(max_workers, len(jobs)), mp_context=get_context("spawn")) as pool:
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


def independent_review_required(assignment: dict[str, Any], output: Path, reason: str) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    evidence = {
        "schema_version": "alpha-independent-review-failure-v1.0.0",
        "campaign_digest": assignment["campaign_digest"],
        "question_digest": assignment["question_digest"],
        "source_commit": assignment["base_ref"],
        "reason": reason, "trial_count": 0, "authority": AUTHORITY,
    }
    path = output / "independent-review-failure.json"
    path.write_bytes(canonical(evidence) + b"\n")
    attempt = base_attempt(
        assignment, hypothesis_id=f"REVIEW-{assignment['question_digest'][:16]}",
        hypothesis_digest=digest(assignment.get("qualification", {})),
    ) | {
        "trial_count": 0, "outcome": "failed", "failure_stage": "independent_evaluation",
        "gate_report": {
            **{key: False for key in (
                "truth_certified", "point_in_time_valid", "reproducible", "out_of_sample_evaluated",
                "cost_stress_evaluated", "selection_bias_audited", "independent_review_complete",
                "shadow_eligible", "production_eligible", "capital_authority",
            )},
            "failed_gates": ["independent_specification_review"],
        },
        "evidence_digests": [file_digest(path)],
    }
    return {"disposition": "independent_review_required", "alpha_campaign_attempt": attempt}


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
        raise BridgeError("independent specification review is missing or unbound; no compute started")
    scope = execution_scope(assignment, qualification)
    output.mkdir(parents=True, exist_ok=False)
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

    execution_data_path = Path(assignment["dataset_path"])
    window_digest = assignment["dataset_digest"]
    if assignment.get("window_start") and assignment.get("window_end"):
        start = pd.Timestamp(assignment["window_start"])
        end = pd.Timestamp(assignment["window_end"])
        selected = pd.read_parquet(
            execution_data_path,
            filters=[
                ("ts", ">=", start.to_pydatetime()),
                ("ts", "<", end.to_pydatetime()),
            ],
        )
        if selected.empty:
            raise BridgeError("immutable execution window contains no admitted rows")
        execution_data_path = output / "execution-window.parquet"
        selected.to_parquet(execution_data_path, index=False)
        window_digest = file_digest(execution_data_path)

    lightweight_columns = ["ts", "symbol", "close"]
    if "quote_volume" in contract.schema.execution_semantics.get(
        "required_extra_columns", []
    ):
        lightweight_columns.append("quote_volume")
    lightweight = pd.read_parquet(
        execution_data_path, columns=lightweight_columns
    )
    code_digest = digest(assignment["base_ref"].encode())
    rep, leakage = representation(assignment, lightweight, code_digest)
    model = declared_classic_bundle(
        profile="tier2",
        parameters={
            "taker_fee_bps": 6.0,
            "slippage_bps": 2.0,
            "spread_bps": 1.0,
            "delay_bars": 1,
        },
    )
    search = compile_hypothesis_search_plan(
        contract=contract,
        family_id=f"alpha002-{assignment['question_digest'][:12]}",
        hypothesis_digest=contract_receipt["content_digest"],
        dataset_snapshot_id=assignment["dataset_build_id"],
        dataset_digest=assignment["dataset_digest"],
        repository_commit=assignment["base_ref"],
        code_digest=code_digest,
        market_model_bundle_digest=model.digest,
        representation_contract_digest=rep.digest,
        tiers=("Tier2",),
        seeds=(7,),
        resources={"max_workers": min(max_workers, len(variants))},
        budget=SearchBudget(len(variants), len(variants), 86400, min(max_workers, len(variants))),
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
    execution_overrides: list[str] = []
    if required_extra_columns:
        if (
            not isinstance(required_extra_columns, list)
            or not all(
                isinstance(column, str) and column
                for column in required_extra_columns
            )
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
    jobs = [dict(
        contract=contract, spec=spec,
        tier="Tier3" if assignment["tier"] == "Tier3" else "Tier2",
        config_path=str(repository / "configs/engine.yaml"),
        data_path=str(execution_data_path), out_root=str(runs),
        override_paths=execution_overrides,
        run_slug=f"row_{index:04d}", phase=phase,
    ) for index, spec in enumerate(variants, start=1)]
    for result in execute_variant_grid(jobs, max_workers):
        run_dir = Path(result["run_dir"])
        for name, document in (
            ("market_model_bundle.json", model.document()),
            ("representation_contract.json", rep.document()),
            ("representation_leakage_report.json", leakage),
            ("search_plan.json", search.document()),
        ):
            (run_dir / name).write_bytes(canonical(document) + b"\n")
        results.append(result)
        run_dirs.append(run_dir)
    trials = search.trials()
    validation = [
        period_evaluation(path, rep.split.validation_start, rep.split.validation_end)
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
    selection_audit = {
        "schema_version": "alpha-selection-bias-audit-v1.0.0",
        "declared_variant_count": len(variants),
        "evaluated_variant_count": len(validation),
        "selection_partition": "validation",
        "selection_metric": "mean_net_r",
        "held_out_test_consulted": False,
        "stopping_rule": "exhaustive",
        "selected_variant_index": selected_index,
        "validation_results": validation,
        "search_plan_digest": search.digest,
    }
    selection_audit["record_digest"] = digest(selection_audit)
    is_impact_proxy = (
        contract.schema.metadata.hypothesis_family == "impact_proxy_reversal"
    )
    evaluation_artifact = (
        impact_proxy_evaluation(
            lightweight,
            test_start=rep.split.test_start,
            params=variants[selected_index]["params"],
        )
        if is_impact_proxy
        else weekend_regime_comparison(lightweight)
    )
    if "record_digest" not in evaluation_artifact:
        evaluation_artifact["record_digest"] = digest(evaluation_artifact)
    evaluation_artifact_name = (
        "impact_proxy_evaluation.json"
        if is_impact_proxy
        else "weekend_regime_comparison.json"
    )
    for candidate_run in run_dirs:
        (candidate_run / "selection_bias_audit.json").write_bytes(
            canonical(selection_audit) + b"\n"
        )
        (candidate_run / evaluation_artifact_name).write_bytes(
            canonical(evaluation_artifact) + b"\n"
        )
    truth = validate_experiment_root(experiment)
    write_truth_report(truth, experiment / "summaries")
    if truth.status != "PASS":
        raise BridgeError(f"native truth validation failed: {truth.hard_failures}")
    trial = trials[selected_index]
    result = results[selected_index]
    run_dir = run_dirs[selected_index]
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
    bundle = bundles[selected_index]
    retained_bundle = retained[selected_index]
    holdout = held_out_evaluation(run_dir, rep.split.test_start)
    manifest = json.loads(
        (retained_bundle / "run_bundle_manifest.json").read_text(encoding="utf-8")
    )
    memory_receipts = [
        record_alpha_memory(
            Path(assignment["memory_database"]), assignment=assignment, bundle=item
        )
        for item in bundles
    ]
    memory = memory_receipts[selected_index]
    metrics = {
        key: value
        for key, value in result.items()
        if isinstance(value, (int, float, bool))
    }
    metrics.update(
        {
            "oos_trade_count": holdout["trade_count"],
            "oos_mean_net_r": holdout["mean_net_r"],
            "double_cost_oos_mean_net_r": holdout["double_cost_mean_net_r"],
            "declared_variant_count": len(variants),
            "selected_variant_index": selected_index,
            "selection_basis": "validation_mean_net_r",
        }
    )
    metrics["maximum_drawdown"] = float(result.get("max_drawdown_r", 0.0))
    if is_impact_proxy:
        metrics["direction_balance"] = evaluation_artifact["direction_balance"]
        metrics["matched_return_shock_control"] = evaluation_artifact[
            "matched_return_shock_control"
        ]
    else:
        metrics["selection_bias_audit"] = selection_audit
    required_metrics = tuple(
        contract_document.get("evaluation", {}).get("metrics", ())
    )
    missing_metrics = sorted(set(required_metrics) - set(metrics))
    if missing_metrics:
        raise BridgeError(
            f"declared evaluation metrics were not produced: {missing_metrics}"
        )
    started_at = datetime.fromtimestamp(run_dir.stat().st_mtime, tz=UTC)
    ended_at = datetime.now(UTC)
    independent_review_complete = governed_review_verified(assignment, qualification)
    passed_edge = bool(
        holdout["adequate_support"]
        and holdout["positive_net_edge"]
        and holdout["cost_stress_passed"]
        and independent_review_complete
        and scope["qualification_authority"]
        and (
            not is_impact_proxy
            or evaluation_artifact["matched_return_shock_control"][
                "outperformed_control"
            ]
        )
    )
    failed_gates = [
        name
        for name, passed in (
            ("oos_trade_support", holdout["adequate_support"]),
            ("positive_oos_net_edge", holdout["positive_net_edge"]),
            ("double_cost_oos_edge", holdout["cost_stress_passed"]),
        )
        if not passed
    ]
    if not independent_review_complete:
        failed_gates.append("independent_specification_review")
    if not scope["qualification_authority"]:
        failed_gates.append("commissioning_run_has_no_qualification_authority")
    if is_impact_proxy and not evaluation_artifact["matched_return_shock_control"][
        "outperformed_control"
    ]:
        failed_gates.append("matched_return_shock_control")
    gate_report = {
        "truth_certified": True,
        "point_in_time_valid": True,
        "reproducible": True,
        "out_of_sample_evaluated": True,
        "cost_stress_evaluated": True,
        "selection_bias_audited": True,
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
            "failed" if not independent_review_complete
            else "candidate" if passed_edge else "negative"
        ),
        "failure_stage": None if independent_review_complete else "independent_evaluation",
        "gate_report": gate_report,
        "evidence_digests": [
            *[item["bundle_digest"] for item in bundles],
            selection_audit["record_digest"],
            evaluation_artifact["record_digest"],
            truth.to_dict()["report_digest"],
        ]
        if "report_digest" in truth.to_dict()
        else [bundle["bundle_digest"], digest(truth.to_dict())],
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
            "instrument": assignment["instrument"],
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
            "selection_basis": "validation_mean_net_r",
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
            "delay_bars": 1,
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
            "search_plan_digest": search.digest,
            "result_disposition": "accepted" if passed_edge else "rejected",
            "held_out_evaluation": holdout,
            "selection_bias_audit": selection_audit,
            "hypothesis_evaluation": evaluation_artifact,
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
    }
    if scope["qualification_authority"]:
        result_document["alpha_campaign_attempt"] = attempt
    else:
        result_document["disposition"] = "commissioning_complete"
        result_document["commissioning_receipt"] = {
            "schema_version": "alpha-commissioning-receipt-v1.0.0",
            "execution_class": "commissioning",
            "qualification_authority": False,
            "campaign_digest": assignment["campaign_digest"],
            "question_digest": assignment["question_digest"],
            "source_commit": assignment["base_ref"],
            "dataset_digest": assignment["dataset_digest"],
            "execution_window_digest": window_digest,
            "window": scope["commissioning_window"],
            "reviewed_window": scope["reviewed_window"],
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
            result = execute_registered(assignment, repository, args.output, max_workers=args.max_workers)
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
