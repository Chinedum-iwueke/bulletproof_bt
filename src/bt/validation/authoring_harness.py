"""Offline fixture provenance and native-result assertions for strategy authors.

This module grants no review or execution authority. Tests still call the native
runner; these helpers never replace the compiler, evaluator, or scientific gates.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import json
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import pandas as pd


class HarnessError(ValueError):
    pass


PROVENANCE_IDENTITY_KEYS = frozenset(
    {
        "dataset_build_id",
        "dataset_digest",
        "catalog_id",
        "catalog_digest",
        "manifest_digest",
        "producer_receipt_id",
        "producer_receipt_digest",
        "lake_governance_snapshot_id",
        "lake_governance_digest",
        "dataset_key",
    }
)


def assert_fixture_document_provenance(
    document: Any, production: list[dict[str, Any]]
) -> None:
    forbidden = {
        str(item[key])
        for item in production
        for key in PROVENANCE_IDENTITY_KEYS
        if key in item
    }
    pending = [document]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
        elif isinstance(value, str) and value in forbidden:
            raise HarnessError("fixture document retains production provenance")


def _digest(value: Any) -> str:
    return sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def _file_digest(path: Path) -> str:
    checksum = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def write_fixture_binding(
    root: Path,
    frame: pd.DataFrame,
    *,
    instrument: str,
    venue: str,
    source_commit: str,
    case: str,
) -> dict[str, Any]:
    """Build identities from fixture bytes, never by copying a production binding."""
    if not instrument.isalnum() or not case.replace("-", "").isalnum():
        raise HarnessError("fixture instrument and case must be safe identifiers")
    if frame.empty or not {"ts", "open", "high", "low", "close", "volume"} <= set(
        frame
    ):
        raise HarnessError("fixture requires nonempty OHLCV data and timestamps")
    if not source_commit or not venue:
        raise HarnessError("fixture requires explicit implementation commit and venue")
    directory = root / case / instrument
    directory.mkdir(parents=True, exist_ok=False)
    panel = directory / "panel.parquet"
    frame.to_parquet(panel, index=False)
    content = _file_digest(panel)
    columns = sorted(map(str, frame.columns))
    documents = {
        kind: {
            "schema_version": "offline-authoring-fixture-v1",
            "kind": kind,
            "case": case,
            "instrument": instrument,
            "venue": venue,
            "dataset_digest": content,
            "rows": len(frame),
            "columns": columns,
            "source_commit": source_commit,
            "evidence_class": "synthetic_fixture",
            "capital_or_order_authority": False,
            "scientific_certification": False,
        }
        for kind in ("catalog", "manifest", "producer_receipt", "lake_governance")
    }
    for kind, document in documents.items():
        (directory / f"{kind}.json").write_text(
            json.dumps(document, sort_keys=True) + "\n"
        )
    identity = lambda kind: str(
        uuid5(NAMESPACE_URL, f"offline-fixture:{kind}:{case}:{content}")
    )
    return {
        "instrument": instrument,
        "instruments": [instrument],
        "venue": venue,
        "dataset_path": str(panel.resolve()),
        "dataset_digest": content,
        "partition_digest": content,
        "partition_digests": [content],
        "dataset_build_id": identity("build"),
        "catalog_id": identity("catalog"),
        "producer_receipt_id": identity("producer"),
        "lake_governance_snapshot_id": identity("governance"),
        "dataset_key": f"offline-fixture-{case}-{instrument}-{content[:12]}",
        "evidence_class": "synthetic_fixture",
        "rows": len(frame),
        "output_columns_count": len(columns),
        "output_columns_digest": _digest(columns),
        "relevant_output_columns": columns,
        "builder_commit": source_commit,
        "admission_source_commit": source_commit,
        **{f"{kind}_digest": _digest(document) for kind, document in documents.items()},
    }


def assert_separate_fixture_bindings(
    fixtures: list[dict[str, Any]],
    production: list[dict[str, Any]],
) -> None:
    """Reject fixture bytes attributed to real-data receipts, even for another asset."""
    identity_keys = PROVENANCE_IDENTITY_KEYS
    production_values = {
        str(item[key]) for item in production for key in identity_keys if key in item
    }
    if not fixtures:
        raise HarnessError("fixture basket is empty")
    for binding in fixtures:
        if not identity_keys.issubset(binding) or any(
            not binding[key] for key in identity_keys
        ):
            raise HarnessError("fixture provenance is incomplete")
        if binding.get("evidence_class") != "synthetic_fixture":
            raise HarnessError("fixture must be explicitly synthetic")
        if any(str(binding.get(key)) in production_values for key in identity_keys):
            raise HarnessError("fixture reuses production provenance")
        path = Path(binding["dataset_path"])
        if not path.is_file() or _file_digest(path) != binding["dataset_digest"]:
            raise HarnessError("fixture bytes differ from binding")
        if binding.get("partition_digests") != [binding["dataset_digest"]]:
            raise HarnessError("fixture partition digest differs from fixture bytes")
        for kind in ("catalog", "manifest", "producer_receipt", "lake_governance"):
            try:
                document = json.loads((path.parent / f"{kind}.json").read_text())
            except (OSError, ValueError) as exc:
                raise HarnessError(
                    "fixture provenance document is missing or invalid"
                ) from exc
            if (
                _digest(document) != binding[f"{kind}_digest"]
                or document.get("dataset_digest") != binding["dataset_digest"]
                or document.get("evidence_class") != "synthetic_fixture"
                or document.get("scientific_certification") is not False
            ):
                raise HarnessError(
                    "fixture provenance document does not bind these bytes"
                )


def assert_unchanged_scientific_contract(
    production: dict[str, Any],
    fixture: dict[str, Any],
) -> None:
    """Only dataset bindings may change; gates, grid, timing, costs and windows stay frozen.

    Fixture contracts are made separately. This check does not mutate either input
    or make an invalid contract valid by removing fields from the supplied objects.
    """
    for document in (production, fixture):
        immutable = document.get("immutable_contract")
        if not isinstance(immutable, dict) or "dataset_bindings" not in immutable:
            raise HarnessError("contract requires explicit immutable dataset bindings")
    # Typed intents can repeat the binding and its content-derived digest. Ignore
    # only these provenance locations, never thresholds, clocks or parameters.
    provenance_paths = {
        ("strategy_intent_digest",),
        ("immutable_contract", "dataset_bindings"),
        ("immutable_contract", "intent_digest"),
        ("immutable_contract", "evidence_digest"),
        ("immutable_contract", "validation_requirements", "coverage"),
    }

    def semantics(value, path=()):
        if isinstance(value, dict):
            return {
                key: semantics(item, (*path, key))
                for key, item in value.items()
                if (*path, key) not in provenance_paths
            }
        if isinstance(value, list):
            return [semantics(item, (*path, index)) for index, item in enumerate(value)]
        return value

    left, right = semantics(production), semantics(fixture)
    if left != right:
        raise HarnessError(
            "fixture changed scientific contract outside dataset bindings"
        )


def assert_native_implementation_unmodified(
    repository: Path, contract: dict[str, Any]
) -> None:
    """Catch test monkeypatches of native functions and literal frozen constants."""
    native = contract["evaluation"]["native_implementation"]
    path = (repository / native["path"]).resolve(strict=True)
    if (
        path.parent != (repository / "src/bt/strategy").resolve()
        or path.suffix != ".py"
    ):
        raise HarnessError("native implementation escapes strategy directory")
    module = importlib.import_module(f"bt.strategy.{path.stem}")
    if Path(module.__file__).resolve() != path:
        raise HarnessError("native implementation loaded from another worktree")
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function = getattr(module, node.name, None)
            if (
                not inspect.isfunction(function)
                or Path(function.__code__.co_filename).resolve() != path
                or function.__code__.co_firstlineno
                != (
                    node.decorator_list[0].lineno
                    if node.decorator_list
                    else node.lineno
                )
            ):
                raise HarnessError(f"native function replaced by test: {node.name}")
        elif isinstance(node, ast.Assign):
            try:
                literal = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id.isupper()
                    and getattr(module, target.id, object()) != literal
                ):
                    raise HarnessError(f"native constant changed by test: {target.id}")


def assert_runner_governance_unmodified(repository: Path) -> None:
    """Reject replacing the runner or its approval/provenance checks in fixtures."""
    runner = importlib.import_module("scripts.run_alpha_research_assignment")
    sources = {
        "scripts/run_alpha_research_assignment.py": (
            "execute_registered",
            "verify_trusted_dataset_bindings",
        ),
        "src/bt/governance/alpha_strategy_pipeline.py": (
            "governed_review_verified",
            "complete_independent_review",
        ),
    }
    for relative, names in sources.items():
        path = (repository / relative).resolve(strict=True)
        declarations = {
            node.name: node
            for node in ast.parse(path.read_text()).body
            if isinstance(node, ast.FunctionDef)
        }
        for name in names:
            function = getattr(runner, name, None)
            declaration = declarations.get(name)
            if (
                declaration is None
                or not inspect.isfunction(function)
                or Path(function.__code__.co_filename).resolve() != path
                or function.__code__.co_firstlineno != declaration.lineno
            ):
                raise HarnessError(f"runner governance replaced by test: {name}")


def run_native_case(
    assignment: dict[str, Any],
    *,
    repository: Path,
    output: Path,
    production_contract: dict[str, Any],
    expected_outcome: str,
    expected_parameters: list[dict[str, Any]],
    required_metrics: list[str],
) -> dict[str, Any]:
    """Run a separately reviewed offline fixture without replacing native logic.

    This helper does not synthesize approvals, change thresholds, or mock runner
    components. Callers must supply a valid review packet bound to their fixture.
    """
    from scripts.run_alpha_research_assignment import execute_registered

    if assignment.get("execution_class") != "commissioning":
        raise HarnessError("offline native cases must be commissioning-only")
    assert_runner_governance_unmodified(repository)
    fixture = assignment["qualification"]["artifact_bundle"]["engine_hypothesis_yaml"]
    assert_separate_fixture_bindings(
        assignment["dataset_bindings"],
        production_contract["immutable_contract"]["dataset_bindings"],
    )
    assert_fixture_document_provenance(
        fixture, production_contract["immutable_contract"]["dataset_bindings"]
    )
    if fixture["immutable_contract"]["dataset_bindings"] != [
        {key: value for key, value in item.items() if key != "dataset_path"}
        for item in assignment["dataset_bindings"]
    ]:
        raise HarnessError("fixture contract and assignment bindings disagree")
    assert_unchanged_scientific_contract(production_contract, fixture)
    assert_native_implementation_unmodified(repository, fixture)
    result = execute_registered(assignment, repository, output, max_workers=1)
    assert_runner_governance_unmodified(repository)
    assert_native_implementation_unmodified(repository, fixture)
    report = assert_terminal_case(
        result,
        output=output,
        expected_outcome=expected_outcome,
        expected_parameters=expected_parameters,
        required_metrics=required_metrics,
    )
    return report | {"validation_mode": "smoke"}


def run_scientific_case(
    *,
    repository: Path,
    output: Path,
    production_contract: dict[str, Any],
    fixture_contract: dict[str, Any],
    bindings: list[dict[str, Any]],
    source_commit: str,
    expected_outcome: str,
) -> dict[str, Any]:
    """Long-horizon synthetic evaluator test, not engine execution or a receipt.

    Unlike commissioning smoke, this uses the requirements-planned scientific
    partitions. It never extends production execution authority or opens real data.
    """
    import scripts.run_alpha_research_assignment as runner

    from bt.experiments.adaptive_representation import (
        materialize_adaptive_representation,
    )
    from bt.governance.data_requirements import plan_validation
    from bt.hypotheses.contract import HypothesisContract

    if expected_outcome not in {"positive", "negative", "invalid", "failed"}:
        raise HarnessError("unknown expected scientific outcome")
    assert_unchanged_scientific_contract(production_contract, fixture_contract)
    production_bindings = production_contract["immutable_contract"]["dataset_bindings"]
    assert_separate_fixture_bindings(bindings, production_bindings)
    assert_fixture_document_provenance(fixture_contract, production_bindings)
    immutable = fixture_contract["immutable_contract"]
    identities = [
        {k: v for k, v in binding.items() if k != "dataset_path"}
        for binding in bindings
    ]
    if identities != immutable["dataset_bindings"]:
        raise HarnessError("scientific fixture and contract bindings disagree")
    package = immutable["validation_requirements"]
    contract = HypothesisContract.from_dict(fixture_contract)
    variants = contract.to_run_specs()
    plan = plan_validation(
        requirements=package["requirements"],
        coverage=package["coverage"],
        dataset_bindings=identities,
        instruments=immutable["instruments"],
        source_commit=source_commit,
        approved_window=immutable["window"],
        maximum_history_days=package["maximum_history_days"],
        maximum_bar_variant_work=package["maximum_bar_variant_work"],
        variants=len(variants),
        mode="scientific",
    )
    if plan["status"] != "ready":
        raise HarnessError(
            "scientific fixture is infeasible: " + ",".join(plan["reasons"])
        )
    assert_native_implementation_unmodified(repository, fixture_contract)
    assert_runner_governance_unmodified(repository)
    output.mkdir(parents=True, exist_ok=False)
    partitions = plan["partitions"]
    start = pd.Timestamp(partitions["feature_warmup"]["start"])
    end = pd.Timestamp(partitions["target_tail"]["end"])
    panels = {}
    for binding in bindings:
        # Predicate pushdown bounds memory by the frozen plan, not the entire lake.
        panel = pd.read_parquet(
            binding["dataset_path"],
            filters=[
                ("ts", ">=", start.to_pydatetime()),
                ("ts", "<", end.to_pydatetime()),
            ],
        )
        symbol = binding["instrument"]
        if "symbol" in panel and not panel["symbol"].eq(symbol).all():
            raise HarnessError("fixture panel asset identity mismatch")
        panel["symbol"] = symbol
        panels[symbol] = panel
    representation = materialize_adaptive_representation(
        fixture_contract["representation_plan"], panels
    )
    raw_path = output / "raw-fixture.parquet"
    pd.concat(list(panels.values()), ignore_index=True).to_parquet(
        raw_path, index=False
    )
    features = runner.materialize_causal_feature_frame(representation)
    attached = runner.attach_adaptive_features(
        raw_path,
        representation,
        output=output,
        declared_fields=fixture_contract["execution_semantics"][
            "adaptive_representation_fields"
        ],
        feature_frame=features,
    )
    runner.validate_materialized_payload(
        attached,
        representation,
        instruments=immutable["instruments"],
        feature_frame=features,
    )
    frame = pd.read_parquet(attached)
    frame = frame.loc[
        pd.to_datetime(frame["ts"], utc=True)
        >= pd.Timestamp(partitions["training"]["start"])
    ].copy()
    evaluator = runner.resolve_strategy_intent_evaluator(fixture_contract, repository)
    payload = runner.validate_strategy_intent_evaluation(
        evaluator(
            frame=frame,
            decision_frame=runner.evaluator_decision_frame(features, frame),
            adaptive_frame=representation.frame,
            parameter_grid=contract.schema.parameter_grid,
            validation_start=partitions["validation"]["start"],
            validation_end=partitions["validation"]["end"],
            test_start=partitions["test"]["start"],
            test_end=partitions["test"]["end"],
            representation_plan_digest=representation.receipt["plan_digest"],
            immutable_contract=immutable,
            costs=fixture_contract.get("costs", {}),
        ),
        variants,
    )
    assert_native_implementation_unmodified(repository, fixture_contract)
    assert_runner_governance_unmodified(repository)
    # Retain failures too, before asserting expected outcome. Never publish them.
    artifact = {
        "plan": plan,
        "evaluation": payload,
        "representation_receipt": representation.receipt,
        "scope": "offline_synthetic_evaluator_only",
        "scientific_certification": False,
        "capital_or_order_authority": False,
    }
    artifact["artifact_digest"] = _digest(artifact)
    (output / "scientific-fixture-evaluation.json").write_text(
        json.dumps(artifact, sort_keys=True) + "\n"
    )
    terminal = payload["held_out_evaluation"]
    if terminal["outcome"] != expected_outcome:
        raise HarnessError("scientific terminal outcome differs from expected case")
    if expected_outcome == "positive" and not (
        terminal["passed"] and terminal["held_out_evaluated"]
    ):
        raise HarnessError(
            "positive scientific case must actually evaluate held-out selection"
        )
    if not set(fixture_contract["evaluation"]["metrics"]) <= set(
        terminal.get("metrics", {})
    ):
        raise HarnessError("scientific terminal evidence omits declared metrics")
    return {
        "schema_version": "offline-scientific-case-v1",
        "validation_mode": "scientific",
        "outcome": expected_outcome,
        "variant_count": len(variants),
        "artifact_digest": artifact["artifact_digest"],
        "plan_digest": plan["plan_digest"],
        "scientific_certification": False,
        "capital_or_order_authority": False,
    }


def assert_terminal_case(
    result: dict[str, Any],
    *,
    output: Path,
    expected_outcome: str,
    expected_parameters: list[dict[str, Any]],
    required_metrics: list[str],
) -> dict[str, Any]:
    """Require an exact terminal outcome and one digest-verified artifact per variant."""
    if expected_outcome not in {"positive", "negative", "invalid", "failed"}:
        raise HarnessError("unknown expected outcome")
    envelope = result.get("publication_envelope", {})
    if (
        envelope.get("qualification_authority") is not False
        or result.get("disposition") != "commissioning_complete"
    ):
        raise HarnessError("fixture result must have no qualification authority")
    trial = envelope.get("trial", {})
    # Native scientific evaluators retain their result separately from the
    # trade-log held-out score, which is deliberately null for these strategies.
    heldout = trial.get("hypothesis_evaluation")
    if heldout is None:
        heldout = trial.get("held_out_evaluation")
    if not isinstance(heldout, dict):
        raise HarnessError("native terminal outcome evidence is missing")
    if heldout.get("outcome") != expected_outcome:
        raise HarnessError("native terminal outcome differs from expected case")
    if expected_outcome == "positive" and (
        heldout.get("passed") is not True
        or heldout.get("held_out_evaluated") is not True
    ):
        raise HarnessError(
            "positive fixture must actually open and evaluate held-out selection"
        )
    if not set(required_metrics) <= set(heldout.get("metrics", {})):
        raise HarnessError("terminal evidence omits declared metrics")
    paths = sorted(
        output.glob(
            "run-bundles-*/bundles/*/artifacts/strategy_intent_validation_evaluation.json"
        )
    )
    expected = sorted(_digest(params) for params in expected_parameters)
    if (
        not expected
        or len(set(expected)) != len(expected)
        or len(paths) != len(expected)
    ):
        raise HarnessError("variant artifact count is incomplete or ambiguous")
    actual = []
    for path in paths:
        record = json.loads(path.read_text())
        actual.append(_digest(record["parameters"]))
        if record.get("record_digest") != _digest(
            {k: v for k, v in record.items() if k != "record_digest"}
        ):
            raise HarnessError("variant record digest is invalid")
        reuse = json.loads((path.parent / "downstream_reuse_manifest.json").read_text())
        if reuse.get("artifacts", {}).get(path.name) != _file_digest(path):
            raise HarnessError("retained artifact digest is invalid")
    if sorted(actual) != expected:
        raise HarnessError("variant parameter coverage differs from frozen grid")
    return {
        "schema_version": "offline-authoring-case-v1",
        "outcome": expected_outcome,
        "variant_count": len(paths),
        "scientific_certification": False,
        "capital_or_order_authority": False,
    }
