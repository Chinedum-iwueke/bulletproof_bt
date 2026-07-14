from __future__ import annotations

from pathlib import Path

EXPERIMENT_VARIANTS = {"stable", "vol"}


def resolve_experiment_root(
    *,
    outputs_root: str | Path,
    phase: str,
    experiment_name: str,
    variant: str,
) -> Path:
    """
    Return the canonical filesystem root for one experiment dataset variant.

    variant:
      stable
      vol
    """
    if variant not in EXPERIMENT_VARIANTS:
        raise ValueError(f"unsupported experiment variant: {variant!r}")
    if not phase:
        raise ValueError("phase is required")
    if not experiment_name:
        raise ValueError("experiment_name is required")
    return Path(outputs_root) / phase / f"{experiment_name}_parallel_{variant}"


def resolve_legacy_experiment_root(
    *,
    outputs_root: str | Path,
    experiment_name: str,
    variant: str,
) -> Path:
    if variant not in EXPERIMENT_VARIANTS:
        raise ValueError(f"unsupported experiment variant: {variant!r}")
    return Path(outputs_root) / f"{experiment_name}_parallel_{variant}"


def resolve_existing_experiment_root(
    *,
    outputs_root: str | Path,
    phase: str,
    experiment_name: str,
    variant: str,
) -> Path:
    root = resolve_experiment_root(
        outputs_root=outputs_root,
        phase=phase,
        experiment_name=experiment_name,
        variant=variant,
    )
    if root.exists():
        return root
    legacy_root = resolve_legacy_experiment_root(
        outputs_root=outputs_root,
        experiment_name=experiment_name,
        variant=variant,
    )
    return legacy_root if legacy_root.exists() else root


def resolve_existing_experiment_root_from_path(path: str | Path) -> Path:
    root = Path(path)
    if root.exists():
        return root
    if root.parent.name in {"tier2", "tier2a", "tier2b", "tier3"}:
        legacy_root = root.parent.parent / root.name
        if legacy_root.exists():
            return legacy_root
    return root


def resolve_output_phase_root(*, outputs_root: str | Path, phase: str) -> Path:
    if not phase:
        raise ValueError("phase is required")
    return Path(outputs_root) / phase


def resolve_pipeline_log_path(*, outputs_root: str | Path, phase: str, experiment_name: str) -> Path:
    return resolve_output_phase_root(outputs_root=outputs_root, phase=phase) / f"{experiment_name}_pipeline.log"


def resolve_command_log_dir(*, outputs_root: str | Path, phase: str, experiment_name: str) -> Path:
    return resolve_output_phase_root(outputs_root=outputs_root, phase=phase) / f"{experiment_name}_command_logs"


def resolve_daemon_command_log_dir(*, outputs_root: str | Path, phase: str, experiment_name: str) -> Path:
    return resolve_output_phase_root(outputs_root=outputs_root, phase=phase) / f"{experiment_name}_daemon_command_logs"


def resolve_verdict_bundle_root(*, outputs_root: str | Path, phase: str, experiment_name: str) -> Path:
    return resolve_output_phase_root(outputs_root=outputs_root, phase=phase) / f"{experiment_name}_verdict_bundle"


def resolve_phase_artifact_dir(*, artifact_root: str | Path, phase: str) -> Path:
    if not phase:
        raise ValueError("phase is required")
    root = Path(artifact_root)
    return root if root.name == phase else root / phase


def resolve_phase_artifact_search_dirs(*, artifact_root: str | Path, phase: str) -> list[Path]:
    preferred = resolve_phase_artifact_dir(artifact_root=artifact_root, phase=phase)
    root = Path(artifact_root)
    legacy = root.parent if root.name == phase else root
    dirs = [preferred]
    if legacy != preferred:
        dirs.append(legacy)
    return dirs


def discover_experiment_roots(outputs_root: str | Path) -> list[Path]:
    root = Path(outputs_root)
    if not root.exists():
        return []
    discovered = {runs_dir.parent for runs_dir in root.rglob("runs") if runs_dir.is_dir()}
    return sorted(discovered)
