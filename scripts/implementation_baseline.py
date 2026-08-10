#!/usr/bin/env python3
"""Collect deterministic, secret-safe implementation baseline metadata."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

SCHEMA_VERSION = "implementation-baseline-v1"
CLAIM_VOCABULARY = [
    "current",
    "partial",
    "documented",
    "tested",
    "observed",
    "target",
    "extension",
    "deferred",
    "unknown",
    "blocked",
]
DEPENDENCY_PATTERNS = (
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    "package.json",
    "Dockerfile",
    "compose.yaml",
    "docker-compose.yml",
)
LOCK_NAMES = (
    "uv.lock",
    "poetry.lock",
    "Pipfile.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
)
PROFILES = {
    "swarm-control-plane": {
        "acceptance_commands": [
            ["python", "-m", "compileall", "-q", "backend/app", "worker/src"],
            ["pytest", "-q", "backend/tests"],
            ["pytest", "-q", "worker/tests"],
            ["ruff", "check", "backend/app", "backend/tests", "worker/src", "worker/tests"],
        ]
    },
    "bulletproof_bt": {
        "acceptance_commands": [
            ["python", "-m", "compileall", "-q", "src", "schemas"],
            ["pytest", "--collect-only", "-q"],
            ["pytest", "-q"],
            ["ruff", "check", "src", "tests"],
        ]
    },
}


class BaselineError(RuntimeError):
    """The baseline cannot be collected or validated safely."""


def _run(args: list[str], cwd: Path, *, required: bool = True) -> str | None:
    env = {
        key: value
        for key, value in os.environ.items()
        if key in {"PATH", "HOME", "LANG", "LC_ALL", "SYSTEMROOT"}
    }
    try:
        completed = subprocess.run(
            args,
            cwd=cwd,
            env=env,
            check=required,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        if required:
            raise BaselineError(f"Required metadata command failed: {args[0]}") from None
        return None
    value = completed.stdout.rstrip() or completed.stderr.rstrip()
    return value or None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_digest(document: dict[str, Any]) -> str:
    material = json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _safe_remote(value: str | None) -> str | None:
    if not value:
        return None
    if "://" in value:
        parsed = urlsplit(value)
        if parsed.scheme == "file":
            return f"file:///{Path(parsed.path).name}"
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        return urlunsplit((parsed.scheme, f"{host}{port}", parsed.path, "", ""))
    scp = re.fullmatch(r"(?:[^@/:]+@)?([^:/]+):(.+)", value)
    if scp:
        return f"ssh://{scp.group(1)}/{scp.group(2)}"
    return Path(value).name if Path(value).is_absolute() else value


def _tracked_files(root: Path, pattern: str | None = None) -> list[Path]:
    args = ["git", "ls-files", "-z"]
    if pattern:
        args.extend(["--", pattern])
    output = _run(args, root) or ""
    return [root / item for item in output.split("\0") if item]


def _file_record(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": _sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _runtime_versions(root: Path) -> list[dict[str, str]]:
    probes = (
        ("python", [sys.executable, "--version"]),
        ("git", ["git", "--version"]),
        ("node", ["node", "--version"]),
        ("docker", ["docker", "--version"]),
    )
    records = []
    for name, command in probes:
        value = _run(command, root, required=False)
        if value:
            records.append({"name": name, "version": value.splitlines()[0]})
    return records


def collect(root: Path, *, require_clean: bool = True) -> dict[str, Any]:
    root = root.resolve()
    if not (root / ".git").exists():
        raise BaselineError("Repository root must contain a .git directory.")
    repository = root.name
    if repository not in PROFILES:
        raise BaselineError(f"Unsupported repository profile: {repository}")

    status = _run(
        [
            "git",
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--no-renames",
        ],
        root,
    ) or ""
    dirty_entries = sorted(
        entry[3:] if len(entry) > 3 else entry
        for entry in status.split("\0")
        if entry
    )
    if require_clean and dirty_entries:
        raise BaselineError(
            "Repository is dirty; commit/stash intentional work or use --allow-dirty."
        )

    tracked = {path.relative_to(root).as_posix(): path for path in _tracked_files(root)}
    dependencies = [
        _file_record(path, root)
        for name, path in sorted(tracked.items())
        if Path(name).name in DEPENDENCY_PATTERNS
    ]
    locks = [
        _file_record(path, root)
        for name, path in sorted(tracked.items())
        if Path(name).name in LOCK_NAMES
    ]
    schemas = [
        _file_record(path, root)
        for name, path in sorted(tracked.items())
        if name.endswith(".schema.json")
    ]
    remote = _run(["git", "remote", "get-url", "origin"], root, required=False)
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "repository": {
            "name": repository,
            "branch": _run(["git", "branch", "--show-current"], root) or "",
            "commit": _run(["git", "rev-parse", "HEAD"], root) or "",
            "origin": _safe_remote(remote),
            "dirty": bool(dirty_entries),
            "dirty_paths": dirty_entries,
        },
        "runtime": {
            "platform": _run(["uname", "-srm"], root, required=False) or "unknown",
            "tools": _runtime_versions(root),
        },
        "dependencies": {
            "manifests": dependencies,
            "locks": locks,
            "lock_present": bool(locks),
        },
        "schemas": schemas,
        "acceptance_commands": PROFILES[repository]["acceptance_commands"],
        "claim_vocabulary": CLAIM_VOCABULARY,
        "safety": {
            "environment_values_collected": False,
            "ignored_files_scanned": False,
            "raw_data_scanned": False,
            "remote_credentials_retained": False,
        },
    }
    document["baseline_digest"] = _canonical_digest(document)
    validate(document)
    return document


def validate(document: dict[str, Any]) -> None:
    def exact_keys(value: Any, keys: set[str], label: str) -> dict[str, Any]:
        if not isinstance(value, dict) or set(value) != keys:
            raise BaselineError(f"{label} fields do not match the v1 contract.")
        return value

    def validate_files(value: Any, label: str) -> None:
        if not isinstance(value, list):
            raise BaselineError(f"{label} must be an array.")
        paths = []
        for item in value:
            record = exact_keys(item, {"path", "sha256", "size_bytes"}, label)
            path = record["path"]
            if (
                not isinstance(path, str)
                or not path
                or Path(path).is_absolute()
                or ".." in Path(path).parts
            ):
                raise BaselineError(f"{label} contains an unsafe path.")
            if not re.fullmatch(r"[0-9a-f]{64}", record["sha256"]):
                raise BaselineError(f"{label} contains an invalid digest.")
            if not isinstance(record["size_bytes"], int) or record["size_bytes"] < 0:
                raise BaselineError(f"{label} contains an invalid size.")
            paths.append(path)
        if paths != sorted(set(paths)):
            raise BaselineError(f"{label} must be uniquely path-sorted.")

    required = {
        "schema_version",
        "repository",
        "runtime",
        "dependencies",
        "schemas",
        "acceptance_commands",
        "claim_vocabulary",
        "safety",
        "baseline_digest",
    }
    if set(document) != required or document["schema_version"] != SCHEMA_VERSION:
        raise BaselineError("Document does not match implementation-baseline-v1.")
    repository = exact_keys(
        document["repository"],
        {"name", "branch", "commit", "origin", "dirty", "dirty_paths"},
        "Repository",
    )
    if repository["name"] not in PROFILES:
        raise BaselineError("Repository profile is unsupported.")
    if not re.fullmatch(r"[0-9a-f]{40}", repository.get("commit", "")):
        raise BaselineError("Repository commit must be a full SHA-1 identifier.")
    if not isinstance(repository["dirty"], bool):
        raise BaselineError("Repository dirty state must be boolean.")
    dirty_paths = repository["dirty_paths"]
    if (
        not isinstance(dirty_paths, list)
        or any(not isinstance(path, str) or not path for path in dirty_paths)
        or dirty_paths != sorted(set(dirty_paths))
        or repository["dirty"] != bool(dirty_paths)
    ):
        raise BaselineError("Repository dirty paths are inconsistent.")
    origin = repository["origin"]
    if origin is not None and (
        not isinstance(origin, str)
        or "@" in urlsplit(origin).netloc
        or urlsplit(origin).query
        or urlsplit(origin).fragment
    ):
        raise BaselineError("Repository origin is not sanitized.")

    runtime = exact_keys(document["runtime"], {"platform", "tools"}, "Runtime")
    if not isinstance(runtime["platform"], str) or not runtime["platform"]:
        raise BaselineError("Runtime platform is invalid.")
    if not isinstance(runtime["tools"], list):
        raise BaselineError("Runtime tools must be an array.")
    for tool in runtime["tools"]:
        record = exact_keys(tool, {"name", "version"}, "Runtime tool")
        if not all(isinstance(record[key], str) and record[key] for key in record):
            raise BaselineError("Runtime tool metadata is invalid.")

    dependencies = exact_keys(
        document["dependencies"], {"manifests", "locks", "lock_present"}, "Dependencies"
    )
    validate_files(dependencies["manifests"], "Dependency manifests")
    validate_files(dependencies["locks"], "Dependency locks")
    if (
        not isinstance(dependencies["lock_present"], bool)
        or dependencies["lock_present"] != bool(dependencies["locks"])
    ):
        raise BaselineError("Dependency lock state is inconsistent.")
    validate_files(document["schemas"], "Schemas")

    commands = document["acceptance_commands"]
    if not isinstance(commands, list) or any(
        not isinstance(command, list)
        or not command
        or any(not isinstance(argument, str) or not argument for argument in command)
        for command in commands
    ):
        raise BaselineError("Acceptance commands must be argument arrays.")
    if document["claim_vocabulary"] != CLAIM_VOCABULARY:
        raise BaselineError("Claim vocabulary is not canonical.")
    safety = exact_keys(
        document["safety"],
        {
            "environment_values_collected",
            "ignored_files_scanned",
            "raw_data_scanned",
            "remote_credentials_retained",
        },
        "Safety",
    )
    if any(value is not False for value in safety.values()):
        raise BaselineError("Safety assertions must remain false.")
    supplied = document.pop("baseline_digest")
    actual = _canonical_digest(document)
    document["baseline_digest"] = supplied
    if supplied != actual:
        raise BaselineError("Baseline digest mismatch.")


def _write(document: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    output.chmod(0o600)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--repository", type=Path, default=Path.cwd())
    collect_parser.add_argument("--output", type=Path, required=True)
    collect_parser.add_argument("--allow-dirty", action="store_true")
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("path", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "collect":
            document = collect(args.repository, require_clean=not args.allow_dirty)
            _write(document, args.output)
            print(json.dumps({"digest": document["baseline_digest"], "output": str(args.output)}))
        else:
            document = json.loads(args.path.read_text(encoding="utf-8"))
            validate(document)
            print(json.dumps({"digest": document["baseline_digest"], "valid": True}))
    except (BaselineError, json.JSONDecodeError, OSError) as exc:
        print(f"implementation baseline failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
