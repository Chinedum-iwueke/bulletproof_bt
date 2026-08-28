"""Common immutable receipt envelope for Bulletproof-native computations."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

SCHEMA_VERSION = "bulletproof-producer-receipt-v1.0.0"


class ProducerReceiptError(ValueError):
    """A producer receipt is malformed or has lost integrity."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def is_sha256(value: object) -> bool:
    text = str(value)
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def is_git_commit(value: object) -> bool:
    text = str(value)
    return len(text) in {40, 64} and all(char in "0123456789abcdef" for char in text)


@dataclass(frozen=True)
class ProducerReceipt:
    schema_version: str
    milestone: str
    producer: str
    producer_version: str
    source_commit: str
    input_digest: str
    dataset_digest: str
    configuration_digest: str
    artifact_digest: str
    result_digest: str
    result: dict[str, Any]
    authority: dict[str, bool]
    receipt_digest: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_receipt(
    *,
    milestone: str,
    producer: str,
    producer_version: str,
    source_commit: str,
    inputs: Any,
    dataset_digest: str,
    configuration: Any,
    artifacts: Any,
    result: dict[str, Any],
) -> ProducerReceipt:
    if not is_sha256(dataset_digest):
        raise ProducerReceiptError("dataset_digest must be a lowercase sha256")
    if not is_git_commit(source_commit):
        raise ProducerReceiptError("source_commit must be a full lowercase Git commit")
    core = {
        "schema_version": SCHEMA_VERSION,
        "milestone": milestone,
        "producer": producer,
        "producer_version": producer_version,
        "source_commit": source_commit,
        "input_digest": digest(inputs),
        "dataset_digest": dataset_digest,
        "configuration_digest": digest(configuration),
        "artifact_digest": digest(artifacts),
        "result_digest": digest(result),
        "result": result,
        "authority": {
            "allocation": False,
            "capital": False,
            "orders": False,
            "promotion": False,
        },
    }
    return ProducerReceipt(**core, receipt_digest=digest(core))


def verify_receipt(receipt: ProducerReceipt | dict[str, Any]) -> bool:
    document = (
        receipt.as_dict() if isinstance(receipt, ProducerReceipt) else dict(receipt)
    )
    receipt_digest = document.pop("receipt_digest", None)
    if document.get("schema_version") != SCHEMA_VERSION:
        return False
    if document.get("result_digest") != digest(document.get("result")):
        return False
    authority = document.get("authority")
    if authority != {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    }:
        return False
    return receipt_digest == digest(document)
