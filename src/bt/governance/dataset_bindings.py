"""Compare dataset identities to an independently trusted contract, without I/O.

This verifies identity equality, not source bytes, membership truth or authority.
Callers must obtain expected bindings from their verified immutable contract, not
from the receipt being checked. The runner separately hashes materialized files.
"""

from copy import deepcopy
from typing import Any


class DatasetBindingError(ValueError):
    """Dataset identity evidence is malformed, incomplete or inconsistent."""


def _indexed(items: list) -> dict[str, dict[str, Any]]:
    result = {}
    for original in items:
        if not isinstance(original, dict):
            raise DatasetBindingError("trusted dataset binding member must be an object")
        item = dict(original)
        source_digest = item.get("dataset_digest")
        if (not isinstance(source_digest, str) or len(source_digest) != 64
                or any(char not in "0123456789abcdef" for char in source_digest)):
            raise DatasetBindingError("trusted dataset binding requires a SHA-256 source identity")
        symbol = item.get("instrument")
        plural = item.get("instruments")
        if symbol is None and isinstance(plural, list) and len(plural) == 1:
            symbol = plural[0]
        if (not isinstance(symbol, str) or not symbol
                or (plural is not None and plural != [symbol]) or symbol in result):
            raise DatasetBindingError("trusted dataset basket identity mismatch")
        item["instrument"] = symbol
        item["instruments"] = [symbol]
        result[symbol] = item
    return result


def verify_dataset_binding_identities(
    bindings: Any, expected_bindings: Any,
) -> dict[str, dict[str, Any]]:
    """Return normalized copies only after exact basket and declared-field checks.

    Extra transport metadata is permitted; missing or changed declared identity
    fields are not. Instrument order is immaterial, partition order is retained.
    """
    if (not isinstance(expected_bindings, list) or not expected_bindings
            or not isinstance(bindings, list) or len(bindings) != len(expected_bindings)):
        raise DatasetBindingError("execution requires every trusted dataset binding")
    actual = _indexed(bindings)
    expected_by_symbol = _indexed(expected_bindings)
    if set(actual) != set(expected_by_symbol):
        raise DatasetBindingError("trusted dataset basket identity mismatch")
    for symbol, expected in expected_by_symbol.items():
        normalized = actual[symbol]
        if ("partition_digest" not in normalized
                and isinstance(normalized.get("partition_digests"), list)
                and len(normalized["partition_digests"]) == 1):
            normalized["partition_digest"] = normalized["partition_digests"][0]
        if "partition_digests" in expected:
            partitions = normalized.get("partition_digests")
            if (not isinstance(partitions, list) or partitions != expected["partition_digests"]
                    or not partitions
                    or any(not isinstance(value, str) or not value for value in partitions)):
                raise DatasetBindingError(f"trusted immutable partition binding mismatch: {symbol}")
            singular = normalized.get("partition_digest")
            if singular is not None and (len(partitions) != 1 or singular != partitions[0]):
                raise DatasetBindingError(f"trusted singular/plural partition binding mismatch: {symbol}")
        # Paths vary by host; all other declared identities must be present.
        if any(key not in normalized or normalized[key] != value
               for key, value in expected.items() if key != "dataset_path"):
            raise DatasetBindingError(f"trusted immutable evidence binding mismatch: {symbol}")
    return deepcopy(actual)
