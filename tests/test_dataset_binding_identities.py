"""Pure shared identity checks, not admission or authority certification."""
from copy import deepcopy

import pytest

from bt.governance.dataset_bindings import (
    DatasetBindingError,
    verify_dataset_binding_identities,
)


def basket():
    return [dict(instrument=symbol, dataset_digest=char * 64,
                 catalog_digest="c" * 64, producer_receipt_id="producer",
                 partition_digests=[char * 64], venue="bybit")
            for symbol, char in [("ETHUSDT", "a"), ("SOLUSDT", "b")]]


def test_reordering_normalizes_without_mutating_or_aliasing():
    expected = basket()
    actual = deepcopy(expected)[::-1]
    actual[0]["instruments"] = [actual[0].pop("instrument")]
    before = deepcopy(actual)
    result = verify_dataset_binding_identities(actual, expected)
    assert set(result) == {"ETHUSDT", "SOLUSDT"}
    assert actual == before and expected == basket()
    result["SOLUSDT"]["partition_digests"].append("f" * 64)
    assert actual == before


@pytest.mark.parametrize("case", ["missing_member", "duplicate_member", "extra_member",
    "missing_identity", "changed_identity", "unhashable_identity", "non_mapping"])
def test_malformed_basket_rejected_against_independent_expected(case):
    expected = basket()
    actual = deepcopy(expected)
    if case == "missing_member":
        actual.pop()
    elif case == "duplicate_member":
        actual[1] = deepcopy(actual[0])
    elif case == "extra_member":
        actual.append(deepcopy(actual[0]))
    elif case == "missing_identity":
        actual[0].pop("catalog_digest")
    elif case == "changed_identity":
        actual[0]["producer_receipt_id"] = "other"
    elif case == "unhashable_identity":
        actual[0]["instrument"] = []
    else:
        actual[0] = None
    with pytest.raises(DatasetBindingError):
        verify_dataset_binding_identities(actual, expected)


def test_missing_declared_null_is_not_equal_to_present_null():
    expected = basket()
    expected[0]["optional_identity"] = None
    with pytest.raises(DatasetBindingError, match="immutable evidence"):
        verify_dataset_binding_identities(basket(), expected)
    assert verify_dataset_binding_identities(expected, expected)


def test_matching_untrusted_inputs_do_not_establish_authority():
    # A successful comparison is not a certificate: trust must come from outside.
    result = verify_dataset_binding_identities(basket(), basket())
    assert all("execution_authority" not in item for item in result.values())


def test_duplicate_expected_symbols_rejected_even_when_actual_matches():
    duplicated = [basket()[0], basket()[0]]
    with pytest.raises(DatasetBindingError, match="basket identity"):
        verify_dataset_binding_identities(duplicated, duplicated)
