from __future__ import annotations

import pytest

from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.exceptions import InvalidHypothesisSchemaError


def test_deterministic_grid_materialization_and_hashes() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h2.yaml")
    one = contract.materialize_grid()
    two = contract.materialize_grid()
    assert one == two
    assert one[0]["config_hash"] == contract.fingerprint_variant(one[0]["params"])


def test_empty_grid_fails_loudly() -> None:
    with pytest.raises(InvalidHypothesisSchemaError):
        HypothesisContract.from_dict(
            {
                "hypothesis_id": "X",
                "title": "X",
                "description": "",
                "research_layer": "L1",
                "hypothesis_family": "f",
                "version": "1",
                "required_indicators": ["adx"],
                "parameter_grid": {},
            }
        )
