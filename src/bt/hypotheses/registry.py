"""Helpers for loading hypothesis contracts from YAML directories."""
from __future__ import annotations

from pathlib import Path

from bt.hypotheses.contract import HypothesisContract


def load_contract(path: str | Path) -> HypothesisContract:
    return HypothesisContract.from_yaml(path)


def load_contracts(root: str | Path) -> dict[str, HypothesisContract]:
    root_path = Path(root)
    contracts: dict[str, HypothesisContract] = {}
    for path in sorted(root_path.glob("*.yaml")):
        contract = HypothesisContract.from_yaml(path)
        contracts[contract.schema.metadata.hypothesis_id] = contract
    return contracts
