"""Authoritative quantitative producers for Hermes-governed research."""

from .receipt import ProducerReceipt, build_receipt, verify_receipt
from .portfolio import PortfolioDependencyError, dependency_dossier_receipt

__all__ = [
    "PortfolioDependencyError",
    "ProducerReceipt",
    "build_receipt",
    "dependency_dossier_receipt",
    "verify_receipt",
]
