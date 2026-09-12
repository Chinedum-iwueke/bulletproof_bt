"""Authoritative quantitative producers for Hermes-governed research."""

from .receipt import ProducerReceipt, build_receipt, verify_receipt
from .portfolio import PortfolioDependencyError, dependency_dossier_receipt
from .construction import PortfolioConstructionError, construction_dossier_receipt
from .execution import (
    CanonicalEventError,
    CanonicalEventJournal,
    CanonicalExecutionEvent,
    canonical_event,
    execution_journal_receipt,
    verify_event,
)
from .microstructure import MicrostructureStateError, microstructure_state, microstructure_state_receipt
from .oms import OmsError, oms_reconciliation_receipt, reconcile_oms, replay_oms

__all__ = [
    "PortfolioDependencyError",
    "PortfolioConstructionError",
    "ProducerReceipt",
    "build_receipt",
    "construction_dossier_receipt",
    "CanonicalEventError",
    "CanonicalEventJournal",
    "CanonicalExecutionEvent",
    "canonical_event",
    "execution_journal_receipt",
    "verify_event",
    "MicrostructureStateError",
    "OmsError",
    "microstructure_state",
    "microstructure_state_receipt",
    "oms_reconciliation_receipt",
    "reconcile_oms",
    "replay_oms",
    "dependency_dossier_receipt",
    "verify_receipt",
]
