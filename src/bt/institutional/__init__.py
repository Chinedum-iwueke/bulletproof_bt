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
from .execution_calibration import (
    ExecutionCalibrationError,
    calibrate_execution_quality,
    execution_calibration_receipt,
)
from .capacity import (
    PortfolioCapacityError,
    capacity_dossier_receipt,
    portfolio_capacity,
)
from .risk_budget import (
    RISK_BUDGET_SCHEMA_VERSION,
    RISK_BUDGET_SPECIFICATION,
    DynamicRiskBudgetError,
    dynamic_risk_budget,
    dynamic_risk_budget_receipt,
)
from .shadow_monitoring import (
    SHADOW_MONITORING_SCHEMA_VERSION,
    SHADOW_MONITORING_SPECIFICATION,
    ShadowMonitoringError,
    monitor_shadow_candidate,
    shadow_monitoring_receipt,
)

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
    "ExecutionCalibrationError",
    "calibrate_execution_quality",
    "execution_calibration_receipt",
    "PortfolioCapacityError",
    "capacity_dossier_receipt",
    "portfolio_capacity",
    "RISK_BUDGET_SCHEMA_VERSION",
    "RISK_BUDGET_SPECIFICATION",
    "DynamicRiskBudgetError",
    "dynamic_risk_budget",
    "dynamic_risk_budget_receipt",
    "microstructure_state",
    "microstructure_state_receipt",
    "oms_reconciliation_receipt",
    "reconcile_oms",
    "replay_oms",
    "dependency_dossier_receipt",
    "verify_receipt",
    "SHADOW_MONITORING_SCHEMA_VERSION",
    "SHADOW_MONITORING_SPECIFICATION",
    "ShadowMonitoringError",
    "monitor_shadow_candidate",
    "shadow_monitoring_receipt",
]
