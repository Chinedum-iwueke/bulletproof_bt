"""Authoritative quantitative producers for Hermes-governed research."""

from .candidate_admission import (
    ADMISSION_SCHEMA_VERSION,
    ADMISSION_SPECIFICATION,
    CandidateAdmissionError,
    candidate_admission_receipt,
)
from .capacity import (
    PortfolioCapacityError,
    capacity_dossier_receipt,
    portfolio_capacity,
)
from .construction import PortfolioConstructionError, construction_dossier_receipt
from .execution import (
    CanonicalEventError,
    CanonicalEventJournal,
    CanonicalExecutionEvent,
    canonical_event,
    execution_journal_receipt,
    verify_event,
)
from .execution_calibration import (
    ExecutionCalibrationError,
    calibrate_execution_quality,
    execution_calibration_receipt,
)
from .execution_degradation import (
    EXECUTION_DEGRADATION_SCHEMA_VERSION,
    EXECUTION_DEGRADATION_SPECIFICATION,
    ExecutionDegradationError,
    evaluate_execution_degradation,
    execution_degradation_receipt,
)
from .microstructure import (
    MicrostructureStateError,
    microstructure_state,
    microstructure_state_receipt,
)
from .oms import OmsError, oms_reconciliation_receipt, reconcile_oms, replay_oms
from .portfolio import PortfolioDependencyError, dependency_dossier_receipt
from .realtime_risk import (
    REALTIME_RISK_SCHEMA_VERSION,
    REALTIME_RISK_SPECIFICATION,
    RealtimeRiskError,
    order_binding,
    realtime_risk_decision_receipt,
    require_realtime_risk_authorization,
)
from .receipt import ProducerReceipt, build_receipt, verify_receipt
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
    "ADMISSION_SCHEMA_VERSION",
    "ADMISSION_SPECIFICATION",
    "EXECUTION_DEGRADATION_SCHEMA_VERSION",
    "EXECUTION_DEGRADATION_SPECIFICATION",
    "REALTIME_RISK_SCHEMA_VERSION",
    "REALTIME_RISK_SPECIFICATION",
    "RISK_BUDGET_SCHEMA_VERSION",
    "RISK_BUDGET_SPECIFICATION",
    "SHADOW_MONITORING_SCHEMA_VERSION",
    "SHADOW_MONITORING_SPECIFICATION",
    "CandidateAdmissionError",
    "CanonicalEventError",
    "CanonicalEventJournal",
    "CanonicalExecutionEvent",
    "DynamicRiskBudgetError",
    "ExecutionCalibrationError",
    "ExecutionDegradationError",
    "MicrostructureStateError",
    "OmsError",
    "PortfolioCapacityError",
    "PortfolioConstructionError",
    "PortfolioDependencyError",
    "ProducerReceipt",
    "RealtimeRiskError",
    "ShadowMonitoringError",
    "build_receipt",
    "calibrate_execution_quality",
    "candidate_admission_receipt",
    "canonical_event",
    "capacity_dossier_receipt",
    "construction_dossier_receipt",
    "dependency_dossier_receipt",
    "dynamic_risk_budget",
    "dynamic_risk_budget_receipt",
    "evaluate_execution_degradation",
    "execution_calibration_receipt",
    "execution_degradation_receipt",
    "execution_journal_receipt",
    "microstructure_state",
    "microstructure_state_receipt",
    "monitor_shadow_candidate",
    "oms_reconciliation_receipt",
    "order_binding",
    "portfolio_capacity",
    "realtime_risk_decision_receipt",
    "reconcile_oms",
    "replay_oms",
    "require_realtime_risk_authorization",
    "shadow_monitoring_receipt",
    "verify_event",
    "verify_receipt",
]
