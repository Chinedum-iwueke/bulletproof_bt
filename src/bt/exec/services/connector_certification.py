from __future__ import annotations

from dataclasses import dataclass


REQUIRED_CHECKS = (
    "rest_auth",
    "server_time_sync",
    "instrument_metadata",
    "balance_snapshot",
    "order_round_trip",
    "private_stream_auth",
    "private_order_event",
    "private_fill_event",
    "private_position_or_balance_event",
    "restart_reconciliation",
    "live_mutation_lock",
    "emergency_freeze",
)
REQUIRED_FAULTS = (
    "transport_disconnect_freezes",
    "stale_private_stream_freezes",
    "rate_limit_freezes",
    "duplicate_fill_is_idempotent",
    "reconciliation_divergence_freezes",
)


@dataclass(frozen=True)
class ConnectorCertification:
    venue: str
    environment: str
    product_type: str
    status: str
    checks: dict[str, bool]
    fault_tests: dict[str, bool]
    blockers: tuple[str, ...]


def certify_connector(*, venue: str, environment: str, product_type: str, checks: dict[str, bool], fault_tests: dict[str, bool]) -> ConnectorCertification:
    blockers = tuple([f"check:{name}" for name in REQUIRED_CHECKS if checks.get(name) is not True] + [f"fault:{name}" for name in REQUIRED_FAULTS if fault_tests.get(name) is not True])
    return ConnectorCertification(venue=venue,environment=environment,product_type=product_type,status="certified" if not blockers else "blocked",checks={name:checks.get(name) is True for name in REQUIRED_CHECKS},fault_tests={name:fault_tests.get(name) is True for name in REQUIRED_FAULTS},blockers=blockers)

