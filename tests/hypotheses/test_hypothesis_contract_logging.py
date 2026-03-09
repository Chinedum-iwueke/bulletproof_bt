from __future__ import annotations

from bt.hypotheses.logging import REQUIRED_LOG_FIELDS, make_log_row


def test_standardized_logging_fields_present() -> None:
    row = make_log_row({"run_id": "r1"}, {"num_trades": 0})
    for field in REQUIRED_LOG_FIELDS:
        assert field in row
