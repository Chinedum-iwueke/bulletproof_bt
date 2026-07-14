from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bt.metrics.performance import compute_performance, write_performance_artifacts
from bt.metrics.r_metrics import summarize_r


def _write_equity(path: Path, values: list[float]) -> None:
    pd.DataFrame({"equity": values}).to_csv(path, index=False)


def _write_trades(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_summarize_r_basic_correctness() -> None:
    summary = summarize_r([1.0, -0.5, 2.0, None])

    assert summary.n == 3
    assert summary.ev_r == pytest.approx((1.0 - 0.5 + 2.0) / 3.0)
    assert summary.win_rate == pytest.approx(2.0 / 3.0)
    assert summary.avg_r_win == pytest.approx((1.0 + 2.0) / 2.0)
    assert summary.avg_r_loss == pytest.approx(-0.5)
    assert summary.profit_factor_r == pytest.approx((1.0 + 2.0) / 0.5)
    assert summary.payoff_ratio_r == pytest.approx(summary.avg_r_win / abs(summary.avg_r_loss))


def test_summarize_r_empty_values_returns_none_metrics() -> None:
    summary = summarize_r([None, None])

    assert summary.n == 0
    assert summary.ev_r is None
    assert summary.win_rate is None
    assert summary.avg_r_win is None
    assert summary.avg_r_loss is None
    assert summary.sum_r_pos is None
    assert summary.sum_r_neg_abs is None
    assert summary.profit_factor_r is None
    assert summary.payoff_ratio_r is None




def test_summarize_r_ignores_non_numeric_tokens() -> None:
    summary = summarize_r([1.0, "{}", "bad", -0.5, None])

    assert summary.n == 2
    assert summary.ev_r == pytest.approx(0.25)
    assert summary.win_rate == pytest.approx(0.5)

def test_performance_json_includes_r_metrics_when_present(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_r"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100.0, 110.0, 108.0, 115.0])
    trades = [
        {"pnl_net": 10.0, "fees": -1.0, "slippage": -0.5, "r_multiple_net": 1.0, "r_multiple_gross": 1.2},
        {"pnl_net": -5.0, "fees": -0.5, "slippage": -0.2, "r_multiple_net": -0.5, "r_multiple_gross": -0.4},
        {"pnl_net": 7.0, "fees": -0.7, "slippage": -0.3, "r_multiple_net": 2.0, "r_multiple_gross": 2.2},
    ]
    _write_trades(run_dir / "trades.csv", trades)

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    expected_net = summarize_r([1.0, -0.5, 2.0])
    expected_gross = summarize_r([1.2, -0.4, 2.2])

    for key in [
        "ev_r_gross",
        "ev_r_net",
        "avg_r_win",
        "avg_r_loss",
        "profit_factor_r",
        "payoff_ratio_r",
    ]:
        assert key in payload

    assert payload["ev_r_net"] == pytest.approx(expected_net.ev_r)
    assert payload["ev_r_gross"] == pytest.approx(expected_gross.ev_r)
    assert payload["win_rate_r"] == pytest.approx(expected_net.win_rate)
    assert payload["avg_r_win"] == pytest.approx(expected_net.avg_r_win)
    assert payload["avg_r_loss"] == pytest.approx(expected_net.avg_r_loss)
    assert payload["profit_factor_r"] == pytest.approx(expected_net.profit_factor_r)
    assert payload["payoff_ratio_r"] == pytest.approx(expected_net.payoff_ratio_r)


def test_performance_json_legacy_run_without_r_fields_is_resilient(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_legacy"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100.0, 101.0, 99.0])
    _write_trades(
        run_dir / "trades.csv",
        [
            {"pnl_net": 1.0, "fees": -0.1, "slippage": -0.1},
            {"pnl_net": -2.0, "fees": -0.2, "slippage": -0.1},
        ],
    )

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    for key in [
        "ev_r_gross",
        "ev_r_net",
        "win_rate_r",
        "avg_r_win",
        "avg_r_loss",
        "profit_factor_r",
        "payoff_ratio_r",
    ]:
        assert key in payload
        assert payload[key] is None


def test_performance_prefers_derived_r_when_reported_r_is_inconsistent(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_inconsistent_r"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100.0, 98.0, 96.0])
    _write_trades(
        run_dir / "trades.csv",
        [
            {"pnl_net": -2.0, "risk_amount": 1.0, "r_multiple_net": 9999.0},
            {"pnl_net": -1.0, "risk_amount": 1.0, "r_multiple_net": 8888.0},
        ],
    )

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)
    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))

    # Derived truth should be [-2.0, -1.0], not the corrupted reported values.
    assert payload["ev_r_net"] == pytest.approx(-1.5)
    assert payload["win_rate_r"] == pytest.approx(0.0)
    assert payload["avg_r_win"] is None
    assert payload["avg_r_loss"] == pytest.approx(-1.5)
    notes = payload.get("extra", {}).get("notes", [])
    assert any("r_multiple_net: reported_values_inconsistent_with_pnl_and_risk_amount_using_derived" in note for note in notes)


def test_performance_writes_validation_and_marks_invalid_when_reconciliation_fails(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_invalid"
    run_dir.mkdir()

    # Equity implies negative net pnl, while trade rows are zeroed -> reconciliation should fail.
    pd.DataFrame({"equity": [100000.0, 79217.0]}).to_csv(run_dir / "equity.csv", index=False)
    _write_trades(
        run_dir / "trades.csv",
        [{"pnl_net": 0.0, "pnl_price": 5.0, "fees_paid": 10.0, "slippage": 5.0, "spread_cost": 2.0, "risk_amount": 0.0, "r_net": 999.0}],
    )

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    perf = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    validation = json.loads((run_dir / "performance_validation.json").read_text(encoding="utf-8"))
    assert perf["metrics_valid"] is False
    assert validation["passed"] is False
    assert validation["errors"]


def test_performance_validation_treats_slippage_and_spread_as_embedded_diagnostics(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_embedded_costs"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100000.0, 99940.0])
    _write_trades(
        run_dir / "trades.csv",
        [
            {
                "pnl_price": -50.0,
                "pnl_net": -60.0,
                "fees_paid": 10.0,
                "slippage": 7.0,
                "spread_cost": 3.0,
                "risk_amount": 100.0,
                "r_net": -0.6,
            }
        ],
    )
    (run_dir / "fills.jsonl").write_text(
        '{"fee": 10.0, "slippage": 7.0, "spread_cost": 3.0}\n',
        encoding="utf-8",
    )

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    perf = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    validation = json.loads((run_dir / "performance_validation.json").read_text(encoding="utf-8"))
    assert perf["metrics_valid"] is True
    assert validation["passed"] is True
    assert validation["reconciliation"]["gross_minus_cash_costs"] == pytest.approx(-60.0)
    assert validation["reconciliation"]["diagnostic_slippage_total"] == pytest.approx(7.0)
    assert validation["reconciliation"]["diagnostic_spread_total"] == pytest.approx(3.0)


def test_performance_validation_marks_negative_free_margin_invalid(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_margin_breach"
    run_dir.mkdir()

    pd.DataFrame(
        {
            "ts": ["2024-01-01T00:00:00+00:00", "2024-01-01T00:01:00+00:00"],
            "cash": [100000.0, 100000.0],
            "equity": [100000.0, 90000.0],
            "realized_pnl": [0.0, 0.0],
            "unrealized_pnl": [0.0, -10000.0],
            "used_margin": [10000.0, 95000.0],
            "free_margin": [90000.0, -5000.0],
        }
    ).to_csv(run_dir / "equity.csv", index=False)
    pd.DataFrame(columns=["pnl_net", "pnl_price", "fees_paid", "risk_amount", "r_net"]).to_csv(
        run_dir / "trades.csv", index=False
    )

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    perf = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    validation = json.loads((run_dir / "performance_validation.json").read_text(encoding="utf-8"))
    assert perf["metrics_valid"] is False
    assert perf["margin"]["negative_free_margin_bars"] == 1
    assert perf["margin"]["margin_breach"] is True
    assert any("negative_free_margin_margin_call_breach" in error for error in validation["errors"])


def test_tier2a_signal_episode_treats_negative_free_margin_as_diagnostic(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_tier2a_margin_diagnostic"
    run_dir.mkdir()
    (run_dir / "config_used.yaml").write_text(
        "research_mode: signal_episode\nresearch_tier: tier2a\n"
        "research:\n"
        "  research_mode: signal_episode\n"
        "  research_tier: tier2a\n"
        "  portfolio_constraints_applied: false\n",
        encoding="utf-8",
    )

    pd.DataFrame(
        {
            "ts": ["2024-01-01T00:00:00+00:00", "2024-01-01T00:01:00+00:00"],
            "cash": [100000.0, 100000.0],
            "equity": [100000.0, 100000.0],
            "realized_pnl": [0.0, 0.0],
            "unrealized_pnl": [0.0, 0.0],
            "used_margin": [10000.0, 105000.0],
            "free_margin": [90000.0, -5000.0],
        }
    ).to_csv(run_dir / "equity.csv", index=False)
    pd.DataFrame(columns=["pnl_net", "pnl_price", "fees_paid", "risk_amount", "r_net"]).to_csv(
        run_dir / "trades.csv", index=False
    )

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    perf = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    validation = json.loads((run_dir / "performance_validation.json").read_text(encoding="utf-8"))
    assert perf["metrics_valid"] is True
    assert validation["passed"] is True
    assert validation["portfolio_validation_policy"] == "diagnostic_only_for_signal_episode"
    assert validation["errors"] == []
    assert any("negative_free_margin_margin_call_breach" in error for error in validation["portfolio_diagnostic_errors"])


def test_tier2a_signal_episode_keeps_invalid_r_denominator_fatal(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_tier2a_bad_r"
    run_dir.mkdir()
    (run_dir / "config_used.yaml").write_text(
        "research_mode: signal_episode\nresearch_tier: tier2a\n"
        "research:\n"
        "  research_mode: signal_episode\n"
        "  research_tier: tier2a\n"
        "  portfolio_constraints_applied: false\n",
        encoding="utf-8",
    )
    _write_equity(run_dir / "equity.csv", [100000.0, 100000.0])
    _write_trades(
        run_dir / "trades.csv",
        [{"pnl_net": 0.0, "pnl_price": 0.0, "fees_paid": 0.0, "risk_amount": 0.0, "r_net": 1.0}],
    )

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    perf = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    validation = json.loads((run_dir / "performance_validation.json").read_text(encoding="utf-8"))
    assert perf["metrics_valid"] is False
    assert validation["passed"] is False
    assert "r_net_present_when_risk_amount_missing_or_nonpositive" in validation["errors"]
