from __future__ import annotations

import pandas as pd

from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.adapters.bybit.mapper import map_balances, map_fills, map_orders, map_positions
from bt.exec.reconcile import ReconciliationEngine, ReconciliationInputs, ReconciliationPolicy, ReconciliationScope


def test_bybit_mapped_surfaces_reconcile_smoke() -> None:
    ts = pd.Timestamp("2026-01-01T00:00:00Z")
    orders = map_orders({"list": []})
    fills = map_fills({"list": []})
    positions = map_positions({"list": []})
    balances = map_balances({"list": [{"coin": [{"coin": "USDT", "walletBalance": "100"}]}]})

    inputs = ReconciliationInputs(
        run_id="r",
        ts=ts,
        local_open_orders=orders,
        adapter_open_orders=orders,
        adapter_completed_orders=[],
        local_fills=fills,
        adapter_fills=fills,
        local_positions=positions,
        adapter_positions=positions,
        local_balances=BalanceSnapshot(ts=balances.ts, balances=dict(balances.balances)),
        adapter_balances=balances,
        scope=ReconciliationScope(compare_orders=True, compare_fills=True, compare_positions=True, compare_balances=True),
        material_fill_qty_tolerance=0.0,
        material_position_qty_tolerance=0.0,
        material_balance_tolerance=0.0,
    )
    result = ReconciliationEngine().reconcile(inputs=inputs, policy=ReconciliationPolicy.LOG_ONLY)
    assert result.mismatches == []
