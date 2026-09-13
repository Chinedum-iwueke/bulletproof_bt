from datetime import UTC, datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from bt.institutional.venue_telemetry import replay_venue_telemetry


SCRIPT = Path(__file__).parents[1] / "scripts" / "demo001_exec011_closure.py"
SPEC = spec_from_file_location("demo001_exec011_closure", SCRIPT)
assert SPEC and SPEC.loader
MODULE = module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_real_history_normalizes_to_flat_reconstructable_trade_episode():
    fetched = datetime(2026, 9, 13, 12, tzinfo=UTC)
    history = {
        "orders": [
            {
                "orderId": "entry",
                "orderLinkId": "demo001-run-entry",
                "updatedTime": "1789300798000",
                "orderStatus": "Filled",
                "side": "Buy",
                "qty": "0.001",
                "price": "0",
                "orderType": "Market",
                "reduceOnly": False,
            },
            {
                "orderId": "exit",
                "orderLinkId": "demo001-run-exit",
                "updatedTime": "1789300799000",
                "orderStatus": "Filled",
                "side": "Sell",
                "qty": "0.001",
                "price": "0",
                "orderType": "Market",
                "reduceOnly": True,
            },
        ],
        "executions": [
            {
                "execId": "fill-entry",
                "orderId": "entry",
                "execTime": "1789300798000",
                "side": "Buy",
                "execQty": "0.001",
                "execPrice": "77000",
                "execFee": "0.04",
                "feeCurrency": "USDT",
            },
            {
                "execId": "fill-exit",
                "orderId": "exit",
                "execTime": "1789300799000",
                "side": "Sell",
                "execQty": "0.001",
                "execPrice": "77100",
                "execFee": "0.04",
                "feeCurrency": "USDT",
            },
        ],
        "positions": [{"symbol": "BTCUSDT", "size": "0", "side": ""}],
        "wallet": [
            {
                "totalWalletBalance": "1000.02",
                "totalInitialMargin": "0",
                "totalMaintenanceMargin": "0",
                "totalAvailableBalance": "1000.02",
            }
        ],
        "owned_open_orders": [],
    }
    events = MODULE.canonical_events(
        history,
        symbol="BTCUSDT",
        account="bybit-demo-pseudonym",
        fetched_at=fetched,
    )
    projection = replay_venue_telemetry(
        events, known_at=datetime(2026, 9, 13, 12, 0, 1, tzinfo=UTC)
    )
    assert projection["status"] == "current"
    assert projection["reconciliation_discrepancies"] == []
    assert len(projection["trade_episodes"]) == 1
    assert projection["trade_episodes"][0]["gross_pnl"] == "0.100"
    assert projection["positions"][0]["quantity"] == "0"
    assert all("api" not in event.payload for event in events)
