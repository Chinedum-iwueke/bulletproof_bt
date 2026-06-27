import json
import time

from bt.exec.adapters.binance.client_ws_private import BinancePrivateWSClient
from bt.exec.adapters.binance.mapper import map_private_message
from bt.exec.adapters.bybit.client_ws_private import BybitPrivateWSClient
from bt.exec.events.broker_events import BrokerOrderFilledEvent, BrokerPositionSnapshotEvent
from bt.exec.services.connector_certification import REQUIRED_CHECKS, REQUIRED_FAULTS, certify_connector


class Rest:
    def api_post(self, _endpoint):
        return type("Response", (), {"result": {"listenKey": "listen"}})()

    def api_put(self, _endpoint, params=None):
        return None

    def api_delete(self, _endpoint, params=None):
        return None


class Socket:
    def __init__(self):
        self.closed = False

    def recv(self):
        time.sleep(.01)
        return json.dumps({"e":"ORDER_TRADE_UPDATE","E":1,"o":{"i":1,"s":"BTCUSDT","S":"BUY","o":"MARKET","X":"FILLED","q":"1","z":"1","l":"1","L":"100","T":1,"n":".1"}})

    def close(self):
        self.closed = True


class BybitSocket:
    def __init__(self):
        self.sent: list[dict] = []
        self.reads = 0
        self.closed = False

    def send(self, payload: str) -> None:
        self.sent.append(json.loads(payload))

    def recv(self) -> str:
        self.reads += 1
        if self.reads == 1:
            return json.dumps({"success": True, "op": "auth"})
        time.sleep(.01)
        return json.dumps({"topic": "wallet", "data": [{"coin": [{"coin": "USDT", "walletBalance": "100"}]}]})

    def close(self) -> None:
        self.closed = True


def test_binance_private_stream_lifecycle_and_event_mapping() -> None:
    client = BinancePrivateWSClient(rest=Rest(),ws_base_url="wss://example/ws",product_type="perpetual",socket_factory=lambda _url:Socket())
    client.start()
    time.sleep(.04)
    messages = client.drain_messages()
    client.stop()
    assert messages
    assert any(isinstance(event,BrokerOrderFilledEvent) for event in map_private_message(ts=messages[0].ts,payload=messages[0].payload))
    account={"e":"ACCOUNT_UPDATE","a":{"B":[{"a":"USDT","wb":"100"}],"P":[{"s":"BTCUSDT","pa":"1","ep":"100","up":"2"}]}}
    assert any(isinstance(event,BrokerPositionSnapshotEvent) for event in map_private_message(ts=messages[0].ts,payload=account))


def test_binance_and_bybit_require_identical_certification_contract() -> None:
    checks = {name: True for name in REQUIRED_CHECKS}
    faults = {name: True for name in REQUIRED_FAULTS}
    for venue in ("binance","bybit"):
        for product in ("spot","perpetual"):
            assert certify_connector(venue=venue,environment="demo",product_type=product,checks=checks,fault_tests=faults).status=="certified"
    broken=dict(checks,private_stream_auth=False)
    result=certify_connector(venue="binance",environment="live",product_type="perpetual",checks=broken,fault_tests=faults)
    assert result.status=="blocked" and "check:private_stream_auth" in result.blockers


def test_bybit_private_stream_authenticates_and_subscribes() -> None:
    socket = BybitSocket()
    client = BybitPrivateWSClient(
        url="wss://example/private",
        topics=["order", "execution", "position", "wallet"],
        api_key="key",
        api_secret="secret",
        socket_factory=lambda _url: socket,
    )
    client.start()
    time.sleep(.04)
    messages = client.drain_messages()
    health = client.health()
    client.stop()
    assert health.metadata["authenticated"] is True
    assert any(item.get("op") == "subscribe" for item in socket.sent)
    assert any(message.topic == "wallet" for message in messages)
