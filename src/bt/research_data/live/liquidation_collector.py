"""Forward-only liquidation collection and aggregation."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import ssl
import struct
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator
from urllib.parse import urlparse

import pandas as pd

from bt.research_data.schemas import LIQUIDATION_1M_COLUMNS, LIQUIDATION_EVENT_COLUMNS
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import timeframe_to_pandas_freq, utc_ts


@dataclass(frozen=True)
class LiquidationEvent:
    ts: pd.Timestamp
    exchange: str
    native_symbol: str
    canonical_symbol: str
    side: str
    price: float
    qty: float
    notional: float
    event_id: str
    raw: str

    def to_json(self) -> str:
        payload = asdict(self)
        payload["ts"] = utc_ts(self.ts).isoformat()
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


class WebSocketJSONClient:
    """Small stdlib websocket client with ping/pong support."""

    def __init__(self, url: str, subscribe_message: dict[str, object] | None = None, timeout: float = 30.0) -> None:
        self.url = url
        self.subscribe_message = subscribe_message
        self.timeout = timeout

    def iter_messages(self) -> Iterator[dict[str, object]]:
        while True:
            sock = None
            try:
                sock = self._connect()
                if self.subscribe_message is not None:
                    self._send_text(sock, json.dumps(self.subscribe_message))
                while True:
                    opcode, payload = self._recv_frame(sock)
                    if opcode == 0x1:
                        yield json.loads(payload.decode("utf-8"))
                    elif opcode == 0x8:
                        break
                    elif opcode == 0x9:
                        self._send_frame(sock, 0xA, payload)
            finally:
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass

    def _connect(self) -> ssl.SSLSocket:
        parsed = urlparse(self.url)
        if parsed.scheme != "wss":
            raise ValueError("only wss websocket URLs are supported")
        host = parsed.hostname or ""
        port = parsed.port or 443
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        raw = socket.create_connection((host, port), timeout=self.timeout)
        sock = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
        sock.settimeout(self.timeout)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        sock.sendall(request.encode("ascii"))
        response = b""
        while b"\r\n\r\n" not in response:
            response += sock.recv(4096)
        if b" 101 " not in response.split(b"\r\n", 1)[0]:
            raise ConnectionError(f"websocket handshake failed: {response[:120]!r}")
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
        if accept not in response:
            raise ConnectionError("websocket accept key mismatch")
        return sock

    def _recv_exact(self, sock: ssl.SSLSocket, n: int) -> bytes:
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("websocket closed")
            data += chunk
        return data

    def _recv_frame(self, sock: ssl.SSLSocket) -> tuple[int, bytes]:
        first, second = self._recv_exact(sock, 2)
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_exact(sock, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(sock, 8))[0]
        mask = self._recv_exact(sock, 4) if masked else b""
        payload = self._recv_exact(sock, length) if length else b""
        if masked:
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        return opcode, payload

    def _send_text(self, sock: ssl.SSLSocket, text: str) -> None:
        self._send_frame(sock, 0x1, text.encode("utf-8"))

    def _send_frame(self, sock: ssl.SSLSocket, opcode: int, payload: bytes = b"") -> None:
        mask = os.urandom(4)
        length = len(payload)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.extend([0x80 | 126])
            header.extend(struct.pack("!H", length))
        else:
            header.extend([0x80 | 127])
            header.extend(struct.pack("!Q", length))
        masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        sock.sendall(bytes(header) + mask + masked)


def append_liquidation_event(store: ResearchDataStore, event: LiquidationEvent) -> bool:
    path = store.raw_jsonl_path(event.exchange, event.native_symbol, "liquidations", "event")
    path.parent.mkdir(parents=True, exist_ok=True)
    seen = _load_event_ids(path)
    if event.event_id and event.event_id in seen:
        return False
    with path.open("a", encoding="utf-8") as handle:
        handle.write(event.to_json() + "\n")
        handle.flush()
        try:
            os.fsync(handle.fileno())
        except OSError:
            pass
    return True


class LiquidationJsonlWriter:
    def __init__(self, store: ResearchDataStore) -> None:
        self.store = store
        self._seen_by_path: dict[Path, set[str]] = {}

    def append(self, event: LiquidationEvent) -> bool:
        path = self.store.raw_jsonl_path(event.exchange, event.native_symbol, "liquidations", "event")
        path.parent.mkdir(parents=True, exist_ok=True)
        seen = self._seen_by_path.setdefault(path, _load_event_ids(path))
        if event.event_id and event.event_id in seen:
            return False
        with path.open("a", encoding="utf-8") as handle:
            handle.write(event.to_json() + "\n")
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError:
                pass
        if event.event_id:
            seen.add(event.event_id)
        return True


def _load_event_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    ids: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                event_id = json.loads(line).get("event_id")
            except json.JSONDecodeError:
                continue
            if event_id:
                ids.add(str(event_id))
    return ids


def read_liquidation_events(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=LIQUIDATION_EVENT_COLUMNS)
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            payload["ts"] = pd.to_datetime(payload["ts"], utc=True)
            rows.append(payload)
    if not rows:
        return pd.DataFrame(columns=LIQUIDATION_EVENT_COLUMNS)
    return pd.DataFrame(rows, columns=LIQUIDATION_EVENT_COLUMNS).drop_duplicates("event_id", keep="last")


def aggregate_liquidation_frame(events: pd.DataFrame, timeframe: str = "1m") -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=LIQUIDATION_1M_COLUMNS)
    df = events.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    freq = timeframe_to_pandas_freq(timeframe)
    df["bucket_ts"] = df["ts"].dt.floor(freq)
    df["side_norm"] = df["side"].astype(str).str.lower()
    df["buy_qty"] = df["qty"].where(df["side_norm"].eq("buy"), 0.0)
    df["sell_qty"] = df["qty"].where(df["side_norm"].eq("sell"), 0.0)
    df["buy_notional"] = df["notional"].where(df["side_norm"].eq("buy"), 0.0)
    df["sell_notional"] = df["notional"].where(df["side_norm"].eq("sell"), 0.0)
    grouped = (
        df.groupby(["bucket_ts", "exchange", "native_symbol", "canonical_symbol"], as_index=False)
        .agg(
            liq_buy_qty=("buy_qty", "sum"),
            liq_sell_qty=("sell_qty", "sum"),
            liq_buy_notional=("buy_notional", "sum"),
            liq_sell_notional=("sell_notional", "sum"),
            liq_event_count=("event_id", "count"),
        )
        .rename(columns={"bucket_ts": "ts"})
    )
    return grouped[list(LIQUIDATION_1M_COLUMNS)].sort_values(["exchange", "native_symbol", "ts"]).reset_index(drop=True)


def aggregate_liquidations(exchange: str, timeframe: str = "1m", store: ResearchDataStore | None = None) -> pd.DataFrame:
    store = store or ResearchDataStore()
    roots = [store.root / "raw" / "perp" / exchange, store.root / "raw" / exchange]
    frames: list[pd.DataFrame] = []
    if not any(root.exists() for root in roots):
        return pd.DataFrame(columns=LIQUIDATION_1M_COLUMNS)
    paths = sorted({path for root in roots for path in root.glob("*/liquidations/timeframe=event/data.jsonl")})
    for path in paths:
        events = read_liquidation_events(path)
        if events.empty:
            continue
        aggregated = aggregate_liquidation_frame(events, timeframe)
        if aggregated.empty:
            continue
        symbol = str(aggregated["native_symbol"].iloc[0])
        store.upsert_parquet(
            store.canonical_path(exchange, symbol, timeframe, f"liquidation_{timeframe}"),
            aggregated,
            key=("exchange", "native_symbol", "ts"),
            columns=LIQUIDATION_1M_COLUMNS,
        )
        frames.append(aggregated)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=LIQUIDATION_1M_COLUMNS)


def collect_liquidations(exchange: str, symbols: list[str], store: ResearchDataStore | None = None) -> None:
    store = store or ResearchDataStore()
    writer = LiquidationJsonlWriter(store)
    backoff = 1.0
    while True:
        try:
            for event in _iter_exchange_liquidations(exchange, symbols):
                writer.append(event)
            backoff = 1.0
        except KeyboardInterrupt:
            raise
        except Exception:
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)


def _iter_exchange_liquidations(exchange: str, symbols: list[str]) -> Iterator[LiquidationEvent]:
    if exchange == "binance":
        from bt.research_data.live.binance_ws import iter_liquidations
    elif exchange == "bybit":
        from bt.research_data.live.bybit_ws import iter_liquidations
    elif exchange == "okx":
        from bt.research_data.live.okx_ws import iter_liquidations
    else:
        raise ValueError(f"unsupported liquidation exchange: {exchange}")
    yield from iter_liquidations(symbols)
