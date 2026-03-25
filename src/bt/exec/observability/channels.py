from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib import request

from bt.logging.jsonl import JsonlWriter

from bt.exec.observability.alerts import Alert


class StdoutAlertChannel:
    def send(self, alert: Alert) -> None:
        print(json.dumps(alert.to_jsonable(), sort_keys=True))


class FileAlertChannel:
    def __init__(self, path: Path) -> None:
        self._writer = JsonlWriter(path)

    def send(self, alert: Alert) -> None:
        self._writer.write(alert.to_jsonable())

    def close(self) -> None:
        self._writer.close()


class SlackWebhookAlertChannel:
    def __init__(self, *, webhook_url_env: str) -> None:
        self._webhook = os.getenv(webhook_url_env, "").strip()

    def send(self, alert: Alert) -> None:
        if not self._webhook:
            return
        payload = {"text": f"[{alert.severity.value}] {alert.event_type.value}: {alert.message}"}
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(self._webhook, data=data, method="POST", headers={"Content-Type": "application/json"})
        with request.urlopen(req, timeout=3):
            pass


class TelegramAlertChannel:
    def __init__(self, *, bot_token_env: str, chat_id_env: str) -> None:
        self._bot_token = os.getenv(bot_token_env, "").strip()
        self._chat_id = os.getenv(chat_id_env, "").strip()

    def send(self, alert: Alert) -> None:
        if not self._bot_token or not self._chat_id:
            return
        text = f"[{alert.severity.value}] {alert.event_type.value}: {alert.message}"
        payload: dict[str, Any] = {"chat_id": self._chat_id, "text": text}
        url = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
        with request.urlopen(req, timeout=3):
            pass
