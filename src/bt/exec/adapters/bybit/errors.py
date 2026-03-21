from __future__ import annotations

from dataclasses import dataclass


class BybitAdapterError(RuntimeError):
    """Base error for Bybit adapter failures."""


class BybitConfigError(BybitAdapterError):
    """Raised for invalid or unsafe Bybit configuration."""


class BybitAuthError(BybitAdapterError):
    """Raised for authentication/signature failures."""


class BybitTransportError(BybitAdapterError):
    """Raised for transport-level failures (timeouts, DNS, etc.)."""


@dataclass(frozen=True)
class BybitAPIError(BybitAdapterError):
    """Normalized error returned from Bybit API responses."""

    ret_code: int
    ret_msg: str
    endpoint: str

    def __str__(self) -> str:
        return f"BybitAPIError(ret_code={self.ret_code}, ret_msg={self.ret_msg}, endpoint={self.endpoint})"
