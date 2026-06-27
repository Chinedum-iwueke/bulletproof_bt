class BinanceAdapterError(RuntimeError):
    """Base Binance execution-adapter error."""


class BinanceConfigError(BinanceAdapterError):
    """Invalid or unsafe Binance connector configuration."""


class BinanceAuthError(BinanceAdapterError):
    """Binance rejected connector authentication."""


class BinanceTransportError(BinanceAdapterError):
    """Binance could not be reached within the bounded retry policy."""


class BinanceAPIError(BinanceAdapterError):
    def __init__(self, *, code: int, message: str, endpoint: str) -> None:
        super().__init__(f"Binance API error code={code} endpoint={endpoint}: {message}")
        self.code = code
        self.endpoint = endpoint
