"""Hypothesis contract exception types."""


class HypothesisContractError(ValueError):
    """Base error for hypothesis contracts."""


class InvalidHypothesisSchemaError(HypothesisContractError):
    """Raised on schema validation failures."""


class MissingRequiredTierError(HypothesisContractError):
    """Raised when required execution tiers are unavailable."""


class MissingIndicatorDependencyError(HypothesisContractError):
    """Raised when required indicators are not registered."""


class GridMaterializationError(HypothesisContractError):
    """Raised when deterministic parameter expansion fails."""
