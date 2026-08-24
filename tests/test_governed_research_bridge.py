from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from bt.governance.research_bridge import (
    BridgeLedger,
    BridgeError,
    DatasetBinding,
    HypothesisSubmission,
    compile_submission,
    validate_approved_proposal,
    register_with_hermes,
    materialize_approved_contract,
)


ROOT = Path(__file__).parents[1]


def submission(**changes) -> HypothesisSubmission:
    values = {
        "original_text": "Run the CSI-Gated Displacement Trend at Tier2B with the declared grid.",
        "hypothesis": "CSI-Gated Displacement Trend",
        "tier": "Tier2B",
        "grid": {
            "d0": (1.8, 2.2),
            "theta": (0.7, 0.8),
            "k_stop": (3, 4),
            "k_trail": (3, 5),
        },
        "dataset": DatasetBinding(
            snapshot_id="11111111-1111-4111-8111-111111111111",
            digest="2" * 64,
            available_fields=("ohlcv", "funding", "open_interest", "volume"),
            universe="BTC perpetuals",
            timeframe="1m",
        ),
    }
    values.update(changes)
    return HypothesisSubmission(**values)


def compile(value: HypothesisSubmission | None = None, *, max_variants: int = 64):
    return compile_submission(
        value or submission(),
        repository_root=ROOT,
        repository_commit="1" * 40,
        max_variants=max_variants,
    )


def test_csi_card_reuses_registered_strategy_and_preserves_sixteen_variants() -> None:
    proposal = compile()
    assert proposal["resolution"]["disposition"] == "reuse_registered_strategy"
    assert proposal["resolution"]["strategy_identity"] == "l7_h1_csi_gated_displacement_trend"
    assert proposal["resolution"]["tier"] == "Tier2B"
    assert proposal["search"]["variant_count"] == 16
    assert proposal["search"]["parameter_grid"] == {
        "d0": [1.8, 2.2],
        "k_stop": [3, 4],
        "k_trail": [3, 5],
        "theta": [0.7, 0.8],
    }
    assert set(proposal["authority"].values()) == {"prohibited"}


def test_legacy_tier_requires_visible_resolution() -> None:
    with pytest.raises(BridgeError, match="ambiguous"):
        compile(submission(tier="Tier2"))
    proposal = compile(submission(tier="Tier2", legacy_tier_resolution="Tier2A"))
    assert proposal["resolution"]["tier"] == "Tier2A"
    assert "resolved explicitly" in proposal["resolution"]["legacy_tier_resolution"]


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (submission(grid={"made_up": (1,)}), "unknown parameters"),
        (submission(grid={"d0": (99,)}), "unregistered values"),
        (submission(grid={"d0": (1.8, 1.8)}), "duplicate values"),
        (submission(original_text="Keep trying until profitable"), "optimization language"),
    ],
)
def test_invalid_and_hidden_search_inputs_fail_closed(value, message) -> None:
    with pytest.raises(BridgeError, match=message):
        compile(value)


def test_cartesian_budget_and_auxiliary_availability_fail_closed() -> None:
    with pytest.raises(BridgeError, match="budget permits"):
        compile(max_variants=15)
    with pytest.raises(BridgeError, match="auxiliary fields"):
        compile(submission(dataset=DatasetBinding(
            snapshot_id="snapshot",
            digest="2" * 64,
            available_fields=("ohlcv", "volume"),
            universe="BTC perpetuals",
            timeframe="1m",
        )))


def test_approval_digest_is_immutable_and_authority_cannot_expand() -> None:
    proposal = compile()
    approved = proposal | {"state": "approved"}
    validate_approved_proposal(approved, proposal["proposal_digest"])
    changed = approved | {"search": approved["search"] | {"variant_count": 17}}
    with pytest.raises(BridgeError, match="changed after approval"):
        validate_approved_proposal(changed, proposal["proposal_digest"])
    expanded = approved | {"authority": approved["authority"] | {"capital": "allowed"}}
    with pytest.raises(BridgeError):
        validate_approved_proposal(expanded, proposal["proposal_digest"])


def test_lifecycle_is_durable_monotone_and_idempotent(tmp_path: Path) -> None:
    proposal = compile()
    ledger = BridgeLedger(tmp_path / "bridge.sqlite")
    try:
        assert ledger.register(proposal)["state"] == "awaiting_approval"
        receipt = {"approved_by": "founder", "approval_digest": proposal["proposal_digest"]}
        assert ledger.advance(
            proposal["proposal_digest"],
            expected_state="awaiting_approval",
            next_state="approved",
            receipt=receipt,
        )["state"] == "approved"
        assert ledger.advance(
            proposal["proposal_digest"],
            expected_state="awaiting_approval",
            next_state="approved",
            receipt=receipt,
        )["state"] == "approved"
        with pytest.raises(BridgeError, match="skipped"):
            ledger.advance(
                proposal["proposal_digest"],
                expected_state="approved",
                next_state="executed",
                receipt={"run": "a" * 64},
            )
        with pytest.raises(BridgeError, match="immutable"):
            ledger.advance(
                proposal["proposal_digest"],
                expected_state="awaiting_approval",
                next_state="approved",
                receipt={"approved_by": "other"},
            )
    finally:
        ledger.close()


def test_hermes_registration_sends_only_typed_proposal() -> None:
    value = compile()
    response = type("Response", (), {
        "__enter__": lambda self: self,
        "__exit__": lambda self, *args: None,
        "read": lambda self: b'{"state":"awaiting_approval"}',
    })()
    with patch("urllib.request.urlopen", return_value=response) as opened:
        result = register_with_hermes(api_url="http://hermes", token="secret", proposal=value)
    assert result["state"] == "awaiting_approval"
    request = opened.call_args.args[0]
    assert request.full_url.endswith("/v1/research/governed-bridges")
    assert b'"command"' not in request.data


def test_approved_contract_materializes_only_sixteen_requested_variants(tmp_path: Path) -> None:
    value = compile() | {"state": "approved"}
    receipt = materialize_approved_contract(
        value, repository_root=ROOT, output=tmp_path / "approved.yaml"
    )
    assert receipt["variant_count"] == 16
    derived = __import__("yaml").safe_load((tmp_path / "approved.yaml").read_text())
    assert derived["parameter_grid"]["d0"] == [1.8, 2.2]
    assert derived["parameter_grid"]["signal_timeframe"] == ["15m"]
    assert derived["governance"]["proposal_digest"] == value["proposal_digest"]
