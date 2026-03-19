from bt.hypotheses.contract import HypothesisContract


def test_l1_h2b_contract_filters_invalid_z_pairs() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h2b_compression_confirmed_fade.yaml")
    assert contract.schema.metadata.hypothesis_id == "L1-H2B"
    rows = contract.materialize_grid()
    assert rows
    for row in rows:
        params = row["params"]
        assert float(params["z_reentry"]) < float(params["z_ext"])
