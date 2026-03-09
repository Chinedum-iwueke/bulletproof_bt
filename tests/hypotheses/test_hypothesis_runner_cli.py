from bt.experiments.hypothesis_runner import build_parser


def test_hypothesis_runner_cli_accepts_production_runtime_args() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--config",
            "configs/engine.yaml",
            "--local-config",
            "configs/local/engine.lab.yaml",
            "--data",
            "/tmp/data_1m",
            "--out",
            "outputs/l1_h1_tier2",
            "--hypothesis",
            "research/hypotheses/l1_h1_vol_floor_trend.yaml",
            "--phase",
            "tier2",
            "--override",
            "configs/extra.yaml",
        ]
    )

    assert args.config == "configs/engine.yaml"
    assert args.local_config == "configs/local/engine.lab.yaml"
    assert args.data == "/tmp/data_1m"
    assert args.out == "outputs/l1_h1_tier2"
    assert args.hypothesis == "research/hypotheses/l1_h1_vol_floor_trend.yaml"
    assert args.phase == "tier2"
    assert args.override == ["configs/extra.yaml"]
