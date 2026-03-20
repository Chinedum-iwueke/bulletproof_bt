from __future__ import annotations

import argparse

from bt.exec.runtime import run_exec_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Run exec shadow mode.")
    parser.add_argument("--config", default="configs/exec/shadow_simulated.yaml")
    parser.add_argument("--data", required=True)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--override", action="append", default=[])
    args = parser.parse_args()
    run_exec_session(config_path=args.config, data_path=args.data, mode="shadow", out_dir=args.out_dir, override_paths=args.override or None, run_id=args.run_id)


if __name__ == "__main__":
    main()
