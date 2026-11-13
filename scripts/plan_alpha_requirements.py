"""Read retained admission evidence only; never scan the lake or invoke a model."""
import argparse
import json
from pathlib import Path

from bt.governance.requirements_feasibility import plan_requirements


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--assignment", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assignment = json.loads(args.assignment.read_text())
    result = plan_requirements(assignment["feasibility_evidence"], source_commit=assignment["source_commit"])
    args.output.write_text(json.dumps(result, sort_keys=True, allow_nan=False) + "\n")
    args.output.chmod(0o600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
