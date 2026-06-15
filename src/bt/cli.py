from __future__ import annotations

import sys

from bt.contracts.research_specs import main as research_specs_main


def main() -> int:
    return research_specs_main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
