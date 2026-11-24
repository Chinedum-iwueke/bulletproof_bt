"""Tests for the stable and lightweight ``bt`` public surface."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys


HEAVY_MODULES = (
    "pandas",
    "pyarrow",
    "bt.core.engine",
    "bt.execution.execution_model",
)


def test_bt_public_api_exports():
    import bt

    assert hasattr(bt, "run_backtest")
    assert hasattr(bt, "run_grid")
    assert hasattr(bt, "__version__")


def test_import_bt_is_lightweight():
    # A cold import needs a new interpreter, not eviction of collected tests'
    # module objects and registries from the shared pytest interpreter.
    source = Path(__file__).resolve().parents[1] / "src"
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(source)!r})\n"
        f"heavy_modules = {HEAVY_MODULES!r}\n"
        "assert all(name not in sys.modules for name in heavy_modules)\n"
        "import bt\n"
        "assert all(name not in sys.modules for name in heavy_modules)\n"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_lightweight_import_preserves_parent_module_identity():
    from bt.contracts import research_specs_v2

    registry = research_specs_v2.STRATEGY_REGISTRY
    test_import_bt_is_lightweight()
    from bt.contracts import research_specs_v2 as after

    assert after is research_specs_v2
    assert after.STRATEGY_REGISTRY is registry
    assert sys.modules[research_specs_v2.__name__] is research_specs_v2


def test_run_backtest_still_works_via_bt_namespace(tmp_path):
    import bt

    run_dir = bt.run_backtest(
        config_path="configs/engine.yaml",
        data_path="data/curated/sample.csv",
        out_dir=str(tmp_path),
        override_paths=[],
        run_name="public_surface_smoke",
    )

    assert run_dir == str(tmp_path / "public_surface_smoke")
    assert (tmp_path / "public_surface_smoke").exists()
