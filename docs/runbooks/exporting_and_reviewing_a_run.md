# Exporting and reviewing a run

1. Build summaries + export bundle:
   - `python -m bt.exec.cli.export_run --run-dir <run_dir> --export-root outputs/exec_exports`
2. Review bundle index:
   - `export_manifest.json` (copied vs skipped files).
3. Review operator summaries:
   - `session_summary.json`
   - `incident_summary.json`
4. Use CLI helpers for quick review:
   - `python -m bt.exec.cli.status --run-dir <run_dir>`
   - `python -m bt.exec.cli.incidents --run-dir <run_dir>`
