# Recovery and resume

1. Ensure state DB and checkpoint files are present for the previous run.
2. Start with `exec.restart_policy=resume`.
3. Confirm lineage:
   - `run_manifest.json` should include `resumed_from_run_id`.
4. Confirm recovery incident exists:
   - `incident_type=recovery_resume_started` in `incidents.jsonl`.
5. If resume fails, switch to read-only diagnostics and inspect `run_status.json`/`incidents.jsonl`.
