# Responding to frozen runtime

1. Confirm freeze reason:
   - read `run_status.json` (`frozen`, `freeze_reason`, `startup_gate_reason`).
2. Inspect incident timeline:
   - `python -m bt.exec.cli.incidents --run-dir <run_dir> --limit 50`.
3. Check latest reconciliation and heartbeat:
   - tail `reconciliation.jsonl` and `heartbeat.jsonl`.
4. Keep runtime read-only until root cause is explained.
5. Only resume when transport/stream/auth issues are cleared and startup gate can pass.
