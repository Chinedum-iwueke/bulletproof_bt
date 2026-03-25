# Starting demo execution

1. Validate config and credentials first:
   - `python -m bt.exec.cli.doctor --config configs/exec/bybit_demo_execution.yaml --check-ws`
2. Start demo execution:
   - `python scripts/run_exec_bybit_demo.py --data <bars.csv>`
3. Confirm startup in `run_status.json`:
   - `startup_gate_result`, `read_only`, `trading_enabled`, `frozen`.
4. Watch artifacts while running:
   - `heartbeat.jsonl`, `reconciliation.jsonl`, `incidents.jsonl`, `alerts.jsonl`.
5. If frozen, stop and follow `responding_to_frozen_runtime.md`.
