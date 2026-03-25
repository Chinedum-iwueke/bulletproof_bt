# Starting live canary

1. Dry-check live readiness (no orders placed):
   - `python -m bt.exec.cli.doctor --config configs/exec/bybit_live_canary.yaml --check-ws --live-readiness`
2. Ensure doctor `readiness` is `healthy_live_readiness`.
3. Start live canary:
   - `python scripts/run_exec_bybit_live.py --data <bars.csv>`
4. Verify status keys:
   - `startup_gate_result=passed`, `canary_enabled=true`, `mutation_enabled=true`, `frozen=false`.
5. If `startup_gate_result=blocked`, remain read-only and investigate before restart.
