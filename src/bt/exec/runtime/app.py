from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from bt.config import deep_merge, load_yaml, resolve_paths_relative_to
from bt.core.config_resolver import resolve_config
from bt.data.load_feed import load_feed
from bt.execution.commission import CommissionSpec
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.intrabar import parse_intrabar_spec
from bt.execution.profile import resolve_execution_profile
from bt.execution.slippage import SlippageModel
from bt.instruments.registry import resolve_instrument_spec
from bt.logging.trades import make_run_id, prepare_run_dir, write_config_used
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.risk.spec import parse_risk_spec
from bt.strategy import make_strategy
from bt.strategy.htf_context import ReadOnlyContextStrategyAdapter

from bt.exec.adapters.base import BrokerAdapter
from bt.exec.adapters.bybit import BybitBrokerAdapter, BybitRESTClient, resolve_bybit_config
from bt.exec.adapters.bybit.client_ws_private import BybitPrivateWSClient
from bt.exec.adapters.bybit.client_ws_public import BybitPublicWSClient
from bt.exec.adapters.simulated import SimulatedBrokerAdapter
from bt.exec.logging.exec_artifacts import ExecArtifactWriters
from bt.exec.reconcile import ReconciliationEngine, ReconciliationInputs, ReconciliationPolicy, ReconciliationScope, reconciliation_record
from bt.exec.runtime.bar_gate import ClosedBarGate
from bt.exec.runtime.health import RuntimeHealthMonitor
from bt.exec.runtime.loop import ReconciliationConfig, RuntimeLoop, RuntimeLoopState
from bt.exec.runtime.scheduler import HeartbeatScheduler
from bt.exec.services.execution_router import ExecutionRouter
from bt.exec.services.kill_switch import KillSwitch
from bt.exec.services.live_controls import CanaryGuard, load_canary_policy
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.exec.services.risk_runner import RiskRunner
from bt.exec.services.strategy_runner import StrategyRunner
from bt.exec.state import RuntimeSessionState, SQLiteExecutionStateStore
from bt.exec.state.checkpoints import CheckpointCadence, build_runtime_checkpoint, save_checkpoint
from bt.exec.state.recovery import build_recovery_plan


def _load_exec_config(config_path: str, override_paths: list[str] | None) -> dict[str, Any]:
    cfg = load_yaml(config_path)
    for path in resolve_paths_relative_to(Path(config_path).parent, override_paths):
        cfg = deep_merge(cfg, load_yaml(path))
    return resolve_config(cfg)


def _coerce_spread_mode(value: object) -> Literal["none", "fixed_bps", "bar_range_proxy", "fixed_pips"]:
    normalized = str(value)
    if normalized in {"none", "fixed_bps", "bar_range_proxy", "fixed_pips"}:
        return cast(Literal["none", "fixed_bps", "bar_range_proxy", "fixed_pips"], normalized)
    return "none"


def _build_components(config: dict[str, Any]) -> tuple[StrategyRunner, RiskRunner, PortfolioRunner, BrokerAdapter]:
    strategy_cfg = config.get("strategy", {}) if isinstance(config.get("strategy"), dict) else {}
    strategy = ReadOnlyContextStrategyAdapter(inner=make_strategy(strategy_cfg.get("name", "coinflip"), **{k: v for k, v in strategy_cfg.items() if k != "name"}))
    risk_cfg = dict(config.get("risk", {}))
    risk_spec = parse_risk_spec({"risk": risk_cfg})
    profile = resolve_execution_profile(config)
    eff_slip = profile.slippage_bps + profile.spread_bps
    risk = RiskEngine(
        max_positions=int(risk_cfg.get("max_positions", 5)),
        max_notional_per_symbol=config.get("max_notional_per_symbol"),
        margin_buffer_tier=int(risk_cfg.get("margin_buffer_tier", 1)),
        maker_fee_bps=profile.maker_fee * 1e4,
        taker_fee_bps=profile.taker_fee * 1e4,
        slippage_k_proxy=float(risk_cfg.get("slippage_k_proxy", 0.0)),
        config={"risk": risk_cfg, "model": "fixed_bps", "fixed_bps": eff_slip, "slippage": config.get("slippage")},
    )
    portfolio = Portfolio(initial_cash=float(config.get("initial_cash", 100000.0)), max_leverage=risk_spec.max_leverage or float(config.get("max_leverage", 2.0)))

    ex_cfg = config.get("execution", {}) if isinstance(config.get("execution"), dict) else {}
    ex_model = ExecutionModel(
        fee_model=FeeModel(maker_fee_bps=profile.maker_fee * 1e4, taker_fee_bps=profile.taker_fee * 1e4),
        slippage_model=SlippageModel(k=float(config.get("slippage_k", 1.0)), atr_pct_cap=float(config.get("atr_pct_cap", 0.2)), impact_cap=float(config.get("impact_cap", 0.05)), fixed_bps=eff_slip),
        spread_mode=_coerce_spread_mode(ex_cfg.get("spread_mode", "none")),
        spread_bps=float(ex_cfg.get("spread_bps", 0.0) or 0.0),
        spread_pips=(None if ex_cfg.get("spread_pips") is None else float(cast(object, ex_cfg.get("spread_pips")))),
        intrabar_mode=parse_intrabar_spec(config).mode,
        delay_bars=int(profile.delay_bars),
        instrument=resolve_instrument_spec(config, symbol=None),
        commission=CommissionSpec(mode=str((ex_cfg.get("commission") or {}).get("mode", "none"))),
    )
    broker_cfg = config.get("broker") if isinstance(config.get("broker"), dict) else {}
    venue = str(broker_cfg.get("venue", "simulated")).lower()
    if venue == "bybit":
        bybit_cfg = resolve_bybit_config(config)
        api_key, api_secret = bybit_cfg.auth.resolve()
        rest = BybitRESTClient(
            base_url=bybit_cfg.rest_base_url,
            api_key=api_key,
            api_secret=api_secret,
            recv_window_ms=bybit_cfg.recv_window_ms,
            timeout_ms=bybit_cfg.request_timeout_ms,
            max_retries=bybit_cfg.max_retries,
            retry_backoff_ms=bybit_cfg.retry_backoff_ms,
            environment=bybit_cfg.environment,
        )
        adapter: BrokerAdapter = BybitBrokerAdapter(
            config=bybit_cfg,
            rest_client=rest,
            ws_public=BybitPublicWSClient(url=bybit_cfg.public_ws_url, topics=bybit_cfg.ws.public_topics, symbols=bybit_cfg.symbols, enabled=bybit_cfg.ws.enabled),
            ws_private=BybitPrivateWSClient(url=bybit_cfg.private_ws_url, topics=bybit_cfg.ws.private_topics, api_key=api_key, api_secret=api_secret, enabled=bybit_cfg.ws.enabled),
        )
        return StrategyRunner(strategy=strategy), RiskRunner(risk_engine=risk), PortfolioRunner(portfolio=portfolio), adapter
    simulated: BrokerAdapter = cast(BrokerAdapter, SimulatedBrokerAdapter(execution_model=ex_model))
    return StrategyRunner(strategy=strategy), RiskRunner(risk_engine=risk), PortfolioRunner(portfolio=portfolio), simulated


def _validate_state_config(*, config: dict[str, Any]) -> tuple[bool, str, str, int, bool, bool]:
    exec_cfg = config.setdefault("exec", {})
    state_cfg = config.setdefault("state", {})
    persist_state = bool(exec_cfg.get("persist_state", True))
    restart_policy = str(exec_cfg.get("restart_policy", "resume"))
    if restart_policy not in {"resume", "reconcile_only", "fresh"}:
        raise ValueError(f"Unsupported exec.restart_policy: {restart_policy}")
    checkpoint_interval_seconds = int(exec_cfg.get("checkpoint_interval_seconds", 60))
    save_processed_event_ids = bool(state_cfg.get("save_processed_event_ids", True))
    save_checkpoints = bool(state_cfg.get("save_checkpoints", True))
    state_path = str(state_cfg.get("path", "outputs/exec_state/runtime.sqlite"))
    if persist_state and str(state_cfg.get("backend", "sqlite")) != "sqlite":
        raise ValueError("Phase 2 supports only state.backend=sqlite")
    return persist_state, restart_policy, state_path, checkpoint_interval_seconds, save_processed_event_ids, save_checkpoints


def _resolve_reconcile_config(config: dict[str, Any]) -> tuple[ReconciliationConfig, ReconciliationPolicy, ReconciliationScope, float, float, float]:
    rcfg = config.setdefault("reconcile", {})
    interval = int(rcfg.get("interval_seconds", 30))
    if interval <= 0:
        raise ValueError("reconcile.interval_seconds must be > 0")
    policy = ReconciliationPolicy(str(rcfg.get("policy", "warn")))
    return (
        ReconciliationConfig(enabled=bool(rcfg.get("enabled", True)), interval_seconds=interval),
        policy,
        ReconciliationScope(
            compare_orders=bool(rcfg.get("compare_orders", True)),
            compare_fills=bool(rcfg.get("compare_fills", True)),
            compare_positions=bool(rcfg.get("compare_positions", True)),
            compare_balances=bool(rcfg.get("compare_balances", True)),
        ),
        float(rcfg.get("material_fill_qty_tolerance", 0.0)),
        float(rcfg.get("material_position_qty_tolerance", 0.0)),
        float(rcfg.get("material_balance_tolerance", 0.0)),
    )


def _run_live_startup_gate(*, adapter: BrokerAdapter, execution_router: ExecutionRouter, portfolio_runner: PortfolioRunner, rid: str, reconcile_scope: ReconciliationScope, fill_tol: float, pos_tol: float, bal_tol: float, policy: ReconciliationPolicy, require_private_stream_ready: bool) -> tuple[bool, str | None]:
    if require_private_stream_ready and hasattr(adapter, "private_stream_ready") and not bool(getattr(adapter, "private_stream_ready")()):
        return False, "private_stream_not_ready"
    local_positions = list(portfolio_runner.portfolio.position_book.all_positions().values())
    local_balances = adapter.fetch_balances()
    inputs = ReconciliationInputs(
        run_id=rid,
        ts=pd.Timestamp.now(tz="UTC"),
        local_open_orders=execution_router.current_open_orders(),
        adapter_open_orders=adapter.fetch_open_orders(),
        adapter_completed_orders=adapter.fetch_completed_orders(),
        local_fills=execution_router.local_fills(),
        adapter_fills=adapter.fetch_recent_fills_or_executions(),
        local_positions=local_positions,
        adapter_positions=adapter.fetch_positions(),
        local_balances=local_balances,
        adapter_balances=adapter.fetch_balances(),
        scope=reconcile_scope,
        material_fill_qty_tolerance=fill_tol,
        material_position_qty_tolerance=pos_tol,
        material_balance_tolerance=bal_tol,
    )
    result = ReconciliationEngine().reconcile(inputs=inputs, policy=policy)
    if result.decision.action.value == "freeze":
        return False, "startup_reconciliation_freeze"
    return True, None


def run_exec_session(*, config_path: str, data_path: str, mode: str, out_dir: str | None = None, override_paths: list[str] | None = None, run_id: str | None = None) -> str:
    config = _load_exec_config(config_path, override_paths)
    exec_cfg = config.setdefault("exec", {})
    exec_cfg["mode"] = mode

    reconcile_cfg, reconcile_policy, reconcile_scope, fill_tol, pos_tol, bal_tol = _resolve_reconcile_config(config)
    persist_state, restart_policy, state_path, checkpoint_interval_seconds, save_processed_event_ids, save_checkpoints = _validate_state_config(config=config)
    run_root = Path(out_dir or exec_cfg.get("run_root", "outputs/exec_runs"))
    if state_path == "outputs/exec_state/runtime.sqlite" and out_dir is not None:
        state_path = str((run_root / "exec_state" / "runtime.sqlite"))
    rid = run_id or make_run_id(prefix="exec")
    run_dir = prepare_run_dir(run_root, rid)
    write_config_used(run_dir, config)

    strategy_runner, risk_runner, portfolio_runner, adapter = _build_components(config)
    broker_cfg = config.get("broker") if isinstance(config.get("broker"), dict) else {}
    environment = str(broker_cfg.get("environment", "demo")).lower()
    is_live_mode = mode == "live_broker"
    if mode == "paper_broker":
        if str(broker_cfg.get("venue", "")).lower() != "bybit":
            raise ValueError("exec.mode=paper_broker currently requires broker.venue=bybit")
        if environment != "demo":
            raise ValueError("exec.mode=paper_broker is demo-only in Phase 5")
    if is_live_mode and environment != "live":
        raise ValueError("exec.mode=live_broker requires broker.environment=live")

    checkpoint_ts: pd.Timestamp | None = None
    order_seq = 0
    checkpoint_seq = 0
    resumed_from_run_id: str | None = None
    state_store: SQLiteExecutionStateStore | None = None
    if persist_state:
        state_store = SQLiteExecutionStateStore(path=state_path)
        plan = build_recovery_plan(store=state_store, mode=mode, restart_policy=restart_policy)
        if plan.checkpoint is not None and plan.disposition.value == "resume":
            checkpoint_ts = plan.checkpoint.last_bar_ts
            order_seq = plan.checkpoint.next_client_order_seq
            checkpoint_seq = plan.checkpoint.sequence
            resumed_from_run_id = plan.checkpoint.run_id
        session = RuntimeSessionState(
            run_id=rid,
            mode=mode,
            restart_policy=restart_policy,
            status="running",
            started_at=pd.Timestamp.now(tz="UTC"),
            updated_at=pd.Timestamp.now(tz="UTC"),
            metadata={"recovery_disposition": plan.disposition.value, "recovery_message": plan.message, "resumed_from_run_id": resumed_from_run_id},
        )
        state_store.record_session_liveness(session)

    artifacts = ExecArtifactWriters(run_dir=run_dir, run_id=rid, mode=mode, config=config, data_path=data_path, resumed_from_run_id=resumed_from_run_id)
    artifacts.write_status(state="running", extra={"environment": environment})

    checkpoint_cadence = CheckpointCadence(interval_seconds=checkpoint_interval_seconds)

    def _checkpoint_callback(ts: pd.Timestamp, state: RuntimeLoopState) -> None:
        if state_store is None or not save_checkpoints:
            return
        if not checkpoint_cadence.should_checkpoint(ts):
            return
        checkpoint = build_runtime_checkpoint(
            run_id=rid,
            sequence=state.checkpoint_sequence + 1,
            last_bar_ts=state.last_processed_bar_ts,
            next_client_order_seq=state.client_order_seq,
            open_orders=adapter.fetch_open_orders(),
            positions=list(portfolio_runner.portfolio.position_book.all_positions().values()),
            balances=adapter.fetch_balances(),
            mode=mode,
        )
        save_checkpoint(store=state_store, checkpoint=checkpoint)
        state.checkpoint_sequence = checkpoint.sequence

    execution_router = ExecutionRouter(
        run_id=rid,
        mode=mode,
        adapter=adapter,
        portfolio_runner=portfolio_runner,
        store=state_store,
        save_processed_event_ids=save_processed_event_ids,
    )

    reconciliation_engine = ReconciliationEngine()

    def _run_reconcile(ts: pd.Timestamp):
        local_positions = list(portfolio_runner.portfolio.position_book.all_positions().values())
        local_balances = adapter.fetch_balances()
        inputs = ReconciliationInputs(
            run_id=rid,
            ts=ts,
            local_open_orders=execution_router.current_open_orders(),
            adapter_open_orders=adapter.fetch_open_orders(),
            adapter_completed_orders=adapter.fetch_completed_orders(),
            local_fills=execution_router.local_fills(),
            adapter_fills=adapter.fetch_recent_fills_or_executions(),
            local_positions=local_positions,
            adapter_positions=adapter.fetch_positions(),
            local_balances=local_balances,
            adapter_balances=adapter.fetch_balances(),
            scope=reconcile_scope,
            material_fill_qty_tolerance=fill_tol,
            material_position_qty_tolerance=pos_tol,
            material_balance_tolerance=bal_tol,
        )
        result = reconciliation_engine.reconcile(inputs=inputs, policy=reconcile_policy)
        return reconciliation_record(result)

    kill_switch = KillSwitch(
        allow_reduce_only_exits=bool((config.get("live_controls") or {}).get("allow_reduce_only_when_frozen", True))
    ) if is_live_mode else None
    canary_guard = CanaryGuard(load_canary_policy(config)) if is_live_mode else None

    loop = RuntimeLoop(
        feed=load_feed(data_path, config),
        strategy_runner=strategy_runner,
        risk_runner=risk_runner,
        portfolio_runner=portfolio_runner,
        execution_router=execution_router,
        artifacts=artifacts,
        scheduler=HeartbeatScheduler(heartbeat_seconds=int(exec_cfg.get("heartbeat_seconds", 30))),
        bar_gate=ClosedBarGate(close_bar_only=bool(exec_cfg.get("close_bar_only", True)), warmup_bars=int((config.get("market_data", {}) or {}).get("warmup_bars", 0))),
        health=RuntimeHealthMonitor(stale_after_seconds=int(exec_cfg.get("stale_after_seconds", 120))),
        mode=mode,
        state=RuntimeLoopState(client_order_seq=order_seq, checkpoint_sequence=checkpoint_seq, last_processed_bar_ts=checkpoint_ts),
        reconciliation=reconcile_cfg,
        reconcile_fn=_run_reconcile,
        on_bar_complete=_checkpoint_callback,
        kill_switch=kill_switch,
        canary_guard=canary_guard,
    )

    final_state = loop.state
    try:
        adapter.start()
        if is_live_mode:
            startup_ok, startup_reason = _run_live_startup_gate(
                adapter=adapter,
                execution_router=execution_router,
                portfolio_runner=portfolio_runner,
                rid=rid,
                reconcile_scope=reconcile_scope,
                fill_tol=fill_tol,
                pos_tol=pos_tol,
                bal_tol=bal_tol,
                policy=reconcile_policy,
                require_private_stream_ready=bool(exec_cfg.get("require_private_stream_ready", True)),
            )
            read_only_startup = bool((config.get("live_controls") or {}).get("read_only_startup", False))
            if not startup_ok:
                if kill_switch is not None:
                    kill_switch.freeze(reason=startup_reason or "startup_blocked", ts=pd.Timestamp.now(tz="UTC"))
                artifacts.write_status(
                    state="running",
                    extra={
                        "environment": environment,
                        "startup_gate_result": "blocked",
                        "startup_blocked_reason": startup_reason,
                        "frozen": True,
                        "read_only": True,
                    },
                )
                if not read_only_startup:
                    raise RuntimeError(f"live startup blocked: {startup_reason}")
            if hasattr(adapter, "set_live_mutations_enabled") and startup_ok and not read_only_startup:
                getattr(adapter, "set_live_mutations_enabled")(True)
            artifacts.write_status(
                state="running",
                extra={
                    "environment": environment,
                    "startup_gate_result": "passed" if startup_ok else "blocked",
                    "frozen": False if startup_ok else True,
                    "read_only": read_only_startup or (not startup_ok),
                    "canary_enabled": canary_guard.policy.enabled if canary_guard is not None else False,
                },
            )

        final_state = loop.run()
        artifacts.write_status(
            state="stopped",
            extra={
                "environment": environment,
                "frozen": final_state.frozen,
                "freeze_reason": None if kill_switch is None else kill_switch.state().reason,
            },
        )
        if state_store is not None:
            if save_checkpoints:
                checkpoint = build_runtime_checkpoint(
                    run_id=rid,
                    sequence=final_state.checkpoint_sequence + 1,
                    last_bar_ts=final_state.last_processed_bar_ts,
                    next_client_order_seq=final_state.client_order_seq,
                    open_orders=adapter.fetch_open_orders(),
                    positions=list(portfolio_runner.portfolio.position_book.all_positions().values()),
                    balances=adapter.fetch_balances(),
                    mode=mode,
                )
                save_checkpoint(store=state_store, checkpoint=checkpoint)
            state_store.mark_session_final_status(run_id=rid, status="stopped", ts=pd.Timestamp.now(tz="UTC"))
    except Exception as exc:
        artifacts.write_status(state="failed", error=str(exc), extra={"environment": environment})
        if state_store is not None:
            state_store.mark_session_final_status(run_id=rid, status="failed", ts=pd.Timestamp.now(tz="UTC"), error=str(exc))
        raise
    finally:
        adapter.stop()
        artifacts.close()
        if state_store is not None:
            state_store.close()
    return str(run_dir)
