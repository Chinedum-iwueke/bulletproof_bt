"""Structural EV-by-bucket diagnostics."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from bt.analysis.structural_buckets import CSI_SPEC, DISP_SPEC, LIQ_SPEC, VOL_SPEC, assign_bucket


def _metrics(df: pd.DataFrame, bucket_name: str, key: str, min_trades: int) -> pd.DataFrame:
    def _series(part: pd.DataFrame, candidates: list[str]) -> pd.Series:
        for col in candidates:
            if col in part.columns:
                return pd.to_numeric(part[col], errors="coerce")
        return pd.Series([float("nan")] * len(part), index=part.index, dtype="float64")

    rows = []
    grouped = df.groupby(key, dropna=True)
    for bucket, part in grouped:
        if len(part) < min_trades:
            continue
        r = _series(part, ["r_net", "realized_r_net", "r_multiple_net"])
        rg = _series(part, ["r_gross", "realized_r_gross", "r_multiple_gross"])
        mfe = _series(part, ["path_mfe_r", "mfe_r"])
        mae = _series(part, ["path_mae_r", "mae_r"])
        rows.append(
            {
                "bucket": bucket_name,
                "bucket_key": bucket,
                "n_trades": int(len(part)),
                "ev_r_net": float(r.mean()),
                "ev_r_gross": float(rg.mean()) if rg.notna().any() else None,
                "median_r_net": float(r.median()),
                "win_rate": float((r > 0).mean()),
                "avg_r_win": float(r[r > 0].mean()) if (r > 0).any() else None,
                "avg_r_loss": float(r[r < 0].mean()) if (r < 0).any() else None,
                "payoff_ratio": float(abs(r[r > 0].mean() / r[r < 0].mean())) if (r > 0).any() and (r < 0).any() else None,
                "p25_r": float(r.quantile(0.25)),
                "p75_r": float(r.quantile(0.75)),
                "p95_r": float(r.quantile(0.95)),
                "p99_r": float(r.quantile(0.99)),
                "max_r": float(r.max()),
                "min_r": float(r.min()),
                "tail_2r_count": int((r >= 2).sum()),
                "tail_3r_count": int((r >= 3).sum()),
                "tail_5r_count": int((r >= 5).sum()),
                "tail_10r_count": int((r >= 10).sum()),
                "avg_mfe_r": float(mfe.mean()) if mfe.notna().any() else None,
                "avg_mae_r": float(mae.mean()) if mae.notna().any() else None,
                "median_mfe_r": float(mfe.median()) if mfe.notna().any() else None,
                "median_mae_r": float(mae.median()) if mae.notna().any() else None,
                "avg_exit_efficiency": float(pd.to_numeric(part.get("counterfactual_exit_efficiency_realized_over_mfe"), errors="coerce").mean()),
                "avg_cost_drag_r": float(pd.to_numeric(part.get("cost_drag_r"), errors="coerce").mean()),
                "avg_fee_drag_r": float(pd.to_numeric(part.get("fee_drag_r"), errors="coerce").mean()),
                "avg_slippage_drag_r": float(pd.to_numeric(part.get("slippage_drag_r"), errors="coerce").mean()),
                "avg_spread_drag_r": float(pd.to_numeric(part.get("spread_drag_r"), errors="coerce").mean()),
            }
        )
    baseline = pd.to_numeric(df.get("r_net", df.get("realized_r_net", df.get("r_multiple_net"))), errors="coerce")
    rows.append({"bucket": bucket_name, "bucket_key": "overall_all_trades", "n_trades": int(len(df)), "ev_r_net": float(baseline.mean())})
    return pd.DataFrame(rows)


def run_structural_bucket_analysis(trades_df: pd.DataFrame, output_dir: Path, min_trades: int = 10) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = trades_df.copy()
    missing: dict[str, list[str]] = {}
    outputs: dict[str, str] = {}

    csi_col = "entry_state_csi_pctile" if "entry_state_csi_pctile" in df.columns else ("entry_state_csi_raw" if "entry_state_csi_raw" in df.columns else None)
    if csi_col:
        df["_csi_bucket"] = assign_bucket(df[csi_col], CSI_SPEC)
        out = _metrics(df.dropna(subset=["_csi_bucket"]), "csi", "_csi_bucket", min_trades)
        p = output_dir / "ev_by_bucket_csi.csv"; out.to_csv(p, index=False); outputs["ev_by_bucket_csi"] = str(p)
    else:
        missing["csi"] = ["entry_state_csi_pctile", "entry_state_csi_raw"]

    vol_col = "entry_state_vol_pctile" if "entry_state_vol_pctile" in df.columns else ("entry_state_atr_pct_pctile" if "entry_state_atr_pct_pctile" in df.columns else None)
    if vol_col:
        df["_vol_bucket"] = assign_bucket(df[vol_col], VOL_SPEC)
        out = _metrics(df.dropna(subset=["_vol_bucket"]), "vol", "_vol_bucket", min_trades)
        p = output_dir / "ev_by_bucket_vol.csv"; out.to_csv(p, index=False); outputs["ev_by_bucket_vol"] = str(p)
    else:
        missing["vol"] = ["entry_state_vol_pctile", "entry_state_atr_pct_pctile"]

    if "entry_state_spread_proxy_pctile" in df.columns:
        df["_liquidity_bucket"] = assign_bucket(df["entry_state_spread_proxy_pctile"], LIQ_SPEC)
        p = output_dir / "ev_by_bucket_liquidity.csv"; _metrics(df.dropna(subset=["_liquidity_bucket"]), "liquidity", "_liquidity_bucket", min_trades).to_csv(p, index=False); outputs["ev_by_bucket_liquidity"] = str(p)
    else:
        missing["liquidity"] = ["entry_state_spread_proxy_pctile"]

    if "entry_state_tr_over_atr" in df.columns:
        df["_disp_bucket"] = assign_bucket(df["entry_state_tr_over_atr"], DISP_SPEC)
        p = output_dir / "ev_by_bucket_displacement.csv"; _metrics(df.dropna(subset=["_disp_bucket"]), "displacement", "_disp_bucket", min_trades).to_csv(p, index=False); outputs["ev_by_bucket_displacement"] = str(p)
    else:
        missing["displacement"] = ["entry_state_tr_over_atr"]

    setup_col = "entry_decision_setup_class" if "entry_decision_setup_class" in df.columns else ("label_structure_class" if "label_structure_class" in df.columns else None)
    if setup_col:
        p = output_dir / "ev_by_bucket_setup_class.csv"; _metrics(df.dropna(subset=[setup_col]), "setup_class", setup_col, min_trades).to_csv(p, index=False); outputs["ev_by_bucket_setup_class"] = str(p)
    else:
        missing["setup_class"] = ["entry_decision_setup_class", "label_structure_class"]

    if {"_csi_bucket", "_vol_bucket"}.issubset(df.columns):
        df["_joint_csi_vol"] = df["_csi_bucket"].astype(str) + "__" + df["_vol_bucket"].astype(str)
        p = output_dir / "ev_by_bucket_joint_csi_vol.csv"; _metrics(df.dropna(subset=["_joint_csi_vol"]), "joint_csi_vol", "_joint_csi_vol", min_trades).to_csv(p, index=False); outputs["ev_by_bucket_joint_csi_vol"] = str(p)

    if {"_csi_bucket", "_liquidity_bucket"}.issubset(df.columns):
        df["_joint_csi_liq"] = df["_csi_bucket"].astype(str) + "__" + df["_liquidity_bucket"].astype(str)
        p = output_dir / "ev_by_bucket_joint_csi_liquidity.csv"; _metrics(df.dropna(subset=["_joint_csi_liq"]), "joint_csi_liquidity", "_joint_csi_liq", min_trades).to_csv(p, index=False); outputs["ev_by_bucket_joint_csi_liquidity"] = str(p)

    if {"_vol_bucket", "_liquidity_bucket"}.issubset(df.columns):
        df["_joint_vol_liq"] = df["_vol_bucket"].astype(str) + "__" + df["_liquidity_bucket"].astype(str)
        p = output_dir / "ev_by_bucket_joint_vol_liquidity.csv"; _metrics(df.dropna(subset=["_joint_vol_liq"]), "joint_vol_liquidity", "_joint_vol_liq", min_trades).to_csv(p, index=False); outputs["ev_by_bucket_joint_vol_liquidity"] = str(p)

    pd.DataFrame([{"bucket": "all", "bucket_key": "overall_all_trades", "n_trades": len(df), "ev_r_net": pd.to_numeric(df.get("r_net", df.get("realized_r_net", df.get("r_multiple_net"))), errors="coerce").mean()}]).to_csv(output_dir / "ev_by_bucket.csv", index=False)
    outputs["ev_by_bucket"] = str(output_dir / "ev_by_bucket.csv")
    (output_dir / "tail_by_bucket.csv").write_text("bucket,n_trades\n", encoding="utf-8")
    (output_dir / "cost_by_bucket.csv").write_text("bucket,n_trades\n", encoding="utf-8")
    (output_dir / "path_by_bucket.csv").write_text("bucket,n_trades\n", encoding="utf-8")

    if missing:
        payload = {"missing": missing, "message": "Structural bucket analyses skipped due to missing columns."}
        path = output_dir / "ev_by_bucket_missing_fields.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        outputs["ev_by_bucket_missing_fields"] = str(path)
    return outputs
