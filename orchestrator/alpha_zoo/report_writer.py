from __future__ import annotations
import csv, json
from pathlib import Path


def write_outputs(output_dir: Path, candidates: list[dict], redundancy: list[dict], top_n: int = 50, dry_run: bool = False) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = [{**c.get("identity",{}), **c.get("performance",{}), **c.get("tail",{}), **c.get("cost",{}), **c.get("state_profile",{}), **c.get("zoo_metadata",{})} for c in candidates]
    paths = {
        "alpha_candidates_json": output_dir / "alpha_candidates.json",
        "alpha_candidates_csv": output_dir / "alpha_candidates.csv",
        "alpha_zoo_report_md": output_dir / "alpha_zoo_report.md",
        "promote_tier3_candidates_csv": output_dir / "promote_tier3_candidates.csv",
        "watchlist_candidates_csv": output_dir / "watchlist_candidates.csv",
        "redundant_candidates_csv": output_dir / "redundant_candidates.csv",
        "refinement_candidates_csv": output_dir / "refinement_candidates.csv",
        "rejected_candidates_csv": output_dir / "rejected_candidates.csv",
        "alpha_zoo_manifest_json": output_dir / "alpha_zoo_manifest.json",
    }
    if dry_run:
        return paths
    paths["alpha_candidates_json"].write_text(json.dumps(records, indent=2), encoding="utf-8")
    if records:
        with paths["alpha_candidates_csv"].open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=sorted(set().union(*[r.keys() for r in records])))
            w.writeheader(); w.writerows(records)
    md = "# Alpha Zoo Report\n\n## Executive Summary\n\n" + f"Candidates: {len(records)}\n\n## Promote to Tier3\n"
    md += "\n".join([f"- {r.get('candidate_id')}: {r.get('hypothesis_name')}" for r in records if r.get("candidate_status")=="PROMOTE_TIER3"]) + "\n"
    for h in ["Watchlist","Refinement Candidates","Redundant Candidates","Rejected Candidates","Candidate State Profiles","Stable vs Volatile Behavior","Tail / Convexity Candidates","Cost-Fragile Candidates","Recommended Human Decisions"]:
        md += f"\n## {h}\n"
    paths["alpha_zoo_report_md"].write_text(md, encoding="utf-8")
    for status,key in [("PROMOTE_TIER3","promote_tier3_candidates_csv"),("WATCHLIST","watchlist_candidates_csv"),("REDUNDANT","redundant_candidates_csv"),("REFINE","refinement_candidates_csv"),("REJECTED","rejected_candidates_csv")]:
        rows=[r for r in records if r.get("candidate_status")==status]
        with paths[key].open("w", newline="", encoding="utf-8") as f:
            if rows:
                w=csv.DictWriter(f, fieldnames=sorted(rows[0].keys())); w.writeheader(); w.writerows(rows)
            else:
                f.write("")
    paths["alpha_zoo_manifest_json"].write_text(json.dumps({"artifacts":{k:str(v) for k,v in paths.items()},"top_n":top_n,"redundancy":redundancy}, indent=2), encoding="utf-8")
    return paths
