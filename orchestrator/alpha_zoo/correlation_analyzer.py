from __future__ import annotations


def analyze_redundancy(candidates: list[dict]) -> tuple[list[dict], list[dict]]:
    redundancies = []
    for i, a in enumerate(candidates):
        for b in candidates[i+1:]:
            score = 0.0
            if a["identity"].get("hypothesis_family") == b["identity"].get("hypothesis_family"):
                score += 0.4
            if a["state_profile"].get("setup_class") == b["state_profile"].get("setup_class"):
                score += 0.3
            if a["identity"].get("dataset_type") == b["identity"].get("dataset_type"):
                score += 0.2
            if a["identity"].get("config_hash") == b["identity"].get("config_hash"):
                score += 0.2
            if score >= 0.8:
                weaker, stronger = (a,b) if a["zoo_metadata"].get("promotion_score",0) < b["zoo_metadata"].get("promotion_score",0) else (b,a)
                weaker.setdefault("zoo_metadata",{})["candidate_status"] = "REDUNDANT"
                weaker["zoo_metadata"]["redundancy_group"] = f"{stronger['identity'].get('candidate_id')}"
                weaker["zoo_metadata"]["recommended_action"] = "REDUNDANT"
                redundancies.append({"candidate_id": weaker["identity"].get("candidate_id"), "similar_to": stronger["identity"].get("candidate_id"), "similarity_score": score, "redundancy_reason": "family/setup/config overlap"})
    return candidates, redundancies
