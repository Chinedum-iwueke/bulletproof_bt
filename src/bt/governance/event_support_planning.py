"""Outcome-blind capacity checks, not observed event counts or power analysis."""

from decimal import Decimal, ROUND_CEILING

from bt.governance.data_requirements import RequirementsError, instant


def assess_event_support(specification, requirements, scientific_plan):
    designs = specification.get("quantile_support_plan", [])
    if not isinstance(designs, list) or len(designs) > 100:
        raise RequirementsError("quantile support designs must be a bounded list")
    assessments, blockers = [], []
    seen = set()
    duplicate_count = 0
    for design in designs:
        if not isinstance(design, dict) or set(design) != {"gate", "partition", "quantile", "threshold_policy"}:
            raise RequirementsError("quantile support design fields differ")
        gate, partition = design["gate"], design["partition"]
        if (gate not in requirements["minimum_event_support"]
                or partition not in {"training", "validation", "test"}
                or design["threshold_policy"] not in {
                    "fixed_training_linear_strict_upper_tail",
                    "fixed_training_linear_strict_lower_tail",
                }
                or type(design["quantile"]) not in {int, float}
                or not 0 < design["quantile"] < 1):
            raise RequirementsError("invalid quantile support design")
        key = (gate, partition, design["quantile"], design["threshold_policy"])
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        parts = scientific_plan.get("partitions", {})
        if partition not in parts:
            continue
        bounds = parts[partition]
        rows = int((instant(bounds["end"]) - instant(bounds["start"])).total_seconds() / (60 * requirements["research_bar_minutes"]))
        q = Decimal(str(design["quantile"]))
        tail_mass = q if design["threshold_policy"].endswith("lower_tail") else 1 - q
        nominal = Decimal(rows) * tail_mass
        # A strict tail of the linear empirical quantile cannot select more
        # than this many fit-population rows. Ties/exclusions can only reduce it.
        hard_cap = int((Decimal(max(0, rows - 1)) * tail_mass).to_integral_value(rounding=ROUND_CEILING)) if partition == "training" else None
        required = requirements["minimum_event_support"][gate]
        impossible = hard_cap is not None and hard_cap < required
        if impossible:
            blockers.append(f"event_support:{gate}:strict_training_quantile_capacity_below_minimum")
        assessments.append({
            **design, "available_partition_bars": rows, "required_events": required,
            "nominal_events_before_exclusions": float(nominal),
            "minimum_bars_for_nominal_event_count": int((Decimal(required) / tail_mass).to_integral_value(rounding=ROUND_CEILING)),
            "fit_population_upper_bound": hard_cap,
            "nominal_shortfall_requires_review": nominal < required,
            "status": "impossible_under_declared_policy" if impossible else "requires_independent_review_and_observation",
        })
    return {
        "schema_version": "event-support-planning-v1", "assessments": assessments,
        "input_design_count": len(designs), "unique_design_count": len(seen),
        "duplicate_design_count": duplicate_count,
        "blockers": blockers, "observed_event_support": False,
        "statistical_power_established": False, "execution_authority": False,
        "scope": "declared_fixed_training_quantile_only" if designs else "not_declared_no_capacity_claim",
    }
