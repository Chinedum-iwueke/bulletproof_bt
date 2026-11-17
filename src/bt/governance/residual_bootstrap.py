"""Synthetic/offline checks of frozen primary/rival residual contrasts.

No model fitting, time-series construction, scientific approval or execution
authority is provided. Callers must supply aligned, contiguous eligible rows.
"""
import math
import random


def _contrast(totals):
    event_sum, event_count, other_sum, other_count = totals
    if not event_count or not other_count:
        return None
    return event_sum / event_count - other_sum / other_count


def _totals(residuals, events):
    return [math.fsum(x for x, e in zip(residuals, events) if e), sum(events),
            math.fsum(x for x, e in zip(residuals, events) if not e), len(events)-sum(events)]


def paired_bootstrap(primary, rival, primary_events, rival_events, *, block_length,
                     replicates=10_000, seed=42):
    arrays = [list(a) for a in (primary, rival, primary_events, rival_events)]
    primary, rival, primary_events, rival_events = arrays
    n = len(primary)
    if not 2 <= n <= 10_000 or any(len(a) != n for a in arrays):
        raise ValueError("require 2..10000 aligned rows")
    if any(not math.isfinite(x) for x in primary + rival):
        raise ValueError("residuals must be finite")
    if any(type(e) is not bool for e in primary_events + rival_events):
        raise ValueError("event indicators must be boolean")
    if type(block_length) is not int or not 1 <= block_length <= n:
        raise ValueError("invalid block length")
    if type(replicates) is not int or not 99 <= replicates <= 10_000:
        raise ValueError("replicates must be 99..10000")
    if n * block_length > 2_000_000 or replicates * math.ceil(n / block_length) > 2_000_000:
        raise ValueError("bootstrap exceeds offline work budget")
    observed_p = _contrast(_totals(primary, primary_events))
    observed_r = _contrast(_totals(rival, rival_events))
    if observed_p is None or observed_r is None:
        return {"status": "invalid", "reason": "missing_observed_group"}
    observed = (observed_p, observed_p-observed_r)
    full, remainder = divmod(n, block_length)
    # Cache sufficient statistics per circular block, preserving the same
    # resampled rows for both residuals and both event indicators.
    cache = {}
    for length in {block_length, remainder} - {0}:
        cache[length] = []
        for start in range(n):
            idx = [(start+j) % n for j in range(length)]
            cache[length].append(
                _totals([primary[i] for i in idx], [primary_events[i] for i in idx])
                + _totals([rival[i] for i in idx], [rival_events[i] for i in idx]))
    rng = random.Random(seed)
    draws = [[], []]
    invalid = 0
    lengths = [block_length]*full + ([remainder] if remainder else [])
    for _ in range(replicates):
        totals = [0.0]*8
        for length in lengths:
            block = cache[length][rng.randrange(n)]
            for j, value in enumerate(block):
                totals[j] += value
        p, r = _contrast(totals[:4]), _contrast(totals[4:])
        if p is None or r is None:
            invalid += 1
            continue
        draws[0].append(p)
        draws[1].append(p-r)
    if invalid:
        # The frozen prose does not specify how to handle undefined replicates.
        # Never silently discard or replace them and change the null law.
        return {"status": "invalid", "reason": "undefined_bootstrap_group",
                "invalid_replicates": invalid, "replicates": replicates}

    def quantile(values, q):
        ordered = sorted(values)
        position = (len(ordered)-1)*q
        lo = math.floor(position)
        hi = math.ceil(position)
        return ordered[lo] + (position-lo)*(ordered[hi]-ordered[lo])

    return {"status": "ok", "replicates": replicates, "block_length": block_length,
            "seed": seed, "percentile_convention": "linear",
            "statistics": [{"name": name, "observed": obs,
                "interval95": [quantile(values, .025), quantile(values, .975)],
                "centered_p": (1+sum(x-obs >= obs for x in values))/(replicates+1)}
                for name, obs, values in zip(("primary", "primary_minus_rival"), observed, draws)]}


def holm_rejections(p_values, alpha=.05):
    values = list(p_values)
    if not values or len(values) > 100 or not 0 < alpha < 1:
        raise ValueError("invalid family or alpha")
    if any(not math.isfinite(p) or not 0 <= p <= 1 for p in values):
        raise ValueError("invalid p value")
    rejected = [False]*len(values)
    for rank, index in enumerate(sorted(range(len(values)), key=values.__getitem__)):
        if values[index] > alpha/(len(values)-rank):
            break
        rejected[index] = True
    return rejected
