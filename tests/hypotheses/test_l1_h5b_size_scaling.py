import pytest

from bt.hypotheses.l1_h5b import clipped_inverse_vol_scale


def test_l1_h5b_scale_factor_clips_and_flags_hits() -> None:
    scale, hit_lo, hit_hi = clipped_inverse_vol_scale(sigma_t=0.02, sigma_star=0.01, s_min=0.25, s_max=1.5)
    assert scale == pytest.approx(0.5)
    assert hit_lo is False
    assert hit_hi is False

    lo, hit_lo, hit_hi = clipped_inverse_vol_scale(sigma_t=1.0, sigma_star=0.01, s_min=0.25, s_max=1.5)
    assert lo == pytest.approx(0.25)
    assert hit_lo is True
    assert hit_hi is False

    hi, hit_lo, hit_hi = clipped_inverse_vol_scale(sigma_t=0.005, sigma_star=0.02, s_min=0.25, s_max=1.5)
    assert hi == pytest.approx(1.5)
    assert hit_lo is False
    assert hit_hi is True


def test_l1_h5b_qty_final_overlay_formula() -> None:
    qty_r = (100000.0 * 0.01) / (2.0 * 100.0 * (0.0004 ** 0.5))
    scale, _, _ = clipped_inverse_vol_scale(sigma_t=0.02, sigma_star=0.01, s_min=0.25, s_max=1.5)
    assert qty_r == pytest.approx(250.0)
    assert (qty_r * scale) == pytest.approx(125.0)
