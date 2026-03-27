import pytest

from bt.hypotheses.l1_h5a import clipped_inverse_vol_scale


def test_scale_factor_clips_and_flags_cap_hits() -> None:
    scale, hit_lo, hit_hi = clipped_inverse_vol_scale(sigma_t=0.02, sigma_star=0.01, s_min=0.25, s_max=1.5)
    assert scale == pytest.approx(0.5)
    assert hit_lo is False
    assert hit_hi is False

    scale_lo, hit_lo2, hit_hi2 = clipped_inverse_vol_scale(sigma_t=1.0, sigma_star=0.01, s_min=0.25, s_max=1.5)
    assert scale_lo == pytest.approx(0.25)
    assert hit_lo2 is True
    assert hit_hi2 is False

    scale_hi, hit_lo3, hit_hi3 = clipped_inverse_vol_scale(sigma_t=0.005, sigma_star=0.02, s_min=0.25, s_max=1.5)
    assert scale_hi == pytest.approx(1.5)
    assert hit_lo3 is False
    assert hit_hi3 is True


def test_qty_final_overlay_formula() -> None:
    qty_r = (100000.0 * 0.01) / 100.0
    scale, _, _ = clipped_inverse_vol_scale(sigma_t=0.02, sigma_star=0.01, s_min=0.25, s_max=1.5)
    assert qty_r == pytest.approx(10.0)
    assert (qty_r * scale) == pytest.approx(5.0)
