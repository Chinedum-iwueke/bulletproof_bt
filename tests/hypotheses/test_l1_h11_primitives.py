from bt.hypotheses.l1_h11 import entry_position_ratio, pullback_depth_atr, swing_distance_atr


def test_l1_h11_primitives_are_deterministic() -> None:
    assert swing_distance_atr(trend_dir="long", trend_anchor_price=100, trend_extreme_price=103, atr=2) == 1.5
    assert pullback_depth_atr(trend_dir="long", ema_fast=102, pullback_extreme_low=101, pullback_extreme_high=103, atr=2) == 0.5
    pos = entry_position_ratio(trend_dir="long", entry_price=102.5, pullback_extreme_low=101, pullback_extreme_high=103, trend_extreme_price=104)
    assert pos is not None and 0 <= pos <= 1
