import pytest

from bt.governance.residual_bootstrap import paired_bootstrap, holm_rejections


def test_identical_paired_models_have_exactly_zero_advantage():
    e = [False, True]*20
    y = [float(i % 7) for i in range(40)]
    result = paired_bootstrap(y, y, e, e, block_length=4, replicates=99)
    assert result['status'] == 'ok'
    a = result['statistics'][1]
    assert a['observed'] == 0
    assert a['interval95'] == [0, 0]
    assert a['centered_p'] == 1
    assert result == paired_bootstrap(y, y, e, e, block_length=4, replicates=99)


def test_constant_group_contrast_and_partial_final_block():
    e = [False, True]*21
    p = [2.0 if x else 0.0 for x in e]
    result = paired_bootstrap(p, [0.0]*42, e, e, block_length=5, replicates=99)
    assert result['status'] == 'ok'
    assert result['statistics'][0]['interval95'] == [2, 2]
    assert result['statistics'][1]['observed'] == 2
    assert result['statistics'][0]['centered_p'] == .01


def test_missing_groups_fail_closed_instead_of_dropping_draws():
    assert paired_bootstrap([1, 2], [1, 2], [False]*2, [True]*2,
                            block_length=1, replicates=99)['reason'] == 'missing_observed_group'
    result = paired_bootstrap([1, 2], [1, 2], [False, True], [False, True],
                              block_length=1, replicates=99)
    assert result['reason'] == 'undefined_bootstrap_group'
    assert result['invalid_replicates'] > 0


def test_holm_stops_at_first_failure_and_restores_original_order():
    assert holm_rejections([.04, .001, .03, .02]) == [False, True, False, False]
    assert holm_rejections([.001, .002, .003, .004]) == [True]*4
    with pytest.raises(ValueError):
        holm_rejections([float('nan')])


def test_nonfinite_residuals_rejected():
    with pytest.raises(ValueError):
        paired_bootstrap([1, float('nan')], [1, 2], [True, False], [True, False],
                         block_length=1)


def test_compute_budget_is_bounded():
    with pytest.raises(ValueError, match='work budget'):
        paired_bootstrap([0.0]*10000, [0.0]*10000, [True, False]*5000,
                         [True, False]*5000, block_length=1)

