import pytest
# from functools import reduce
# from operator import mul
from fractions import Fraction
import numpy as np
from programs.engine.discrete_gaussian_utility import limit_denominator, RationalSimpleDiscreteGaussian, \
    RationalScalarDiscreteLaplace, RationalSimpleBernoulliExp, floorsqrt, RationalScalarBernoulliExp


@pytest.mark.parametrize("num, denom, limit", [
    (3141592653589793,1000000000000000, 10),
    (3141592653589793,1000000000000000, 100),
    (4321, 8765, 10000),
    (7179417880, 7736550926, 7856155428),
    (3218099870, 3554356066, 3043165548),
])
def test_limit_denominator(num: int, denom: int, limit: int):
    ans = limit_denominator((num, denom), limit)
    true_ans = Fraction(num, denom).limit_denominator(limit)
    assert ans == (true_ans.numerator, true_ans.denominator), f"num, denom, limit: {num}, {denom}, {limit}"


@pytest.mark.parametrize("high", [10000000000, 1000, 100, 5])
def test_limit_denominator_random_cases(high) -> None:
    high, size = (10000000000, 1000)
    for num, denom, limit in zip(np.random.randint(0, high, size).tolist(), np.random.randint(1, high, size).tolist(), np.random.randint(2, high, size).tolist()):
        ans = limit_denominator((num, denom), limit)
        true_ans = Fraction(num, denom).limit_denominator(limit)
        assert ans == (true_ans.numerator, true_ans.denominator), f"num, denom, limit: {num}, {denom}, {limit}"

@pytest.mark.parametrize("num, denom, limit", [
    (3141592653589793,1000000000000000, 10),
    (3141592653589793,1000000000000000, 100),
    (4321, 8765, 10000),
    (7179417880, 7736550926, 7856155428),
    (3218099870, 3554356066, 3043165548),
    (4878775236920327, 9007199254740992, 1024),
    (2064212098982325, 18014398509481984, 1024)
])
def test_limit_denominator_upper_lower(num: int, denom: int, limit: int) -> None:
    lower = limit_denominator((num, denom), limit, mode="lower")
    best = limit_denominator((num, denom), limit, mode="best")
    upper = limit_denominator((num, denom), limit, mode="upper")
    assert  lower[0]/lower[1]<= best[0]/best[1] <= upper[0]/upper[1]


def test_limit_denominator_errors() -> None:
    errmsg_match = r"Argument to limit\_denominator should be a fraction represented as tuple"
    with pytest.raises(TypeError, match=errmsg_match):
        limit_denominator(1, 2)
    with pytest.raises(ValueError, match=errmsg_match):
        limit_denominator((1,), 2)
    with pytest.raises(ValueError, match=errmsg_match):
        limit_denominator((1, 2, 3), 2)


@pytest.mark.parametrize("sigma_sq", [1.0, [], 'sdf', {}])
@pytest.mark.parametrize("size", [2.0, [], 'sdf', {}, 0])
def test_RationalSimpleDiscreteGaussian_errors(sigma_sq, size) -> None:
    errmsg_match = r"Argument to RationalSimpleDiscreteGaussian should be a Fraction or int"
    with pytest.raises(AttributeError, match=errmsg_match):
        RationalSimpleDiscreteGaussian(sigma_sq=sigma_sq, size = 1, rng=None)
    with pytest.raises(AssertionError):
        RationalSimpleDiscreteGaussian(sigma_sq=1, size=size, rng=None)


@pytest.mark.parametrize("s", [1.0, [], 'sdf', {}])
@pytest.mark.parametrize("t", [2.0, [], 'sdf', {}, 0])
def test_RationalScalarDiscreteLaplacen_errors(s, t) -> None:
    with pytest.raises(AssertionError):
        RationalScalarDiscreteLaplace(s=2, t=t, rng=None)
    with pytest.raises(AssertionError):
        RationalScalarDiscreteLaplace(s=s, t=2, rng=None)


@pytest.mark.parametrize("gamma", [1.0, [], 'sdf', {}])
@pytest.mark.parametrize("size", [2.0, [], 'sdf', {}, 0])
def test_RationalSimpleBernoulliExp_errors(gamma, size) -> None:
    errmsg_match = r"Argument to RationalSimpleBernoulliExp should be a Fraction or int"
    with pytest.raises(AttributeError, match=errmsg_match):
        RationalSimpleBernoulliExp(gamma=gamma, size=1, rng=None)
    with pytest.raises(AssertionError):
        RationalSimpleBernoulliExp(gamma=1, size=size, rng=None)


@pytest.mark.skip  # Commented this function out and replaced by RationalScalarBernoulliExpFactors
@pytest.mark.parametrize("gamma", [1.0, [], 'sdf', {}])
def test_RationalScalarBernoulliExp_errors(gamma) -> None:
    errmsg_match = r"Argument to RationalScalarBernoulliExp should be a fraction represented as tuple"
    with pytest.raises((TypeError, ValueError), match=errmsg_match):
        RationalScalarBernoulliExp(gamma=gamma, rng=None)


@pytest.mark.parametrize("num, denom", [
    (2, -1),
    (1.0, 2),
    (1, 2.0),
    (-1, 10),
    (1, 0)
])
def test_floorsqrt_errors(num: float, denom: float) -> None:
    with pytest.raises(AssertionError):
        floorsqrt(num, denom)


# @pytest.mark.parametrize("factors", [
#     (42534523, 325532345234, 2345324, 234534523),
#     (2342, 45894856, 2345587976896, 22143),
#
# ])
# @pytest.mark.parametrize("fract", [
#     (1, 2),
#     (1, 3),
#     (4, 3),
#
# ])
# def test_RationalScalarBernoulliExpFactors(factors, fract):
#     denom = reduce(mul, factors)
#     num = fract[0] * denom // fract[1] + 1
#     rng = np.random.RandomState()
#     p = np.exp(-float(fract[0])/ (fract[1]))
#     N = 100000
#     assert abs(np.mean([RationalScalarBernoulliExpFactors(numer=num, denom_factors=factors, rng=rng) for _ in range(N)]) - p) < 4 * np.sqrt(p * (1-p)/N)
