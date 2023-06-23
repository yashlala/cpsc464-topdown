import numpy as np
import scipy.stats
import pytest
from fractions import Fraction
import mpmath

from programs.queries.querybase import QueryFactory

from das_constants import CC

try:
    import primitives
except ImportError as e:
    import programs.engine.primitives as primitives


def test_primitives() -> None:
    """
    Tests to see if the geometric mechanism is adding noise.
    """

    true_answer = np.array([5.5, 6.5, 7.2, 8.1])
    budget = Fraction(1, 100)
    sensitivity = 5

    X = primitives.RationalGeometricMechanism(inverse_scale=budget, sensitivity=sensitivity,
                                                true_answer=true_answer).protected_answer

    #import __main__
    #print("main is: ", __main__.__file__)
    assert np.all(abs(X - true_answer))


def test_geometric_pmf() -> None:
    """
    The 2sided geometric pmf should be 1/(2-p) multiplier of
    the 1sided geometric pmf
    """
    for epsilon in [Fraction(1, 1), Fraction(1, 10), Fraction(25, 1000)]:
        p = 1 - np.exp(-float(epsilon))
        for x in [0, 4, 6, 7]:
            a = scipy.stats.geom.pmf(x + 1, p)
            b = primitives.RationalGeometricMechanism(epsilon, 1, np.array([0])).pmf(x)
            assert (round(b * (2 - p), 10) == round(a, 10))


def test_geometric_cdf() -> None:
    """
    The geometric cdf function should be equivalent to
    adding up the pdf
    P(X<=x) should be >= quantile
    P(X<=x-1) should be < quantile
    """
    for quantile in [0.01, 0.3, 0.5, 0.7, 0.95]:
        for location in [0, -5, 10]:
            for epsilon in [Fraction(1, 1), Fraction(1, 10), Fraction(25, 1000)]:
                mech = primitives.RationalGeometricMechanism(epsilon, 1, np.array([0]))
                x = int(mech.inverseCDF(quantile, location))
                cdf = 0
                for i in range(-9999, x - 1, 1): # If 9999 is not enough, increase it. It's really -oo.
                    cdf += mech.pmf(i, location)
                assert (cdf < quantile)
                cdf += mech.pmf(x - 1, location)
                assert (cdf >= quantile)


class TestNaNReturns:

    @pytest.mark.parametrize("mechclass", [CC.GEOMETRIC_MECHANISM, CC.DISCRETE_GAUSSIAN_MECHANISM])
    @pytest.mark.parametrize("query", [
        QueryFactory.makeTabularQuery(array_dims=(2,3,4), add_over_margins=(1,)),
        QueryFactory.makeTabularGroupQuery(array_dims=(3,4,5), groupings={0: [1,2], 1: [2,3]})
    ])
    def test_dp_nan(self, mechclass: str, query) -> None:
        mech = primitives.basic_dp_answer(true_data=None, query=query, inverse_scale=Fraction(0, 1), bounded_dp=True, mechanism_name=mechclass)
        assert np.array_equal(np.isnan(mech.protected_answer), np.ones(query.numAnswers()).astype(bool))

    @pytest.mark.parametrize("mechclass", [CC.GEOMETRIC_MECHANISM, CC.DISCRETE_GAUSSIAN_MECHANISM])
    @pytest.mark.parametrize("query", [
        QueryFactory.makeTabularQuery(array_dims=(2, 3, 4), add_over_margins=(1,)),
        QueryFactory.makeTabularGroupQuery(array_dims=(3, 4, 5), groupings={0: [1, 2], 1: [2, 3]})
    ])
    def test_wrong_budget_type(self, mechclass: str, query) -> None:
        with pytest.raises(TypeError, match="should be Fraction"):
            primitives.basic_dp_answer(true_data=None, query=query, inverse_scale=12.3, bounded_dp=True, mechanism_name=mechclass)

    @pytest.mark.parametrize("shape", [(2, 3), 10, (4, 5, 6), 1])
    @pytest.mark.parametrize("inv_scale_value", [12.5, -np.inf, np.nan, 0, None])
    def test_mechanism_wrong_type(self, shape, inv_scale_value) -> None:
        with pytest.raises(AssertionError):
            primitives.RationalDiscreteGaussianMechanism(inverse_scale=inv_scale_value, true_answer=np.random.random(shape))
        with pytest.raises(AssertionError):
            primitives.RationalGeometricMechanism(inverse_scale=inv_scale_value, sensitivity=2, true_answer=np.random.random(shape))


@pytest.mark.parametrize('sigma_sq', [.027] + list(map(lambda d: 10.**d, np.arange(-1, 11))))
def test_computeDiscreteGaussianVariance(sigma_sq) -> None:

    # Alternative calculation of the variance,
    # based on Lemma 22 in Cannone et al. (aka Eqn. (41) in the form based on Fourier transforms (page 23)
    # Also, the numerator and denominator sums in the definition are expressible via Jacobi Theta functions, implemented in mpmath

    # Definition sums are the fastest, then FT sums, then calculation of jtheta which mpmath does with arbitrary precision,
    # hence the definition sums are chosen to be the working implementation and the others are tests

    SQRT2P = np.sqrt(2. * np.pi)
    sigma_sq_fl = float(sigma_sq)
    sigma = np.sqrt(sigma_sq_fl)
    bound = np.floor(100. * sigma)

    t = np.arange(-bound, bound + 1)
    pi_t = np.pi * t
    fac2 = np.exp(-2. * pi_t * pi_t * sigma_sq_fl)
    f_hat_fac1 = SQRT2P * sigma * sigma * sigma
    f_hat_fac3 = 1. - 4. * pi_t * pi_t * sigma_sq_fl
    g_hat_fac1 = SQRT2P * sigma
    sum_f_hat = np.sum(f_hat_fac1 * fac2 * f_hat_fac3)
    sum_g_hat = np.sum(g_hat_fac1 * fac2)
    var_ft = sum_f_hat / sum_g_hat

    mpmath.MPContext.THETA_Q_LIM = 0.99999999999999999
    mpmath.mp.prec = 80
    q = mpmath.exp(-1 / (2 * sigma_sq))
    var_sp_fun = -mpmath.jtheta(3, 0, q, 2) / (4 * mpmath.jtheta(3, 0, q))

    var = primitives.computeDiscreteGaussianVariance(sigma_sq)

    #print(var_ft, var, 2 * abs(var_ft - var) / (var_ft + var) )
    assert 2 * abs(var_ft - var) / (var_ft + var) < 1e-10
    assert 2 * abs(var_sp_fun - var) / (var_sp_fun + var) < 1e-10