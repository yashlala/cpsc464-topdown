# Simple script used to plot various primitive noise-generation functions in matplotlib
import logging
import os
import sys
import pytest

sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname( os.path.dirname(__file__)))))

import numpy as np
from scipy import stats

from distutils.util import strtobool
from programs.engine.tests.exact_discrete_Anderson_Darling import cvm_test, cdf_eval
from programs.engine.tests.test_cannoneEtAl_exactDiscGauss_benchmark_implementation import sample_dgauss as cannone_sample_dgauss
from programs.engine.tests.test_cannoneEtAl_exactDiscGauss_benchmark_implementation import variance as cannone_variance
import programs.engine.primitives as primitives
import programs.engine.discrete_gaussian_utility as discrete_gaussian_utility
import programs.engine.tests.test_eps_delta_utility as test_eps_delta_utility
# from programs.engine.rngs import DASRandom
from fractions import Fraction

#from scipy.stats import anderson

RNG_FIXED_SEED = 3141592653

logging.warning("TEST CODE! NOT SUITABLE FOR PRODUCTION USE. NUMPY MT19937 IN USE")

class MT19937Wrapper:

    def __init__(self, seed: int = None):
        self.rng = np.random.RandomState(seed=seed)

    def integers(self, low: int = 0, high: int = 0) -> int:
        return self.rng.randint(low=low, high=high)

    def binomial(self, *args, **kwargs):
        return self.rng.binomial(*args, **kwargs)


rng = MT19937Wrapper() # Used in fixed-seed tests below. *Not* used in main DAS.


num_samples = 10000             # Number of samples for comparisons
num_bins = 10                   # num_bins for empirical histogram comparisons (edges automatically chosen by numpy)
compare_to_rng = True          # Should we compare exact samplers to the closest available samplers in the rng object?
compare_to_vector = False       # Should we compare simple "exact" samplers to their vectorized nparray equivalents? (Slow!)
compare_to_exact = True        # Should we compare simple "exact" samplers to exact-rational samplers?
compare_cannone_et_al = True    # Should we compare exact-rational discrete Gaussian to Cannone et al's implementation?

test_discrete_gaussian_float = True      # Perform Anderson-Darling test on the discrete Gaussian mechanism?
test_discrete_gaussian_rational = True   # Perform Anderson-Darling test on the exact-rational discrete Gaussian mechanism?
sig_level = 0.01                         # significance level of Anderson-Darling tests

ljustAmt = 30
distributional_test_threshold = 0.05    # If L1 difference in empirical mass in any bin exceeds this, fail
                                        # (Set to a very generous 0.05 by default, in the absence of a carefully justified bound)
variance_test_threshold = 0.001

@pytest.fixture
def adaptive_sig_level(prod) -> float:
    # Provides significance level of one evaluation of pytest function
    if strtobool(prod) == 0:
        return 0.001
    return 0.0001

def printComparisons(counts1: int, counts2: int, bins):
    differences = counts1 - counts2
    print("counts1:".ljust(ljustAmt) + f"{counts1}")
    print("counts2:".ljust(ljustAmt) + f"{counts2}")
    print("bin edges:".ljust(ljustAmt) + f"{bins}")
    print("differences:".ljust(ljustAmt) + f"{differences}")
    normalized_differences = differences/num_samples
    print("normalized differences:".ljust(ljustAmt) + f"{normalized_differences}")
    error_msg = f"Detected difference that exceeds threshold of {distributional_test_threshold}"
    assert np.max(np.abs(normalized_differences)) <= distributional_test_threshold, error_msg


def test_BernoulliExp(adaptive_sig_level) -> None:
    simpleBernExp = test_eps_delta_utility.SimpleBernoulliExp
    rationalBernExp = discrete_gaussian_utility.RationalSimpleBernoulliExp
    bernoulliExp = test_eps_delta_utility.BernoulliExp
    gammas = [0.001, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 10.0, 100.0]
    dists = [(lambda gamma: bernoulliExp(gamma=gamma, size=num_samples, rng=rng)),
             (lambda gamma: rng.binomial(n=1, p=np.exp(-gamma), size=num_samples)),
             (lambda gamma: simpleBernExp(gamma=gamma, size=num_samples, rng=rng)),
             (lambda gamma: rationalBernExp(gamma=Fraction(gamma), size=num_samples, rng=rng))]
    n_tests = len(gammas) * len(dists)
    # Use a Sidak correction, since all tests are independent:
    fail_prob = 1 - (1 - adaptive_sig_level) ** (1 / n_tests)
    for gamma_k in gammas:
        if np.exp(-gamma_k) * num_samples < fail_prob:
            # These cutoff values follow from a union bound:
            cutoff_low, cutoff_high = 0, 0
        else:
            cutoff_low = stats.binom.ppf(fail_prob / 2, num_samples, np.exp(-gamma_k))
            cutoff_high = stats.binom.ppf(1 - fail_prob / 2, num_samples, np.exp(-gamma_k))
        for k, dist_k in enumerate(dists):
            sample = dist_k(gamma_k)
            assert np.all([sample_k in (0, 1) for sample_k in sample])
            assert cutoff_low <= np.sum(sample) <= cutoff_high, f"{cutoff_low, np.sum(sample), cutoff_high, k}"


def test_DiscreteLaplace() -> None:
    exactVecDiscreteLaplace = test_eps_delta_utility.DiscreteLaplace
    if compare_to_vector:
        print("\nPerforming simple distributional tests... (tests if distributions have similar mass in same bins)")
        exactSimpDiscreteLaplace = test_eps_delta_utility.SimpleDiscreteLaplace
        for t in [1.0, 10.0, 100.0]:
            print("\n < --- [1] exact vector DiscreteLaplace VS [2] exact simple scalar DiscreteLaplace --- >")
            exactVecDiscLap_samples = exactVecDiscreteLaplace(s=1.,t=t, size=num_samples, rng=rng)
            exactVecDiscLap_counts, exactVecDiscLap_bins = np.histogram(exactVecDiscLap_samples, bins=num_bins)
            exactSimpDiscLap_samples = exactSimpDiscreteLaplace(s=1., t=t, size=num_samples, rng=rng)
            exactSimpDiscLap_counts, exactSimpDiscLap_bins = np.histogram(exactSimpDiscLap_samples, bins=exactVecDiscLap_bins)
            printComparisons(exactVecDiscLap_counts, exactSimpDiscLap_counts, exactVecDiscLap_bins)

    if compare_to_exact:
        print("\nPerforming simple distributional tests... (tests if distributions have similar mass in same bins)")
        exactSimpDiscreteLaplace = test_eps_delta_utility.SimpleDiscreteLaplace
        rationalDiscreteLaplace = discrete_gaussian_utility.RationalSimpleDiscreteLaplace
        for t in [1.0, 10.0, 100.0]:
            print(f"\n < --- with t {t}, [1] exact simple scalar DiscreteLaplace VS [2] exact rational DiscreteLaplace --- >")
            exactSimpDiscLap_samples = exactSimpDiscreteLaplace(s=1., t=t, size=num_samples, rng=rng)
            exactSimpDiscLap_counts, exactSimpDiscLap_bins = np.histogram(exactSimpDiscLap_samples, bins=num_bins)
            rationalDiscLap_samples = rationalDiscreteLaplace(s=1, t=int(t), size=num_samples, rng=rng)
            rationalDiscLap_counts, rationalDiscLap_bins = np.histogram(rationalDiscLap_samples, bins=exactSimpDiscLap_bins)
            printComparisons(exactSimpDiscLap_counts, rationalDiscLap_counts, exactSimpDiscLap_bins)


def test_DiscreteGaussian() -> None:
    # This mechanism is commented out due to import problems when using it for December testing, and unwillingness to move it
    #   back into main DAS code:
    #exactVecDiscGaussian = test_eps_delta_utility.DiscreteGaussianMechanismCorrectnessCheck
    #if compare_to_vector:
    #    print("\nPerforming simple distributional tests... (tests if distributions have similar mass in same bins)")
    #    simpleDiscreteGaussian = test_eps_delta_utility.DiscreteGaussianMechanism
    #    for stddev in [0.1, 1.0, 10.0, 100.0]:
    #        print("\n < --- [1] exact vector DiscreteGaussian VS [2] exact simple scalar DiscreteGaussian --- >")
    #        primitives._rng_factory = lambda: MT19937Wrapper()
    #        exactVecDiscGauss_samples = exactVecDiscGaussian(variance=stddev**2.0,
    #                                                        true_answer=np.zeros(num_samples)).protected_answer
    #        exactVecDiscGauss_counts, exactVecDiscGauss_bins = np.histogram(exactVecDiscGauss_samples, bins=num_bins)
    #        exactSimpDiscGauss_samples = simpleDiscreteGaussian(variance=stddev**2.0,
    #                                                        true_answer=np.zeros(num_samples)).protected_answer
    #        exactSimpDiscGauss_counts, exactSimpDiscGauss_bins = np.histogram(exactSimpDiscGauss_samples,
    #                                                                            bins=exactVecDiscGauss_bins)
    #        printComparisons(exactVecDiscGauss_counts, exactSimpDiscGauss_counts, exactVecDiscGauss_bins)

    if compare_to_exact:
        print("\nPerforming simple distributional tests... (tests if distributions have similar mass in same bins)")
        simpleDiscreteGaussian = test_eps_delta_utility.SimpleDiscreteGaussian
        rationalDiscreteGaussian = primitives.RationalDiscreteGaussianMechanism
        for stddev in [Fraction(1,10), Fraction(1,1), Fraction(10,1), Fraction(100,1)]:
            print(f"\n < --- with stddev {stddev}, [1] exact simple scalar DiscreteGaussian VS [2] exact rational DiscreteGaussian --- >")
            exactSimpDiscGauss_samples = simpleDiscreteGaussian(variance=float(stddev**2.0), size=num_samples, rng=np.random.RandomState())
            exactSimpDiscGauss_counts, exactSimpDiscGauss_bins = np.histogram(exactSimpDiscGauss_samples, bins=num_bins)

            primitives._rng_factory = lambda: MT19937Wrapper()
            primitives._rng_factory = lambda: MT19937Wrapper()
            rationalDiscreteGauss_samples = rationalDiscreteGaussian(inverse_scale=1/(stddev**2), true_answer=np.zeros(num_samples)).protected_answer
            rationalDiscGauss_counts, rationalDiscGauss_bins = np.histogram(rationalDiscreteGauss_samples,
                                                                                bins=exactSimpDiscGauss_bins)
            printComparisons(exactSimpDiscGauss_counts, rationalDiscGauss_counts, exactSimpDiscGauss_bins)

    if compare_cannone_et_al:
        cannoneDiscreteGaussian = cannone_sample_dgauss
        rationalDiscreteGaussian = primitives.RationalDiscreteGaussianMechanism
        for sigma in [Fraction(1,10), Fraction(1,1), Fraction(10,1)]:
            print(f"\n < --- with sigma {sigma}, [1] Cannone et al DiscreteGaussian VS [2] exact rational DiscreteGaussian --- >")
            sigma2 = Fraction(sigma**2)
            rng = MT19937Wrapper(seed=RNG_FIXED_SEED)
            cannoneDiscGauss_samples = np.array([cannoneDiscreteGaussian(sigma2=sigma2, rng=rng) for _ in range(num_samples)])
            cannoneDiscGauss_counts, cannoneDiscGauss_bins = np.histogram(cannoneDiscGauss_samples, bins=num_bins)

            primitives._rng_factory = lambda: MT19937Wrapper(seed=RNG_FIXED_SEED)
            rationalDiscGauss_samples = rationalDiscreteGaussian(inverse_scale=1/sigma2, true_answer=np.zeros(num_samples)).protected_answer
            rationalDiscGauss_counts, cannoneDiscGauss_bins = np.histogram(rationalDiscGauss_samples, bins=cannoneDiscGauss_bins)
            printComparisons(cannoneDiscGauss_counts, rationalDiscGauss_counts, cannoneDiscGauss_bins)

            cannone_computed_variance = cannone_variance(sigma2)
            primitives._rng_factory = lambda: MT19937Wrapper(seed=RNG_FIXED_SEED)
            rational_computed_variance = primitives.computeDiscreteGaussianVariance(sigma2)
            print(f"Cannone variance vs exact rational DiscreteGaussian variance: {cannone_computed_variance}, {rational_computed_variance}")
            assert abs(cannone_computed_variance - rational_computed_variance) < variance_test_threshold


def AD_test_DiscreteGaussian() -> None:
    if test_discrete_gaussian_float:
        print("\nTesting the null-hypotheses that the CDFs are discrete Gaussian using Anderson-Darling tests")
        simpleDiscreteGaussian = test_eps_delta_utility.DiscreteGaussianMechanism
        for sigma in [1.0, 10.0, 100.0]:
            print("\n < --- exact simple scalar DiscreteGaussian --- >")
            exactSimpDiscGauss_samples = simpleDiscreteGaussian(variance=sigma**2.0,
                                                                true_answer=np.zeros(num_samples)).protected_answer
            bound = int(max(4. * sigma, 1.))
            elements_in_domain = np.arange(-bound, bound + 1)
            p_value = cvm_test(exactSimpDiscGauss_samples, lambda ks: cdf_eval(ks, bound, sigma), elements_in_domain)
            error_msg = f"Rejected the null hypothesis that the distribution is Discrete Gaussian with a p-value of {p_value}"
            assert p_value > sig_level, error_msg

    if test_discrete_gaussian_rational:
        print("\nTesting the null-hypotheses that the CDFs are discrete Gaussian using Anderson-Darling tests")
        rationalDiscreteGaussian = primitives.RationalDiscreteGaussianMechanism
        for sigma in [1.0, 10.0, 100.0]:
            print(f"\n < --- with sigma {sigma}, exact rational DiscreteGaussian --- >")
            primitives._rng_factory = lambda: MT19937Wrapper()
            rationalDiscreteGauss_samples = np.array([rationalDiscreteGaussian(inverse_scale=Fraction(1./sigma**2.0),
                                                true_answer=np.zeros(1)).protected_answer[0] for _ in range(num_samples)])
            bound = int(max(4. * sigma, 1.))
            elements_in_domain = np.arange(-bound, bound + 1)
            p_value = cvm_test(rationalDiscreteGauss_samples, lambda ks: cdf_eval(ks, bound, sigma), elements_in_domain)
            error_msg = f"Rejected the null hypothesis that the distribution is Discrete Gaussian with a p-value of {p_value}"
            assert p_value > sig_level, error_msg


if __name__ == "__main__":
    checkFxns = []  # Which mechanisms should we test?
    checkFxns += [test_DiscreteGaussian]
    checkFxns += [test_DiscreteLaplace]
    checkFxns += [test_BernoulliExp]
    checkFxns += [AD_test_DiscreteGaussian]

    for checkFxn in checkFxns:
        print(f"\n\n CHECKING {checkFxn} !")
        checkFxn()
