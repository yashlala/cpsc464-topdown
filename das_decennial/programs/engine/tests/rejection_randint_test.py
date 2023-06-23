import pytest
import numpy as np
from distutils.util import strtobool
from scipy import stats

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from programs.engine.rngs import DASRDRandIntegers


@pytest.fixture
def rng() -> DASRDRandIntegers:
    return DASRDRandIntegers()


@pytest.fixture
def n_std(prod) -> int:
    if strtobool(prod) == 0:
        return 4.
    return 8.

@pytest.fixture
def sig_level(prod) -> float:
    # Provides significance level of one evaluation of pytest function
    if strtobool(prod) == 0:
        return 0.001
    return 0.0001

@pytest.mark.parametrize("nbits, nint64", [
    (3, 1),
    (63, 1),
    (64, 2),
    (125, 2),
    (127, 2),
    (128, 3),
])
def test_nextint(rng, nbits, nint64) -> None:
    bl = rng._nextint(nbits // 64 + 1).bit_length()
    assert bl <= 64 * nint64
    assert 64 * nint64 // bl == 1


@pytest.mark.parametrize("a, b", [
    (1234132, 2345252345),
    (0, 3),
    (0, 1 << 20),
    (0, 1 << 30),
    (0, (1 << 30) + 247852475),
    (0, (1 << 63)),
    (0, (1 << 62) + 130091709234),
    (0, (1 << 63) + 1),
    (0, (1 << 64) + 1),
    (0, (1 << 64) - 1 ),
    (0, (1 << 64) + 1),
    (123213, (1 << 64) + 2143053948),
    (123213, (1 << 85) + 2143053948),
    (-123213, (1 << 64) + 2143053948),
    (123213, -(1 << 85) + 2143053948),
])
def test_integers(rng, a: int, b: int, n_std) -> None:
    alg = DASRDRandIntegers()
    N = 10000
    n = b - a
    if a > b:
        with pytest.raises(ValueError, match="Interval length is not positive"):
            alg.integers(low=a, high=b)
        return
    draws = [alg.integers(low=a, high=b) for _ in range(N)]
    assert sum([d >= a for d in draws]) == N
    assert sum([d < b for d in draws]) == N
    if n > 100000:
        # Most significant bit returns 0s and 1s with prob 1/2. variance is p(1-p)/N = 1 / 4N
        assert (sum([d >= (a + n // 2) for d in draws]) / N - 0.5) ** 2 < n_std * n_std / 4. / N  # n_std stdev -> n_std^2 variances
        assert (sum([d < (a + n // 2) for d in draws]) / N - 0.5)  ** 2 < n_std * n_std / 4. / N  # n_std stdev -> n_std^2 variances
        # Least significant bit returns 0s and 1s with prob 1/2. variance is p(1-p)/N = 1 / 4N
        assert (sum([d % 2 for d in draws]) / N - 0.5) ** 2 < n_std * n_std / 4. / N  # n_std stdev -> n_std^2 variances
    m = float(sum(draws)) / N
    # This is central limit theorem for N uniform i.i.d. Uniform variance is (n^2 - 1) / 12
    assert (m - (a + b - 1) / 2) ** 2 <  n_std * n_std * (n * n - 1.) / 12./ N


@pytest.mark.parametrize("a", [
    0, 1, 2, 10, 100, 1000, 1 << 64, 1 << 80
])
def test_integers_0123(rng, a: int, n_std) -> None:
    alg = DASRDRandIntegers()
    N = 10000
    # rangelen = 0
    with pytest.raises(ValueError, match="Interval length is not positive"):
        float(sum([alg.integers(low=a, high=a) for _ in range(N)])) / N

    # rangelen = 1
    # variance is p(1-p)/N = 1 / 2 * 1 / 2 / N = 1/4N
    draws = [alg.integers(low=a, high=a + 1) for _ in range(N)]
    assert sum([d == a for d in draws]) == N

    # rangelen = 2
    draws = [alg.integers(low=a, high=a + 2) for _ in range(N)]
    assert (sum([d == a for d in draws]) / N - 0.5) ** 2 < n_std * n_std / 4. / N
    assert (sum([d == a + 1 for d in draws]) / N - 0.5) ** 2 < n_std * n_std / 4. / N

    # rangelen = 3
    # variance is p(1-p)/N = 1 / 3 * 2/3 / N = 2/9N
    draws = [alg.integers(low=a, high=a + 3) for _ in range(N)]
    assert (sum([d == a for d in draws]) / N - 1./3.) ** 2 < n_std * n_std * 2./9./N
    assert (sum([d == a + 1 for d in draws]) / N - 1./3.) ** 2 < n_std * n_std * 2./9./N
    assert (sum([d == a + 2 for d in draws]) / N - 1. / 3.) ** 2 < n_std * n_std * 2./9./N


@pytest.mark.parametrize("a, b", [
    (1, 6),
    (10, 16),
    (10, 74),
    (0, 100),
    (0, 1000),
])
def test_integers_small(rng, a: int, b: int, sig_level) -> None:
    """ Seeing each number is drawn equal number of times. Multinomial distribution."""
    alg = DASRDRandIntegers()
    N = 100000
    n = b - a
    p = 1. / n
    h = np.zeros(n, dtype=int)
    # Use a Bonferroni correction:
    fail_prob = sig_level / n
    cutoff_low = stats.binom.ppf(fail_prob / 2, N, 1 / n)
    cutoff_high = stats.binom.ppf(1 - fail_prob / 2, N, 1 / n)
    for _ in range(N):
        h[(alg.integers(low=a, high=b) - a)] +=1
    assert np.all([cutoff_low <= hk <= cutoff_high for hk in h])
