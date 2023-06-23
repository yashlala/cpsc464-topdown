# Contains noise primitives that are useful for unit testing, but are not intended for use in Production runs of the DAS.
from typing import Union
from programs.engine.rngs import DASRandom
#from programs.engine import primitives
import numpy as np
import warnings
import sys

#class DPMechanism: # Redundant with this same code in programs.engine.primitives; can't import that b/c import is circular
#    """
#        To check type in other parts of the code, use as dummy for node aggregation; also, to hold common structure
#    """
#    protected_answer: np.ndarray
#    epsilon: float
#    variance: float
#    sensitivity: int
#
#    def __repr__(self):
#        return "Dummy DP Mechanism"


#class DiscreteGaussianMechanism(DPMechanism):
#    """
#    Implements Canonne, Kamath, & Steinke's Discrete Gaussian Mechanism
#
#        TODO: add mass fxn + complete description
#
#    Uses BernoulliExp, DiscreteLaplace fxns as subroutines
#
#    This implementation trusts the rng as a source of Discrete Uniform & Bernoulli samples, and uses float ops in several places. It can be thought of as a hybrid, inexact implementation of the exact sampler described in Cannone et al.
#    """
#
#    def __init__(self, variance: float, true_answer: np.ndarray):
#        """
#        :param stddev (float): standard deviation of Gaussian draws
#        :param true_answer: true_answer (float or numpy array): the true answer
#        """
#        self.epsilon = None  # Not used, but constraints_dpqueries expects it
#        self.sensitivity = 2.0  # Not used, but constraints_dpqueries expects it
#        self.scale = variance
#        self.stddev = np.sqrt(self.scale)
#
#        shape = np.shape(true_answer)
#        size = int(np.prod(shape))
#
#        # If the noise scale is infinite return NaN.
#        if not np.isfinite(self.scale):
#            self.protected_answer = np.nan + np.zeros(shape)
#        else:
#            assert primitives._called_from_test or primitives._rng_factory is DASRandom, "Only unit tests may override _rng_factory."
#            rng = primitives._rng_factory()
#            perturbations = SimpleDiscreteGaussian(variance=self.scale, size=size, rng=rng)
#            self.protected_answer = true_answer + np.reshape(perturbations, shape)
#
#    def __repr__(self):
#        return f"HybridDiscreteGaussianMechanism(variance={self.scale})"


#class DiscreteGaussianMechanismCorrectnessCheck(DPMechanism):
#    """
#    Implements Canonne, Kamath, & Steinke's Discrete Gaussian Mechanism
#
#        TODO: add mass fxn + complete description
#
#    Uses BernoulliExp, DiscreteLaplace fxns as subroutines
#
#    This cls implements a second, np.array-based implementation; intended to be faster, but is slower! Serves as a correctness
#           check on the above, first, scalar-based implementation.
#
#    """
#
#    def __init__(self, variance: float, true_answer: np.ndarray):
#        """
#        :param stddev (float): standard deviation of Gaussian draws
#        :param true_answer: true_answer (float or numpy array): the true answer
#        """
#        self.epsilon = None  # Not used but constraints_dpqueries expects it
#        self.sensitivity = 2.0  # Not used, but constraints_dpqueries expects it
#        self.scale = variance
#        self.stddev = np.sqrt(self.scale)
#
#        shape = np.shape(true_answer)
#        size = int(np.prod(shape))
#
#        # If the noise scale is infinite return NaN.
#        if not np.isfinite(self.scale):
#            self.protected_answer = np.nan + np.zeros(shape)
#        else:
#            assert primitives._called_from_test or primitives._rng_factory is DASRandom, "Only unit tests may override _rng_factory."
#            rng = primitives._rng_factory()
#            t = int(np.floor(self.stddev) + 1)
#            # c = np.zeros(size)
#            cactive = np.ones(size, dtype=bool)  # All indices are active
#            y = np.zeros(size)  # Placeholder
#            while np.any(cactive):
#                ynew = DiscreteLaplace(s=1, t=t, size=size, rng=rng)
#                gamma = (np.abs(ynew) - self.scale / t) ** 2.0 / (2. * self.scale)
#                c = BernoulliExp(gamma=gamma, size=size, rng=rng)
#                y[np.logical_and(cactive, c == 1)] = ynew[np.logical_and(cactive, c == 1)]  # On first 1, fix entry to current c val
#                num_valid_samples = np.sum(np.logical_and(cactive, c == 1))
#                cactive[np.logical_and(cactive, c == 1)] = np.zeros(num_valid_samples, dtype=bool)  # Set newly inactive indices
#            self.protected_answer = true_answer + np.reshape(y, shape)
#
#    def __repr__(self):
#        return f"VectorHybridDiscreteGaussianMechanism(variance={self.scale})"


# ---
# Hybrid numpy / exact Discrete Gaussian implementation subroutines (uses many 64-bit float ops)
# ---
def SimpleDiscreteGaussian(variance: float = 1.0, size: int = 1, rng=None):
    return np.fromiter((ScalarDiscreteGaussian(variance=variance, rng=rng) for _ in range(size)), dtype=int)


def ScalarDiscreteGaussian(variance: float = 1.0, rng=None) -> int:
    stddev: float = np.sqrt(variance)
    t: int = int(np.floor(stddev) + 1.)
    c: bool = False
    while not c:
        y: int = ScalarDiscreteLaplace(s=1, t=t, rng=rng)
        gamma: float = (np.abs(y) - variance/t)**2. / (2. * variance)
        c: bool = bool(ScalarBernoulliExp(gamma=gamma, rng=rng))
        if c:
            return y


def SimpleDiscreteLaplace(s=1, t=1, size=1, rng=None):
    return np.fromiter((ScalarDiscreteLaplace(s=s, t=t, rng=rng) for _ in range(size)), dtype=int)


def ScalarDiscreteLaplace(s=1, t=1, rng=None) -> int:
    valid_sample = False
    while not valid_sample:
        d: bool = False
        while not d:
            u: int = int(DiscreteUniform(low=0, high=t, rng=rng))
            d = bool(ScalarBernoulliExp(gamma=u / t, rng=rng))
        v: int = 0
        a: bool = True
        while a:
            a = bool(ScalarBernoulliExp(gamma=1., rng=rng))
            v = v + 1 if a else v
        x: float = u + t * v
        y: int = int(np.floor(x / s))
        b: int = Bernoulli(numer=1, denom=2, rng=rng)
        if not (b == 1 and y == 0):
            return (1 - 2 * b) * y


def SimpleBernoulliExp(gamma: float = 0.0, size: int = 1, rng=None):
    bernoulliExp_samples = np.fromiter((ScalarBernoulliExp(gamma=gamma, rng=rng) for _ in range(size)), dtype=int)
    return bernoulliExp_samples


def ScalarBernoulliExp(gamma: float = 0.0, rng=None) -> int:
    if 0. <= gamma <= 1.:
        k: int = 1
        a: bool = True
        while a:
            a = bool(Bernoulli(numer=gamma, denom=k, rng=rng))
            k = k+1 if a else k
        return k % 2
    else:
        for k in range(1, int(np.floor(gamma))+1):
            b: bool = bool(ScalarBernoulliExp(gamma=1., rng=rng))
            if not b:
                return 0
        c: int = ScalarBernoulliExp(gamma=gamma-np.floor(gamma), rng=rng)
        return c

def DiscreteUniform(*, low=0, high=100, rng) -> int:
    """
        This function isolates use of the rng Discrete Uniform, which requires trusting the input generator, rng. It
        draws a single numpy.int64 (for most choice of rng; or a single Python int of the byte size determined by :high:) integer,
        and returns the number, which is drawn from a distribution with mass function
                                            Pr[X = x] = 1/(high - low), for x in {low, low+1, ..., high-1}
        Inputs:
                low: int, inclusive lower bound on value to be drawn
                high: int > low+1, exclusive upper bound on value to be drawn
                rng: pseudo-random (or maybe not pseudo?) number generator (see programs.engine.rngs)
        Output:
                rng.randint: int (specific type of int depends on rng) uniformly drawn from {low, low+1, ..., high-1}
    """
    # assert isinstance(low, int) and isinstance(high, int) and high > low
    try:
        # This works with DASRandom
        return int(rng.integers(low=low, high=high))
    except AttributeError:
        # This works with other systems
        return int(rng.randint(low=low, high=high))
    # Otherwise throw whichever error there is if there is one


def VectorDiscreteUniform(low=0, high=100, size: int = 1, rng=None) -> int:
    # This function isolates use of the rng Discrete Uniform
    try:
        # This works with DASRandom
        return rng.integers(low=low, high=high, size=size)
    except AttributeError:
        # This works with other systems
        return rng.randint(low=low, high=high, size=size)


def Bernoulli(numer: float = 1.0, denom: float = 1.0, rng=None):
    # This function isolates use of the rng Bernoulli
    # (This for-testing-only implementation trusts the rng for Bernoulli r.v.'s)
    return rng.binomial(n=1, p=numer/denom)


def VectorBernoulli(numer: float = 1.0, denom: float = 1.0, size: int = 1, rng=None):
    # This function isolates use of the rng Bernoulli
    # (This for-testing-only implementation trusts the rng for Bernoulli r.v.'s)
    return rng.binomial(n=1, p=numer/denom, size=size)


# ---
# Subroutines for slow hybrid Discrete Gaussian implementation using numpy vectors (much slower than scalar versions; only used for unit tests)
# ---
def DiscreteLaplace(s=1, t=1, size: int = 1, rng=None):
    assert rng is not None, "DiscreteLaplace expects an rng."
    zactive = np.ones(size, dtype=bool)  # All z indices active
    zfinal = np.ones(size)  # Placeholder
    while np.any(zactive):
        # Inner loop 1: generate valid vector of u samples
        ucomplete = np.zeros(size, dtype=bool)  # No u values are complete yet
        u = np.zeros(size)  # Placeholder
        while not np.all(ucomplete):  # Terminate when all u samples are complete
            unew = VectorDiscreteUniform(low=0, high=t, size=size, rng=rng)
            d = BernoulliExp(gamma=unew / t, size=size, rng=rng)
            u[np.logical_and(ucomplete == 0, d != 0)] = unew[np.logical_and(ucomplete == 0, d != 0)]  # Set new valid u samples
            num_valid_samples = np.sum(np.logical_and(ucomplete == 0, d != 0))
            ucomplete[np.logical_and(ucomplete == 0, d != 0)] = np.ones(num_valid_samples, dtype=bool)  # set valid indices to inactive

        # Inner loop 2: determine how large v vector is
        v = np.zeros(size)
        # a = np.ones(size)
        aactive = np.ones(size, dtype=bool)  # All indices active
        while np.any(aactive):
            a = BernoulliExp(gamma=1., size=size, rng=rng)
            v[np.logical_and(aactive, a == 1)] = v[np.logical_and(aactive, a == 1)] + 1  # Increment actives where a==1
            num_newly_inactive = np.sum(np.logical_and(aactive, a == 0))
            aactive[np.logical_and(aactive, a == 0)] = np.zeros(num_newly_inactive, dtype=bool)  # Set newly inactive indices

        # Outer loop: check which samples are valid for zfinal, update appropriately
        x = u + t * v
        y = np.floor(x / s)
        b = VectorBernoulli(numer=1, denom=2, size=size, rng=rng)

        valid_samples_mask = np.logical_and(zactive == 1, np.logical_not(np.logical_and(b == 1, y == 0)))
        zfinal[valid_samples_mask] = ((1 - 2 * b) * y)[valid_samples_mask]
        num_valid_samples = np.sum(valid_samples_mask)
        zactive[valid_samples_mask] = np.zeros(num_valid_samples, dtype=bool)  # Entries with valid samples become inactive
    return zfinal


def BernoulliExp(gamma: Union[float, np.ndarray]=0.0, size=1, rng=None):
    """
        Implementation of sampling from Bernoulli(Exp(-gamma)) from Cannone, Kamath, & Steinke

        (slightly generalized to yield an i.i.d. vector of len size, rather than a scalar)
    """
    assert rng is not None, "BernoulliExp expects an rng."
    gamma = np.array(gamma)
    if gamma.size == 1:
        gamma = gamma.repeat(size)
    if np.all(gamma >= 0.) and np.all(gamma <= 1.):
        active_inds = np.arange(size, dtype=int)
        K = np.ones_like(gamma)
        num_active = gamma.size
        while len(active_inds) > 0:
            new_samples = np.array([VectorBernoulli(numer=gamma[i], denom=K[i], size=1, rng=rng)[0] for i in active_inds])
            active_inds = active_inds[new_samples == 1]
            K[active_inds] += 1  # Increment K for active indices
        return K % 2  # Return binary vector with 0 for even, 1 for odd entries
    else:
        B_zeros_mask = np.zeros_like(gamma)  # No indices are marked as B-zeros
        C = np.ones_like(gamma)  # Placeholder
        Bnew = np.ones_like(gamma)  # Placeholder
        max_k = int(np.max(np.floor(gamma))) + 1
        for k in range(1, max_k):
            # print(f"k {k} of {max_k}")
            B_zeros_sum = int(np.sum(B_zeros_mask == 0))
            if B_zeros_sum > 0:
                Bnew[B_zeros_mask == 0] = BernoulliExp(gamma=1., size=B_zeros_sum, rng=rng)
                num_valid_samples = np.sum(Bnew == 0)
                C[Bnew == 0] = np.zeros(num_valid_samples)  # C set to 0 where Bnew==0
                B_zeros_mask[Bnew == 0] = np.ones(num_valid_samples)  # Indices with Bnew==0 now inactive
                B_zeros_mask[k > np.floor(gamma)] = np.ones(np.sum(k > np.floor(gamma)))  # Indices above loop limit now inactive
        # Entries of C not set to zero are sampled in recursive call
        not_B_zeros_mask = np.logical_not(B_zeros_mask)
        not_B_zeros_sum = int(np.sum(not_B_zeros_mask))
        masked_gamma_floor = (gamma - np.floor(gamma))[not_B_zeros_mask]
        if not_B_zeros_sum > 0:
            C[np.logical_not(B_zeros_mask)] = BernoulliExp(gamma=masked_gamma_floor, size=not_B_zeros_sum, rng=rng)
        return C
