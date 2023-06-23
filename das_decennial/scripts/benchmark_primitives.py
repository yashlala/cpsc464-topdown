import time
from fractions import Fraction
import os,sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from programs.engine.discrete_gaussian_utility import RationalSimpleDiscreteLaplace, RationalSimpleDiscreteGaussian
from programs.engine.rngs import DASRandom, DASRDRandIntegers
# from programs.engine.tests.test_eps_delta_utility import SimpleDiscreteGaussian as FloatDiscreteGaussian # TODO: remove this? Uses copious float-ops; but, its speed is useful for large-scale experiments


# N = 1000
# t0 = time.time()
# for s in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]:
#     RationalSimpleDiscreteLaplace(s=s, t=8, size=N, rng=DASRandom())
# print(f"{N} DiscreteLaplace time: {time.time() - t0}")
#
# t0 = time.time()
# for s in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]:
#     RationalSimpleDiscreteGaussian(sigma_sq=Fraction(1, s), size=N, rng=DASRandom())
# print(f"{N} DiscreteGaussian time: {time.time() - t0}")

calls = [
    (lambda size: RationalSimpleDiscreteLaplace(s=10, t=1 * 2, size=size, rng=DASRandom()), "DiscreteLaplace DASRandom"),
    (lambda size: RationalSimpleDiscreteGaussian(sigma_sq=Fraction(1, 20), size=size, rng=DASRandom()), "DiscreteGaussian DASRandom"),
    (lambda size: RationalSimpleDiscreteLaplace(s=10, t=1 * 2, size=size, rng=DASRDRandIntegers()), "DiscreteLaplace DASRandomLemire"),
    (lambda size: RationalSimpleDiscreteGaussian(sigma_sq=1 << 40, size=size, rng=DASRDRandIntegers()), "DiscreteGaussian DASRandomLemire"),

]

for call, name in calls:
    call(1)

N = 10000
for call, name in calls:
    t0 = time.time()
    call(N)
    print(f"{N} {name} time: {time.time() - t0}")

# t0 = time.time()
# FloatDiscreteGaussian(variance=float(Fraction(1, 20)), size=N, rng=DASRandom())
# print(f"{N} FloatDiscreteGaussian DASRandom time: {time.time() - t0}")

# t0 = time.time()
# rng = DASRandomBuffered()
# ints = [rng.integers(0, 100000000000) for _ in range(50 * N)]
# print(f"{N} DASRandomBuffered Lemir int time: {time.time() - t0}")

# t0 = time.time()
# sensitivity = 2
# epsilon = float(Fraction(20, 1))
# p = 1 - np.exp(-epsilon / float(sensitivity))
# rng = DASRandom()
# x = rng.geometric(p, size=N) - 1  # numpy geometrics start with 1
# y = rng.geometric(p, size=N) - 1
# protected_answer = x - y
# print(f"{N} OriginalGeometricMechanism DASRandom time: {time.time() - t0}")
