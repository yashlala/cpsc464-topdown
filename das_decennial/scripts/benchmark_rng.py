import numpy as np
from dprng import DPRNGBitGenerator, discrete_gaussian, random_bytes
import randomgen
import time
import threading


LOW = 0
HIGH=1000
COUNT=10000
MaxIterations=5




# Indexes
STATS_ST_NPMEAN = 0
STATS_ST_NPMEDIAN = 1
STATS_ST_TIME = 2
def rnd_run(generator, low, high, count):
    start = time.time()
    uniform = generator.integers(low, high, count)
    end = time.time()
    return [np.mean(uniform), np.median(uniform), end - start]

def rnd_run_multithreaded(generator, low, high, count, threads):
    # create all threads before starting time
    thread_list = []
    for i in range(0, threads):
        thread_list.append(threading.Thread(target = rnd_run, args=(generator, low, high, count)))

    start = time.time()
    for i in range(0, threads):
        thread_list[i].start()

    for i in range(0,threads):
        thread_list[i].join()
    end = time.time()

    return end-start

if __name__ == "__main__":
    stats_single_thread_np = []
    stats_single_thread_dp = []
    stats_multi_thread_np = []
    stats_multi_thread_dp = []

    for i in range(0, MaxIterations):
        print(f"[ST][RDRAND] [{COUNT} samples]  [{i+1}/{MaxIterations}] ", end='', flush=True)
        stats_single_thread_np.append(rnd_run(np.random.Generator(randomgen.RDRAND()), LOW, HIGH, COUNT))
        print("(", stats_single_thread_np[i][STATS_ST_TIME], " seconds)")
        print(f"[ST][DPRND]  [{COUNT} samples]  [{i+1}/{MaxIterations}] ", end='', flush=True)
        stats_single_thread_dp.append(rnd_run(np.random.Generator(DPRNGBitGenerator()), LOW, HIGH, COUNT))
        print("(", stats_single_thread_dp[i][STATS_ST_TIME], " seconds)")
        print(f"[MT][RDRAND] [{COUNT} samples]  [{i+1}/{MaxIterations}] ", end='', flush=True)
        stats_multi_thread_np.append(rnd_run_multithreaded(np.random.Generator(randomgen.RDRAND()), LOW, HIGH, COUNT, 96))
        print("(", stats_multi_thread_np[i], " seconds)")
        print(f"[MT][DPRND]  [{COUNT} samples]  [{i+1}/{MaxIterations}] ", end='', flush=True)
        stats_multi_thread_dp.append(rnd_run_multithreaded(np.random.Generator(DPRNGBitGenerator()), LOW, HIGH, COUNT, 96))
        print("(", stats_multi_thread_dp[i], " seconds)")
        COUNT = COUNT*10    # increment count by multiple of 10 each iteration

    print("All Results")
    print("===========")
    print("[Single Thread][RDRAND] (average, median, time) = ", stats_single_thread_np)
    print("[Single Thread][DPRND]  (average, median, time) = ", stats_single_thread_dp)
    print("[Multi Thread] [RDRAND] (time) = ", stats_multi_thread_np)
    print("[Multi Thread] [DPRND]  (time) = ", stats_multi_thread_dp)

# The following is the output generated by this program on 9-28-2021 for documentation purposes.
# impol-ITE-MASTER:hadoop@ip-{IP_NAME}$ python benchmark_rng.py
# [ST][RDRAND] [10000 samples]  [1/5] ( 0.0021347999572753906  seconds)
# [ST][DPRND]  [10000 samples]  [1/5] ( 0.00057220458984375  seconds)
# [MT][RDRAND] [10000 samples]  [1/5] ( 0.24912810325622559  seconds)
# [MT][DPRND]  [10000 samples]  [1/5] ( 0.06481194496154785  seconds)
# [ST][RDRAND] [100000 samples]  [2/5] ( 0.021552324295043945  seconds)
# [ST][DPRND]  [100000 samples]  [2/5] ( 0.0021698474884033203  seconds)
# [MT][RDRAND] [100000 samples]  [2/5] ( 2.061887264251709  seconds)
# [MT][DPRND]  [100000 samples]  [2/5] ( 0.2231125831604004  seconds)
# [ST][RDRAND] [1000000 samples]  [3/5] ( 0.20631718635559082  seconds)
# [ST][DPRND]  [1000000 samples]  [3/5] ( 0.018433332443237305  seconds)
# [MT][RDRAND] [1000000 samples]  [3/5] ( 20.32515788078308  seconds)
# [MT][DPRND]  [1000000 samples]  [3/5] ( 1.7944543361663818  seconds)
# [ST][RDRAND] [10000000 samples]  [4/5] ( 2.025177240371704  seconds)
# [ST][DPRND]  [10000000 samples]  [4/5] ( 0.19433045387268066  seconds)
# [MT][RDRAND] [10000000 samples]  [4/5] ( 199.49678087234497  seconds)
# [MT][DPRND]  [10000000 samples]  [4/5] ( 18.68354868888855  seconds)
# [ST][RDRAND] [100000000 samples]  [5/5] ( 21.10179352760315  seconds)
# [ST][DPRND]  [100000000 samples]  [5/5] ( 1.9022095203399658  seconds)
# [MT][RDRAND] [100000000 samples]  [5/5] ( 1977.8356714248657  seconds)
# [MT][DPRND]  [100000000 samples]  [5/5] ( 182.5231113433838  seconds)
# All Results
# ===========
# [Single Thread][RDRAND] (average, median, time) =  [[500.1325, 500.0, 0.0021347999572753906], [501.11827, 501.0, 0.021552324295043945], [499.37289, 499.0, 0.20631718635559082], [499.4239129, 499.0, 2.025177240371704], [499.47621598, 499.0, 21.10179352760315]]
# [Single Thread][DPRND]  (average, median, time) =  [[498.1772, 493.0, 0.00057220458984375], [498.80983, 499.0, 0.0021698474884033203], [499.659398, 499.0, 0.018433332443237305], [499.5903683, 500.0, 0.19433045387268066], [499.51555586, 500.0, 1.9022095203399658]]
# [Multi Thread] [RDRAND] (time) =  [0.24912810325622559, 2.061887264251709, 20.32515788078308, 199.49678087234497, 1977.8356714248657]
# [Multi Thread] [DPRND]  (time) =  [0.06481194496154785, 0.2231125831604004, 1.7944543361663818, 18.68354868888855, 182.5231113433838]


# Analysis
# Percentage Decrease = [ (Starting Value - Final Value) / |Starting Value| ] × 100
#
#
#               |  10000  |  100000  |  1000000  |  10000000 |  100000000
# Single Thread |  73.20  |  89.93   |  91.07    |  90.40    |  90.99
# Multi Thread  |  73.98  |  89.18   |  91.17    |  90.63    |  90.77
#
# ** Depending on the number of random numbers being grabbed, the percent decrease in time is approximately 90%
