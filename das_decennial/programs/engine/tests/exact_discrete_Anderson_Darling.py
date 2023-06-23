import numpy as np
from scipy.integrate import quad
from scipy.stats import norm

def cvm_test(x, cdf_fun, elements_in_domain):
    # This file implements an Anderson-Darling test for an arbitrary discrete
    # distribution function. Note that the Anderson-Darling test statistic has a
    # distribution that depends on the null-hypothesis distribution function in
    # the discrete case, so we cannot use the standard critical value tables.
    # for more information, see "Nonparametric Goodness-of-Fit Tests for
    # Discrete Null Distributions" by Arnold and Emerson (2017), which is the
    # basis for this implementation.

    # This function returns the p-value of the test. The inputs are as follows.
    # x is a dataset, cdf_fun is the (vectorized) CDF function under the null-
    # hypothesis, elements_in_domain is a one dimensional numpy array of the
    # set of elements in the domain of cdf_fun. (Note that this function
    # requires truncating unbounded domains.)

    stat, lambs = compute_test_stat(x, cdf_fun, elements_in_domain)
    pval = compute_p_value(stat, lambs)

    return pval


def theta(stat, u, lambs) -> float:
    res = 0.
    for lamb in lambs:
        res += 0.5 * np.arctan(lamb * u)
    return res - 0.5 * stat * u


def rho(u, lambs) -> float:
    res = 0.
    for lamb in lambs:
        res += np.log(1. + lamb ** 2 * u ** 2)
    return np.exp(res/4.)


def integrand(stat, u, lambs) -> float:
    return np.sin(theta(stat, u, lambs))/(u * rho(u, lambs))


def compute_p_value(stat, lambs) -> float:
    integral = quad(lambda u: integrand(stat, u, lambs), 0., np.inf, epsabs=5e-4, limit=5000, maxp1=5000, limlst=5000)
    if integral[1] > 1e-3:
        print(f"Error in numerical integration is of the order {integral[1]}; p-values may be incorrect.")
    return np.max([0., 0.5 + integral[0]/np.pi])


def compute_test_stat(x, cdf_fun, elements_in_domain):
    n = len(x)
    e = np.diff(np.concatenate(([0.], n * cdf_fun(elements_in_domain))))
    obs = np.zeros(len(elements_in_domain))
    for i, elem in enumerate(elements_in_domain):
      obs[i] = len(np.where(x == elem)[0])

    S = np.cumsum(obs)
    T = np.cumsum(e)
    H = T/n
    p = e/n
    # why not (p[:-1] + p[1:])/2?
    t = (p + np.concatenate((p[1:], [p[0]]))) / 2.
    Z = S - T
    p = p.reshape((len(p), 1))

    S0 = np.diag(p.flatten()) - p.dot(p.T)
    A = np.tril(np.ones(S.shape))
    E = np.diag(t)

    weights = np.concatenate((1./(H[:-1] * (1. - H[:-1])), [0.]))
    K = np.diag(weights)
    Sy = A.dot(S0).dot(A.T)

    message = f"{weights}, {Sy}, {e}"

    assert np.all(weights >= 0.) and np.all(weights <= 1e16), message

    M = E.dot(K)
    lambs = np.real(np.linalg.eig(M.dot(Sy))[0])

    stat = np.sum((Z ** 2. * t * weights)[:-1]) / n
    return stat, lambs


def cdf_eval(ks, bound, sigma):
    """Returns the approximate values of the discrete Gaussian cdf evaluated at the array ks."""
    normcst = 2. * sum([norm.pdf(n, loc=0., scale=sigma) for n in range(-2 * bound, 0)])
    normcst += norm.pdf(0., loc=0., scale=sigma)
    return np.array([sum([norm.pdf(n, loc=0., scale=sigma)/normcst for n in range(-bound, k + 1)]) for k in ks])


if __name__ == "__main__":
    rs = np.random.RandomState(123)

    obs = np.zeros(100)
    for k in range(100):
        x = rs.binomial(10, .75, 100)
        y_data = rs.binomial(10, .75, 10000)
        t = cvm_test(x, lambda z: np.array([len(np.where(y_data <= zi)[0]) / len(y_data) for zi in z]), np.unique(y_data))
        obs[k] = t

    print("Testing the level - these quantiles should be about [.1, .3, .5, .7, .9]:")
    print(np.quantile(obs, [.1, .3, .5, .7, .9]))

    obs = np.zeros(100)
    for k in range(100):
        x = rs.binomial(10, .75, 100)
        y_data = rs.binomial(10, .65, 10000)
        t = cvm_test(x, lambda z: np.array([len(np.where(y_data <= zi)[0]) / len(y_data) for zi in z]), np.unique(y_data))
        obs[k] = t

    print("Testing the power - all quantiles should be near zero:")
    print(np.quantile(obs, [.1, .3, .5, .7, .9]))
