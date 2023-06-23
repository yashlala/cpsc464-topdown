import programs.engine.curve as curve
import numpy as np
import scipy.optimize as so
import pytest


class PrivacyCalcDoubleProgram:

    @staticmethod
    def g_prime(alpha, rho, epsilon):
        out = (2 * alpha - 1) * rho - epsilon + np.log(1 - 1.0 / alpha)
        return out

    @staticmethod
    def gFunc(alpha, rho, eps):
        out = (alpha - 1) * (alpha * rho - eps) + (alpha - 1) * np.log(1 - 1.0 / alpha) - np.log(alpha)
        return out

    def findDelta(self, eps, rho):
        # returns the value delta for a given eps, rho
        lower_interval = (eps + rho) / (2.0 * rho)
        upper_interval = max((eps + rho + 1.0) / (2.0 * rho), 2.0)

        low = lower_interval
        high = upper_interval

        # find alpha such that g_prime is 0
        opt_alpha = so.bisect(f=self.g_prime, args=(rho, eps), a=low, b=high, maxiter=1000)
        alpha: float = opt_alpha
        delta = np.exp(self.gFunc(alpha, rho, eps))
        return delta

    def findRho(self, eps, delta, low_rho=1e-4, high_rho=1., tol=1e-20):
        # search for rho that satisfies eps, delta

        low = low_rho
        high = high_rho

        while True:
            mid = (low + high) / 2
            value = self.findDelta(eps, rho=mid)
            if abs(value - delta) < tol:
                return mid

            # if value -delta >0 too big a delta, we want a smaller rho so lower high to mid
            # if value - delta <0 too small a delta, raise low to mid
            if (value - delta) > 0:
                high = mid
            else:
                low = mid

    @staticmethod
    def findPhi(rho, geo_prop, query_prop):
        # given a rho value, find phi
        thesum = 0
        for g in geo_prop:
            for q in query_prop:
                thesum += g * q

        phi = np.sqrt(thesum / rho)
        return phi

    def findPhidirect(self, eps, delta, geo_prop, query_prop, low_rho=1e-6, high_rho=0.5, tol=1e-20):
        # put the steps together for an all-in-one function
        rho = self.findRho(eps, delta, low_rho, high_rho, tol)
        phi = self.findPhi(rho, geo_prop, query_prop)
        out = {"phi": phi, "rho": rho}
        return out


@pytest.mark.parametrize("eps", (1., 2.4, 7.,))
@pytest.mark.parametrize("delta", (1e-10, 1e-9))
@pytest.mark.parametrize("geo_prop", (
        (0.5, 0.25, 0.25),
        (0.2, 0.2, 0.2, 0.2, 0.2),
        (0.1, 0.1, 0.1, 0.2, 0.5)
))
@pytest.mark.parametrize("query_prop", (
        (0.25, 0.25, 0.5),
        (0.3, 0.25, 0.25, 0.2),
        (0.3, 0.2, 0.2, 0.2, 0.1)
))
def test_rho_and_eps_calculator(eps, delta, geo_prop, query_prop):
    # from curvey.py
    geo_alloc_dict = {i: (gprop, query_prop) for i, gprop in enumerate(geo_prop)}  # NOTE: queries are the same on all levels
    test = curve.zCDPEpsDeltaCurve(geo_alloc_dict)
    phi1 = test.get_scale(eps, delta, zcdp=True)
    rho1 = test.get_rho(nscale=phi1, bounded=True)

    results2 = PrivacyCalcDoubleProgram().findPhidirect(eps, delta, geo_prop, query_prop, low_rho=1e-6, high_rho=1.0)
    assert np.abs(phi1 - results2['phi']) < 1e-7
    assert np.abs(rho1 - results2['rho']) < 1e-7

    # results1 = {"phi": phi1, "rho": rho1}
    # print(results1)
    # print(results2)
