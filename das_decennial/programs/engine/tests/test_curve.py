import pytest
import mpmath

from programs.engine.curve import BaseGeoCurve


def get_plrv(mp_eps, bounded: bool, mp: mpmath):
    """ mp_eps must be an mpmath """
    one = mp.mpf(1)
    two = mp.mpf(2)
    plrv = mp.matrix([0] * 3)
    if bounded:
        tmp_low = one / (one + mp.exp(mp_eps / two))
        tmp_high = one / (one + mp.exp(-mp_eps / two))
        plrv[0] = tmp_low * tmp_low  # prob of -eps
        plrv[1] = 2 * tmp_low * tmp_high  # prob of medium
        plrv[2] = tmp_high * tmp_high  # prob of eps
    else:
        plrv[0] = one / (one + mp.exp(mp_eps))  # prob of -eps
        plrv[1] = mp.mpf(0)  # prob of 0
        plrv[2] = one / (one + mp.exp(-mp_eps))  # prob of eps
    return plrv


def embed_mass(q, denom, mass, mp: mpmath):
    # Note: parameter q is not used
    embedded_mass = mp.matrix([0] * (2 * denom + 1))
    embedded_mass[0] = mass[0]
    embedded_mass[denom] = mass[1]
    embedded_mass[denom*2] = mass[2]
    return embedded_mass


def convolve(a, b, mp: mpmath):
    conv = mp.matrix([0] * (a.rows + b.rows - 1))
    for i in range(conv.rows):
        conv[i] = sum(a[j] * b[i - j] for j in range(max(0, i - b.rows + 1), min(i + 1, a.rows)))
    return conv


@pytest.mark.parametrize("bounded", [True, False])
def test_base_geo_curve_cdf_1(bounded: bool) -> None:
    # just do one variable
    epsilon = 4.0
    geo_alloc = [1]
    query_alloc = [1]
    base_geo = BaseGeoCurve(geo_alloc, query_alloc, epsilon, bounded=bounded)
    mp = base_geo.mp
    # compute expected answer
    mp_eps = mp.mpf(epsilon)
    expected_cdf = mp.matrix([0, 0, 0])
    mass = get_plrv(mp_eps, bounded, mp)
    expected_cdf[0] = mass[0]
    expected_cdf[1] = mass[0] + mass[1]
    expected_cdf[2] = mp.mpf(1)
    for i in range(expected_cdf.rows):
        assert mp.almosteq(expected_cdf[i], base_geo.cdf[i])
        print("exp : ", expected_cdf[i])
        print("got : ", base_geo.cdf[i])
        print("mass: ", mass[i])


@pytest.mark.parametrize("bounded", [True, False])
def test_base_geo_curve_cdf_1b(bounded: bool) -> None:
    # just do one variable, unscaled allocations,
    # all that should change is that the array is larger
    epsilon = 4.0
    r = 2
    c = 3
    geo_alloc = [r]
    query_alloc = [c]
    base_geo = BaseGeoCurve(geo_alloc, query_alloc, epsilon, bounded=bounded)
    mp = base_geo.mp
    # compute expected answer
    mp_eps = mp.mpf(epsilon)
    mass = get_plrv(mp_eps, bounded, mp)
    embedded_mass = embed_mass(r*c, r*c, mass, mp)
    for i in range(embedded_mass.rows):
        assert mp.almosteq(mp.fsum(embedded_mass[:(i+1)]), base_geo.cdf[i])
        print("exp : ", mp.fsum(embedded_mass[:(i+1)]))
        print("got : ", base_geo.cdf[i])


@pytest.mark.parametrize("bounded", [True, False])
def test_base_geo_curve_cdf_2(bounded: bool) -> None:
    # just do 2 geo level and 2 queries per geolevel, unscaled allocations,
    # all that should change is that the array is larger
    epsilon = 4.0
    geo_alloc = [1, 2]
    query_alloc = [3, 4]
    allprod = [3, 4, 6, 8]  # geo_alloc outer product query_allow
    base_geo = BaseGeoCurve(geo_alloc, query_alloc, epsilon, bounded=bounded)
    mp = base_geo.mp
    masses = []
    for q in allprod:
        mp_eps = mp.mpf(epsilon) * mp.mpf(q) / mp.mpf(sum(allprod))
        mass = get_plrv(mp_eps, bounded, mp)
        embedded_mass = embed_mass(q, q, mass, mp)
        masses.append(embedded_mass)
    conv = convolve(masses[0], masses[1], mp)
    for nextmass in masses[2:]:
        conv = convolve(conv, nextmass, mp)
    cdf = mp.matrix([0] * conv.rows)
    assert cdf.rows == (sum(allprod) * 2 + 1), f"rows {cdf.rows}"
    for i in range(cdf.rows):
        cdf[i] = mp.fsum(conv[:(i+1)])
    for i in range(cdf.rows):
        assert mp.almosteq(cdf[i], base_geo.cdf[i])
        print("exp : ", cdf[i])
        print("got : ", base_geo.cdf[i])
