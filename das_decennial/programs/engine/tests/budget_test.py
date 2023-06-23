import os, sys
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname( os.path.dirname(__file__)))))

import pytest
import warnings
from fractions import Fraction
import numpy as np

from programs.schema.schemas.schemamaker import SchemaMaker
from programs.engine.curve import zCDPEpsDeltaCurve

try:
    import programs.engine.budget as bdg
    HAVE_SPARK = True
except ImportError as e:
    HAVE_SPARK = False
    import programs.engine.budget as bdg


def test_assert_sum_to() -> None:
    if not HAVE_SPARK:
        warnings.warn("SPARK not available")
        return
    with pytest.raises(AssertionError) as err:
        bdg.assertSumTo((0.1, 0.1), msg="Test 1")
    assert "Test 1 values (0.1, 0.1) sum to 0.2 instead of 1.0" in str(err.value)

    bdg.assertSumTo((.3, .2, .5))


def test_getAttrQueryProps() -> None:
    attr_query_props_exp = {
        "hhgq": {
            "hhgq": {"Tract": Fraction(2, 10), "Block_Group": Fraction(2, 10), "Block": Fraction(3, 10)},
            "detailed": {"Tract": Fraction(3, 10), "Block_Group": Fraction(3, 10), "Block": Fraction(1, 10)}
        },
        "votingage": {
            "detailed": {"Tract": Fraction(3, 10), "Block_Group": Fraction(3, 10), "Block": Fraction(1, 10)},
            "votingage": {"Tract": Fraction(3, 10), "Block_Group": Fraction(4, 10), "Block": Fraction(1, 10)}
        }}
    # attr_query_props_exp_flipped = {
    #     'hhgq': {
    #         'Tract':       {'hhgq': Fraction(2, 10), 'detailed': Fraction(3, 10)},
    #         'Block_Group': {'hhgq': Fraction(2, 10), 'detailed': Fraction(3, 10)},
    #         'Block':       {'hhgq': Fraction(3, 10), 'detailed': Fraction(1, 10)}
    #     },
    #     'votingage': {
    #         'Tract':       {'detailed': Fraction(3, 10), 'votingage': Fraction(3, 10)},
    #         'Block_Group': {'detailed': Fraction(3, 10), 'votingage': Fraction(2, 5)},
    #         'Block':       {'detailed': Fraction(1, 10), 'votingage': Fraction(1, 10)}
    #     }
    # }
    geolevel_prop_budgets_dict = {"Tract": Fraction(3, 10), "Block_Group": Fraction(1, 5), "Block": Fraction(1, 2)}
    dp_query_prop = {"Tract": (Fraction(2, 10), Fraction(2, 10), Fraction(3, 10), Fraction(3, 10)),
                     "Block_Group": (Fraction(1, 10), Fraction(2, 10), Fraction(4, 10), Fraction(3, 10)),
                     "Block": (Fraction(5, 10), Fraction(3, 10), Fraction(1, 10), Fraction(1, 10))}
    query_names = ("cenrace", "hhgq", "votingage", "detailed",)
    levels = list(reversed(list(geolevel_prop_budgets_dict)))
    dimnames = ["hhgq", "votingage"]

    schema = SchemaMaker.fromName("PL94")

    def query_iter(gl):
        for qname, qprop in zip(query_names, dp_query_prop[gl]):
            yield schema.getQuery(qname), qprop

    attr_query_props = bdg.Budget.getAttrQueryProps(levels, dimnames, query_iter)
    for dim in dimnames:
        for qname, qgl_dict in attr_query_props_exp[dim].items():
            for gl, prop in qgl_dict.items():
                #assert prop == attr_query_props[dim][qname][gl]
                assert prop == attr_query_props[dim][gl][qname]


def test_getPerAttrEpsilonFromProportionsFunctions() -> None:
    attr_query_props = {
        'hhgq': {
            'Tract':       {'hhgq': Fraction(2, 10), 'detailed': Fraction(3, 10)},
            'Block_Group': {'hhgq': Fraction(2, 10), 'detailed': Fraction(3, 10)},
            'Block':       {'hhgq': Fraction(3, 10), 'detailed': Fraction(1, 10)}
        },
        'votingage': {
            'Tract':       {'detailed': Fraction(3, 10), 'votingage': Fraction(3, 10)},
            'Block_Group': {'detailed': Fraction(3, 10), 'votingage': Fraction(2, 5)},
            'Block':       {'detailed': Fraction(1, 10), 'votingage': Fraction(1, 10)}
        }
    }
    geolevel_prop_budgets_dict = {"Tract": Fraction(3, 10), "Block_Group": Fraction(1, 5), "Block": Fraction(1, 2)}
    dp_query_prop = {"Tract":       (Fraction(2, 10), Fraction(2, 10), Fraction(3, 10), Fraction(3, 10)),
                     "Block_Group": (Fraction(1, 10), Fraction(2, 10), Fraction(4, 10), Fraction(3, 10)),
                     "Block":       (Fraction(5, 10), Fraction(3, 10), Fraction(1, 10), Fraction(1, 10))}
    levels = list(reversed(list(geolevel_prop_budgets_dict)))
    total_budget = 17
    expected_per_attr_eps = dict((("hhgq", 7.65), ("votingage", 7.14)))
    expected_per_geolevel_attrs = dict((("Tract", 11.9), ("Block_Group", 8.5,)))

    def getEpsFromGeoAlloc(geo_all_dict):
        return total_budget * sum(gprop * sum(qprops)  for gprop, qprops in geo_all_dict.values())

    per_attr_epsilons, per_geolevel_epsilons = bdg.Budget.getPerAttrEpsilonFromProportions(attr_query_props, getEpsFromGeoAlloc, levels, geolevel_prop_budgets_dict, dp_query_prop)
    for attr_name, attr_eps in expected_per_attr_eps.items():
        assert np.isclose(attr_eps, float(per_attr_epsilons[attr_name]), atol=1e-6)
    for geolevel, geolevel_eps in expected_per_geolevel_attrs.items():
        assert np.isclose(geolevel_eps, float(per_geolevel_epsilons[geolevel]), atol=1e-6)

    delta = 1e-10
    global_scale = .1
    # order is: "total", "hhgq", "votingage", "detailed":

    def getEpsFromGeoAlloc(geo_all_dict) -> Fraction:
        return Fraction(zCDPEpsDeltaCurve(geo_all_dict).get_epsilon(float(delta), global_scale, bounded=True, tol=1e-7,zcdp=True))

    per_attr_epsilons, per_geolevel_epsilons = bdg.Budget.getPerAttrEpsilonFromProportions(attr_query_props, getEpsFromGeoAlloc, levels,
                                                                                           geolevel_prop_budgets_dict, dp_query_prop)
    expected_per_attr_eps = dict((("hhgq", 107.74557956), ("votingage", 102.58843891)))
    expected_per_geolevel_attrs = dict((("Tract", 148.49113099), ("Block_Group", 116.18815593,)))
    for attr_name, attr_eps in expected_per_attr_eps.items():
        assert np.isclose(attr_eps, float(per_attr_epsilons[attr_name]), atol=1e-6)
    for attr_name, geolevel_eps in expected_per_geolevel_attrs.items():
        assert np.isclose(geolevel_eps, float(per_geolevel_epsilons[attr_name]), atol=1e-6)

if __name__ == "__main__":
    test_getPerAttrEpsilonFromProportionsFunctions()
