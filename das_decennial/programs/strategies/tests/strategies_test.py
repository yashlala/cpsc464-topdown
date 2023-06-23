from programs.strategies.strategies import QueryOrderingSelector, StrategySelector
from programs.schema.schemas.schemamaker import SchemaMaker
from das_constants import CC
import numpy as np
import pytest

levels = CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE, CC.GEOLEVEL_US


@pytest.mark.parametrize("ordering_name, ordering_maker", list(QueryOrderingSelector.query_orderings.items()))
def test_ordering_types(ordering_name, ordering_maker):
    ordering = ordering_maker.make(levels)
    first_ordering = ordering[levels[0]][CC.L2_QUERY_ORDERING]
    first_element = first_ordering[list(first_ordering.keys())[0]]
    if isinstance(first_element, dict):
        outer_pass = True
    elif isinstance(first_element, (tuple, list)):
        outer_pass = False
    else:
        raise ValueError(f"Ordering {ordering_name} has wrong syntax")
    for gl, gl_ord in ordering.items():
        for k1, v1 in gl_ord[CC.L2_QUERY_ORDERING].items():
            if outer_pass:
                assert isinstance(v1, dict), f"Ordering {ordering_name}, outer pass {k1} had {v1} instead of a dict inside L2 Query ordering for geolevel {gl}: {gl_ord}"
                for k2, v2 in v1.items():
                    assert isinstance(v2, (tuple, list)), f"Ordering {ordering_name}, outer pass {k1}, inner pass {k2} had {v2} instead of list/tuple inside L2 Query ordering for geolevel {gl}: {gl_ord}"
            else:
                assert isinstance(v1, (tuple, list)), f"Ordering {ordering_name}, pass {k1} had {v1} instead of list/tuple inside L2 Query ordering for geolevel {gl}: {gl_ord}"

        for k1, v1 in gl_ord[CC.ROUNDER_QUERY_ORDERING].items():
            if outer_pass:
                assert isinstance(v1, dict), f"Ordering {ordering_name}, outer pass {k1} had {v1} instead of a dict inside Rounder Query ordering for geolevel {gl}: {gl_ord}"
                for k2, v2 in v1.items():
                    assert isinstance(v2, (tuple, list)), f"Ordering {ordering_name}, outer pass {k1}, inner pass {k2} had {v2} instead of list/tuple inside Rounder Query ordering for geolevel {gl}: {gl_ord}"
            else:
                assert isinstance(v1, (tuple, list)), f"Ordering {ordering_name}, pass {k1} had {v1} instead of list/tuple inside Rounder Query ordering for geolevel {gl}: {gl_ord}"

        if gl_ord[CC.L2_CONSTRAIN_TO_QUERY_ORDERING] is None:
            continue

        for k1, v1 in gl_ord[CC.L2_CONSTRAIN_TO_QUERY_ORDERING].items():
            if outer_pass:
                assert isinstance(v1, dict), f"Ordering {ordering_name}, outer pass {k1} had {v1} instead of a dict inside Constrain-to L2 Query ordering for geolevel {gl}: {gl_ord}"
                for k2, v2 in v1.items():
                    assert isinstance(v2, (tuple, list)), f"Ordering {ordering_name}, outer pass {k1}, inner pass {k2} had {v2} instead of list/tuple inside Constrain-to L2 Query ordering for geolevel {gl}: {gl_ord}"
            else:
                assert isinstance(v1, (tuple, list)), f"Ordering {ordering_name}, pass {k1} had {v1} instead of list/tuple inside Constrain-to L2 Query ordering for geolevel {gl}: {gl_ord}"


@pytest.mark.parametrize("sname, strat", list(StrategySelector.strategies.items()))
def test_impact_gaps(sname, strat):
    try:
        schema = SchemaMaker.fromName(strat.schema)
    except AttributeError:
        raise AttributeError(f"Strategy {sname} doesn't have a schema attribute. Needed to check for impact gaps (to create queries from the schema)")

    try:
        strat_levels = strat.levels
    except AttributeError:
        strat_levels = levels
    s = strat().make(strat_levels)

    for level, qnames in s[CC.DPQUERIES].items():  # TODO: Doesn't test unit queries! (needs different schema for them)
        for qname in qnames:
            query = schema.getQuery(qname)
            # This is just the sum
            # impact = (np.ones(query.numAnswers()) @ np.abs(query.matrixRep()))  # factor of eps/sens doesn't matter here
            impact = np.abs(query.matrixRep()).sum(axis=0)
            # total_impact += impact * prop  # to do this, need to do composition, multiplying by proportion, like here,  only works for pure, epsilon-DP
            impmin, impmax = impact.min(), impact.max()

            if abs(impmin - impmax) > 1e-7:
                print(f"{qname} ~ Impact\n {'':50} Min: {impmin}, Max: {impmax}, All: {impact}")
                raise ValueError(f"There is an impact gap underutilizing parallel composition in query {qname}, geolevel {level}, in strategy {sname}")

            # Having both below is redundant, but for clarity and future flexibility including both
            if impmin != 1:
                raise ValueError(f"Some histogram cells are under-measured in query query {qname}, geolevel {level}, in strategy {sname}")
            if impmax != 1:
                raise ValueError(f"Some histogram cells are measured more than once in query {qname}, geolevel {level}, in strategy {sname}")


@pytest.mark.skip(reason="The new style of assigning direct budget allocations make them sum to the total geolevel budget, not 1")
@pytest.mark.parametrize("sname, strat", list(StrategySelector.strategies.items()))
def test_sum_to_1_within_level(sname, strat):
    # for sname, strat in StrategySelector.strategies.items():
    try:
        strat_levels = strat.levels
    except AttributeError:
        strat_levels = levels
    s = strat().make(strat_levels)
    for geolevel in strat_levels:
        assert sum(s[CC.QUERIESPROP][geolevel]) + sum(s[CC.UNITQUERIESPROP][geolevel]) == 1, f"Geolevel {geolevel} query proportions {s[CC.QUERIESPROP][geolevel]}, {s[CC.UNITQUERIESPROP][geolevel]} don't sum to 1"
