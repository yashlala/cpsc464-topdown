import pytest
from functools import reduce
from operator import add
import numpy as np
from fractions import Fraction
import programs.nodes.nodes as nodes
import programs.nodes.manipulate_nodes as mn
import programs.sparse as sparse
import programs.constraints.Constraints_PL94 as cPL94
from programs.invariants.invariantsmaker import InvariantsMaker
from programs.nodes.nodes import GeounitNode
from programs.schema.schemas.schemamaker import SchemaMaker
from das_utils import table2hists
from programs.queries.querybase import QueryFactory, MultiHistQuery, StubQuery
from programs.queries.constraints_dpqueries import DPquery, Constraint, StackedConstraint, StackedDPquery
from programs.engine.primitives import RationalGeometricMechanism as GeometricMechanism
from programs.das_setup import DASDecennialSetup
from das_constants import CC
from exceptions import IncompatibleAddendsError


d1 = np.array([   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 0, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 2],
                [1, 1, 0, 15, 3]
            ])


d2 = np.array([   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 1, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 4],
                [1, 1, 0, 15, 3]
            ])

geocodeDict = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State', 1: 'US'}
invn = ('tot', 'va', 'gqhh_vect', 'gqhh_tot', 'gq_vect')
consn = ("total", "hhgq_total_lb", "hhgq_total_ub", "voting_age", "hhgq_va_ub", "hhgq_va_lb", "nurse_nva_0")


def convertPL94(d):
    return table2hists(d, SchemaMaker.fromName(CC.SCHEMA_PL94), CC.ATTR_HHGQ)



def makeInvs(ph, invnames):
    return InvariantsMaker.make(schema=CC.SCHEMA_PL94, raw=ph[0], raw_housing=ph[1], invariant_names=invnames)


def makeCons(ph, consnames, inv_dict):
    return cPL94.ConstraintsCreator(hist_shape=(ph[0].shape, ph[1].shape),
                                    invariants=inv_dict,
                                    constraint_names=consnames).calculateConstraints().constraints_dict


def makeNode(d, geocode, geocode_dict, addsyn=False, dpq_make=False, querydict=None, consn=consn, invn=invn):
    ph = convertPL94(d)
    syn = ph[0].toDense() + np.ones(np.prod(ph[0].shape)).reshape(ph[0].shape) if addsyn else None
    dpqdict = {CC.DETAILED: DPquery(QueryFactory.makeTabularGroupQuery(array_dims=ph[0].shape), GeometricMechanism(Fraction(1,10), 2, ph[0].toDense()))} if dpq_make else {}
    if querydict:
        dpqdict.update({
                name: DPquery(query, GeometricMechanism(Fraction(1,10), 2, query.answer(ph[0].toDense())))
                for name, query in SchemaMaker.fromName(CC.SCHEMA_PL94).getQueries(querydict).items()
            })
    inv_dict = makeInvs(ph, invn)
    return GeounitNode(geocode, raw=ph[0], raw_housing=ph[1], syn=syn, cons=makeCons(ph, consn, inv_dict), invar=inv_dict,
                       geocode_dict=geocode_dict, dp_queries=dpqdict)

def makeEmptyNode(geocode):
    return makeNode(np.zeros(d1.shape), geocode=geocode, geocode_dict=geocodeDict)


@pytest.fixture
def n():
    geocode_dict = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County'}
    histogram = sparse.multiSparse(np.array([[[[5, 0], [0, 4]], [[5, 0], [0, 4]]], [[[5, 0], [0, 4]], [[5, 0], [0, 4]]]]))
    housing_hist = sparse.multiSparse((np.array([0, 1, 1, 0, 0, 0, 7, 2])))

    return nodes.GeounitNode(geocode='123456789abcdefg', geocode_dict=geocode_dict, raw=histogram, raw_housing=housing_hist)


class TestNodes:

    def test_nodes_slotsFrozen(self, n):
        with pytest.raises(AttributeError):
            n.newAttribute = "This shouldn't work."

    @staticmethod
    def test_node_init():

        n = nodes.GeounitNode(geocode='123456789abcdefg', geocode_dict={16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County'})
        assert n.parentGeocode == '123456789abc'
        assert n.geolevel == 'Block'

        n = nodes.GeounitNode(geocode='12', geocode_dict={16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State'})
        assert n.parentGeocode == ''
        assert n.geolevel == 'State'

    @pytest.mark.parametrize("add_querydict", [['cenrace'], ['votingage * hhgq', 'hispanic'], False])
    @pytest.mark.parametrize("dpq_make", [True, False])
    @pytest.mark.parametrize("addsyn", [True, False])
    def test_equal(self, add_querydict, dpq_make, addsyn):
        geocode_dict = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State', 1: 'US'}
        n1 = makeNode(d1, '12', geocode_dict=geocode_dict, addsyn=addsyn, dpq_make=dpq_make, querydict=add_querydict)
        n2 = makeNode(d1, '12', geocode_dict=geocode_dict, addsyn=addsyn, dpq_make=dpq_make, querydict=add_querydict)
        n1.dp_queries = n2.dp_queries
        assert n1 == n2

    @pytest.mark.parametrize("add_querydict", [['cenrace'], ['votingage * hhgq', 'hispanic'], False])
    @pytest.mark.parametrize("dpq_make", [True, False])
    @pytest.mark.parametrize("addsyn", [True, False])
    @pytest.mark.parametrize("neq_attr", ["geocode_dict", "histogram", "dpq", "geocode"])
    def test_not_equal(self, add_querydict, dpq_make, addsyn, neq_attr):
        geocode_dicts = [
            {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State', 1: 'US'},
            {16: 'Block', 12: 'Block_Group', 10: 'Tract', 5: 'County', 2: 'State', 1: 'US'}
        ]
        geocodes = ['12', '34']
        ds = [d1, d2]
        if neq_attr == 'geocode_dict':
            geocode_dict = geocode_dicts[1]
            d = ds[0]
            geocode = geocodes[0]
        elif neq_attr == 'histogram':
            d = ds[1]
            geocode = geocodes[0]
            geocode_dict = geocode_dicts[0]
        elif neq_attr == 'geocode':
            geocode = geocodes[1]
            d = ds[0]
            geocode_dict = geocode_dicts[0]
        else:
            geocode = geocodes[0]
            d = ds[0]
            geocode_dict = geocode_dicts[0]

        n1 = makeNode(d1, geocodes[0], geocode_dict=geocode_dicts[0], addsyn=addsyn, dpq_make=dpq_make, querydict=add_querydict)
        n2 = makeNode(d, geocode, geocode_dict=geocode_dict, addsyn=addsyn, dpq_make=dpq_make, querydict=add_querydict)
        if neq_attr == 'dpq':
            if n1.dp_queries:
                with pytest.raises(AssertionError):
                    assert n1 == n2
        else:
            n1.dp_queries = n2.dp_queries
            with pytest.raises(AssertionError):
                assert n1 == n2

    @pytest.mark.parametrize("add_querydict", [['cenrace'], ['votingage * hhgq', 'hispanic'], False])
    @pytest.mark.parametrize("dpq_make", [True, False])
    def test_toDict(self, add_querydict, dpq_make):
        geocode_dict = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State', 1: 'US'}
        n1 = makeNode(d1, '12', geocode_dict=geocode_dict, addsyn=False, dpq_make=dpq_make, querydict=add_querydict)
        ndict1 = n1.toDict(keep_attrs=n1.__slots__)
        n2 = GeounitNode.fromDict(ndict1)
        assert n1 == n2

    @staticmethod
    def test_wconstraints():
        geocode_dict = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County'}
        histogram, housing_hist = table2hists(np.array(
                [   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                    # each row is a person
                    [0, 1, 1, 20, 0],
                    [1, 0, 0, 1, 1],
                    [3, 1, 0, 10, 2],
                    [3, 0, 0, 15, 2],
                    [1, 1, 0, 15, 3]
                ]), SchemaMaker.fromName(CC.SCHEMA_PL94), CC.ATTR_HHGQ)
        inv_dict = InvariantsMaker.make(schema=CC.SCHEMA_PL94, raw=histogram, raw_housing=housing_hist, invariant_names=("tot", "gqhh_vect", "gqhh_tot", "va"))
        con_dict = cPL94.ConstraintsCreator(hist_shape=(histogram.shape, housing_hist.shape), invariants=inv_dict, constraint_names=("total",))\
                .calculateConstraints().constraints_dict

        n1 = nodes.GeounitNode(geocode='123456789abcdefg',
                               geocode_dict=geocode_dict,
                               raw=histogram,
                               raw_housing=housing_hist,
                               cons=con_dict,
                               invar=inv_dict)

        n2 = nodes.GeounitNode(geocode='123456789abcdefg', geocode_dict=geocode_dict, raw=histogram, raw_housing=housing_hist, cons=con_dict,
                               invar=inv_dict)
        assert n1 == n2

    def test_repr(self, n):
        print(n)

    def test_deleteTrueArray(self, n):
        assert n.raw is not None
        assert n.raw_housing is not None
        n.deleteTrueArray()
        assert n.raw is None
        assert n.raw_housing is None

    def test_checkTrueArrayIsDeleted(self, n):
        with pytest.raises(RuntimeError):
            n.checkTrueArrayIsDeleted()

    @staticmethod
    def test_shiftGeocodesUp():
        n = nodes.GeounitNode(geocode='123456789abcdefg', geocode_dict={16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State',
                                                                        0: 'US'})
        n.shiftGeocodesUp()
        assert n.geocode == '123456789abc'
        assert n.geolevel == 'Block_Group'
        n.shiftGeocodesUp()
        assert n.geocode == '123456789ab'
        assert n.geolevel == 'Tract'
        n.shiftGeocodesUp()
        assert n.geocode == '12345'
        assert n.geolevel == 'County'
        n.shiftGeocodesUp()
        assert n.geocode == '12'
        assert n.geolevel == 'State'
        n.shiftGeocodesUp()
        assert n.geocode == ''
        assert n.geolevel == 'US'

    @pytest.mark.parametrize("invs1, invs2", [
        (makeInvs(convertPL94(d1), invn), makeInvs(convertPL94(d2), invn)),
        ({'a': np.array(range(12)).reshape((2, 2, 3))}, {'a': np.array(range(12, 24)).reshape((2, 2, 3))}),
        ({'a': 0}, {'a': 1}),
        ({'a': np.array(range(2)).reshape((2, 1))}, {'a': np.array(range(10, 12)).reshape((2, 1))}),
        ({'a': np.array(range(2)).reshape((1, 2))}, {'a': np.array(range(10, 12)).reshape((1, 2))})
    ])
    def test_addInvariants(self, invs1, invs2):
        n1 = makeEmptyNode('123456789017')
        n2 = makeEmptyNode('123456789013')
        n1.invar = invs1
        n2.invar = invs2

        sum_inv = GeounitNode.addInvariants(n1, n2)
        assert set(sum_inv.keys()) == set(invs1.keys())
        assert set(sum_inv.keys()) == set(invs2.keys())
        for invname in sum_inv.keys():
            assert np.array_equal(invs1[invname] + invs2[invname], sum_inv[invname])

    @pytest.mark.parametrize("test_input,errmsg", [
        # test_input: two invariant dicts
        # errmsg: Error message in response to trying to add those two invariant dicts

        # Different key sets
        (({'a': 1, 'b': 2, 'c': 3}, {'a': 3, 'b': 4}), "Invariant dicts cannot be added: addends have different key set: {"),
        (({'a': 1, 'b': 2, 'c': 3}, {'a': 3, 'b': 4, 'd': 5}), "Invariant dicts cannot be added: addends have different key set: {"),
        # Different shapes
        (({'a': np.array([1, 2])}, {'a': np.array([[1], [2]])}), "Invariant 'a' cannot be added: addends have different shapes: (2,) != (2, 1)"),
    ])
    def test_addInvariantsErrorsRaising(self, test_input, errmsg):
        n1 = makeEmptyNode('123456789017')
        n2 = makeEmptyNode('123456789013')
        n1.invar, n2.invar = test_input

        with pytest.raises(IncompatibleAddendsError) as err:
            GeounitNode.addInvariants(n1, n2)
        assert errmsg in str(err.value)
        # assert err_msg in caplog.text

    @pytest.mark.parametrize("add_querydict", [['cenrace'], ['votingage * hhgq', 'hispanic'], False])
    @pytest.mark.parametrize("add_dpqueries", [True, False])
    @pytest.mark.parametrize("dpq_make", [True, False])
    @pytest.mark.parametrize("ic", [True, False])
    @pytest.mark.parametrize("addsyn", [True, False])
    def test_addGeounitNodes(self, addsyn, ic, dpq_make, add_dpqueries, add_querydict):
        geocode_dict = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State', 1: 'US'}
        node1 = makeNode(d1, '12', geocode_dict=geocode_dict, addsyn=addsyn, dpq_make=dpq_make, querydict=add_querydict)
        node2 = makeNode(d2, '12', geocode_dict=geocode_dict, addsyn=addsyn, dpq_make=dpq_make, querydict=add_querydict)
        node_sum = node1.addInReduce(node2, inv_con=ic, add_dpqueries=add_dpqueries)
        assert node_sum.raw == node1.raw + node2.raw
        if addsyn:
            assert np.array_equal(node_sum.syn, node1.syn + node2.syn)
        else:
            assert node_sum.syn is None

        if ic:
            # Check if constraints are summed
            for cons_name in set(consn):
                assert node_sum.cons[cons_name] == (node1.cons + node2.cons)[cons_name]

            # Check more in depth
            for cons_name in set(consn):
                sign = node_sum.cons[cons_name].sign
                assert sign == node1.cons[cons_name].sign
                assert sign == node2.cons[cons_name].sign
                query = node_sum.cons[cons_name].query
                assert query == node1.cons[cons_name].query
                assert query == node2.cons[cons_name].query

            # # Removed checking constraints rhs, that is done in constraint creator testing
            # for cons_name in set(consn) - {'nurse_nva_0', }:
            #     assert np.array_equal(node_sum.cons[cons_name].rhs, data1[cons_name] + data2[cons_name])

            assert np.array_equal(node_sum.cons['nurse_nva_0'].rhs, np.array([0]))

            # Check if invariants are summed
            for inv_name in set(invn):
                assert np.all(node_sum.invar[inv_name] == GeounitNode.addInvariants(node1, node2)[inv_name])
        else:
            assert not node_sum.cons and not node_sum.invar

        if add_dpqueries and dpq_make:
            assert node_sum.dp.Var == node1.dp.Var + node2.dp.Var
            assert np.array_equal(node_sum.unzipNoisy().dp.DPanswer, node1.dp.DPanswer + node2.dp.DPanswer)
        else:
            assert node_sum.dp is None

        if add_dpqueries and add_querydict:

            for name, query in node_sum.dp_queries.items():
                assert query.Var == node1.dp_queries[name].Var + node2.dp_queries[name].Var
                assert np.array_equal(query.unzipDPanswer().DPanswer, node1.dp_queries[name].DPanswer + node2.dp_queries[name].DPanswer)
        # else:
        #     assert node_sum.dp_queries is None

    @pytest.mark.parametrize("test_input, err_msg", [
        (
                (makeNode(d1, '12', geocode_dict={2: 'State', 1: 'US'}), makeNode(d2, '12', geocode_dict={5: 'County', 2: 'State'})),
                'GeoNodes cannot be added: addends have different geocodeDict:'
        ),
        (
                (makeNode(d1, '1234567890abcedf', geocode_dict={16: 'Block', 12: 'Block_Group'}), makeNode(d2, '12', geocode_dict={5: 'County',
                                                                                                                                   2: 'State'})),
                'GeoNodes cannot be added: addends have different geolevel:'
        ),
    ])
    def test_addGeounitNodesErrorsRaising(self, test_input, err_msg):
        node1, node2 = test_input
        with pytest.raises(IncompatibleAddendsError) as err:
            node1.addInReduce(node2)
        assert err_msg in str(err.value)
        # assert err_msg in caplog.text

    @staticmethod
    def test_addGeounitNodesWarningRaising():
        node1 = makeNode(d1, '12', geocode_dict=geocodeDict, addsyn=True)
        node2 = makeNode(d2, '12', geocode_dict=geocodeDict, addsyn=False)
        wrn_type, wrn_msg = RuntimeWarning, "One of the GeoUnit-node addends has syn data, while the other does not (geocodes"
        with pytest.warns(wrn_type) as wrn:
            node1.addInReduce(node2)
        assert wrn_msg in str(wrn.list[0].message)
        # assert wrn_msg in caplog.text

    @staticmethod
    def test_checkConstraints():
        node = makeNode(d1, '12', geocode_dict=geocodeDict, addsyn=True)
        assert not node.checkConstraints(convertPL94(d1))
        fl = node.checkConstraints(convertPL94(d1), return_list=True)
        assert fl == [('nurse_nva_0', np.array(1), np.array(0), '=')]

    @pytest.mark.parametrize("block_cons", [True, False])
    @pytest.mark.parametrize("county_cons", [True, False])
    @pytest.mark.parametrize("state_cons", [True, False])
    def test_makeAdditionalInvariantsConstraints(self, block_cons, state_cons, county_cons):

        class TestSetup(DASDecennialSetup):
            def __init__(self):
                self.hist_shape = (2,)
                self.hist_vars = ("sex",)
                self.validate_input_data_constraints = False
                self.inv_con_by_level = {
                    'Block': {
                        'invar_names': ('tot',) if block_cons else (),
                        'cons_names': ('total',) if block_cons else (),
                    },
                    'County': {
                        'invar_names': ('tot',) if county_cons else (),
                        'cons_names': ('total',) if county_cons else (),
                    },
                    'State': {
                        'invar_names': ('tot',) if state_cons else (),
                        'cons_names': ('total',) if state_cons else ()
                    }
                }

            @staticmethod
            def makeInvariants(raw, raw_housing, invariant_names):
                inv_dict = {}
                if 'tot' in invariant_names:
                    inv_dict.update({'tot': np.sum(raw.toDense())})
                return inv_dict

            @staticmethod
            def makeConstraints(hist_shape, invariants, constraint_names):
                cons_dict = {}
                if 'total' in constraint_names:
                    cons_dict.update({'total': Constraint(MultiHistQuery(
                                            (QueryFactory.makeTabularGroupQuery((2,), add_over_margins=(0,)), StubQuery((2,1), "stub")),
                                            (1, 0)
                                    ),
                                                          np.array(invariants['tot']), "=", "total")})
                return cons_dict

        setup_instance = TestSetup()
        rb1 = sparse.multiSparse(np.array([1, 2]))
        rb2 = sparse.multiSparse(np.array([3, 4]))
        rb3 = sparse.multiSparse(np.array([5, 6]))
        ub1 = sparse.multiSparse(np.array([101, 102]))
        ub2 = sparse.multiSparse(np.array([103, 104]))
        ub3 = sparse.multiSparse(np.array([105, 106]))

        block_nodes = []
        for block, geocode in zip([rb1, rb2, rb3, ub1, ub2, ub3], ['1RB1', '1RB2', '1RB3', '1UB1', '1UB2', '1UB3']):
            invariants = setup_instance.makeInvariants(raw=block, raw_housing=block, invariant_names=setup_instance.inv_con_by_level['Block']['invar_names'])
            constraints = setup_instance.makeConstraints(hist_shape=(2,), invariants=invariants, constraint_names=setup_instance.inv_con_by_level['Block']['cons_names'])
            block_nodes.append(GeounitNode(geocode, raw=block, raw_housing=block,
                                           invar=invariants,
                                           cons=constraints,
                                           geocode_dict={4: "Block", 3: "County", 1: "State"}))

        rc = block_nodes[0].addInReduce(block_nodes[1]).addInReduce(block_nodes[2]).shiftGeocodesUp()
        rc.makeAdditionalInvariantsConstraints(setup_instance)
        uc = block_nodes[3].addInReduce(block_nodes[4]).addInReduce(block_nodes[5]).shiftGeocodesUp()
        uc.makeAdditionalInvariantsConstraints(setup_instance)
        state = rc.addInReduce(uc).shiftGeocodesUp()
        state.makeAdditionalInvariantsConstraints(setup_instance)

        assert state.checkConstraints()
        assert rc.checkConstraints()
        assert uc.checkConstraints()


class TestManipulateNodes:
    # For .union().groupByKey()
    @pytest.mark.parametrize("geocodes, answercodes", [
        (
                ('02', ('02543', '02', '02123')),
                ('02', ['02123', '02543'])
        ),
        (
                ('44003', ('44003654321', '44003123456', '44003')),
                ('44003', ['44003123456', '44003654321'])

        ), (('12345678901', ('123456789017', '123456789013', '12345678901', '123456789016', '123456789011')),
            ('12345678901', ['123456789011', '123456789013', '123456789016', '123456789017'])),
    ])
    def test_findParentChildNodes(self, geocodes, answercodes):
        pc_nodes = (geocodes[0], map(makeEmptyNode, geocodes[1]))
        self.assertFindParentChildResult(answercodes, pc_nodes)

    # For .cogroup()
    @pytest.mark.parametrize("geocodes, answercodes", [
        (
                ('02', (('02543',), ('02', '02123'))),
                ('02', ['02123', '02543'])
        ),
        (
                ('44003', (('44003654321',), ('44003123456', '44003'))),
                ('44003', ['44003123456', '44003654321'])

        ), (('12345678901', (('123456789017', '123456789013', '12345678901'), ('123456789016', '123456789011'))),
            ('12345678901', ['123456789011', '123456789013', '123456789016', '123456789017'])),
    ])
    def test_findParentChildNodesTuple(self, geocodes, answercodes):
        pc_nodes = (geocodes[0], tuple(map(makeEmptyNode, geocodeiter) for geocodeiter in geocodes[1]))
        self.assertFindParentChildResult(answercodes, pc_nodes)

    @staticmethod
    def assertFindParentChildResult(answercodes, pc_nodes):
        answer = (makeEmptyNode(answercodes[0]), list(map(makeEmptyNode, answercodes[1])))
        assert mn.findParentChildNodes(pc_nodes) == answer

    def test_stackConstraints(self):
        # invn = ('tot', 'va', 'gqhh_vect', 'gqhh_tot', 'gq_vect')
        cons_full_list = ["total", "hhgq_total_lb", "hhgq_total_ub", "voting_age", "hhgq_va_ub", "hhgq_va_lb", "nurse_nva_0"]
        cons_names_list = [
            cons_full_list[:1] + cons_full_list[2:],
            cons_full_list[:2] + cons_full_list[3:],
            cons_full_list[:5] + cons_full_list[6:],
        ]

        children = [makeNode(np.zeros((5, 5)), '02543', geocodeDict, consn=consn, invn=invn) for consn in cons_names_list]
        sc = mn.stackNodeProperties(children, lambda node: node.cons, StackedConstraint)
        assert set(map(lambda c: c.name, sc)) == set(cons_full_list)
        ind_dict = {c.name: c.indices for c in sc}
        for name in np.array(cons_full_list)[[0, 3, 4, 6]]:
            assert sorted(ind_dict[name]) == [0, 1, 2]

        assert sorted(ind_dict["hhgq_total_lb"]) == [1, 2]
        assert sorted(ind_dict["hhgq_total_ub"]) == [0, 2]
        assert sorted(ind_dict["hhgq_va_lb"]) == [0, 1]

        # children[1]._cons = None
        # sc = mn.stackNodeProperties(children, lambda node: node.cons, StackedConstraint)
        # assert sc is None

    def test_stackDpqueries(self):
        querylist = [
            ['cenrace', 'cenrace * hispanic', 'votingage * hispanic', 'votingage'],
            ['cenrace', 'votingage * hhgq', 'cenrace * hispanic'],
            ['cenrace', 'votingage', 'cenrace * hispanic']
        ]

        children = [makeNode(np.zeros((5, 5)), '02543', geocodeDict, querydict=dp_queries) for dp_queries in querylist]
        sdp = mn.stackNodeProperties(children, lambda node: node.dp_queries, StackedDPquery)
        qw = list(map(lambda sdpq: 1./sdpq.VarList[0], sdp))
        assert set(map(lambda q: q.name, sdp)) == set(reduce(add, querylist))
        ind_dict = {q.name: q.indices for q in sdp}
        for name in ['cenrace', 'cenrace * hispanic']:
            assert sorted(ind_dict[name]) == [0, 1, 2]

        assert sorted(ind_dict["votingage"]) == [0, 2]
        assert sorted(ind_dict["votingage * hhgq"]) == [1]
        assert sorted(ind_dict["votingage * hispanic"]) == [0]
        assert abs(qw[0] - 0.00125026043837) < 1e-10

        # children[0].dp_queries = ()
        # sdp = mn.stackNodeProperties(children, lambda node: node.dp_queries, StackedDPquery)
        # assert sdp == []

    def test_makeChildGroups(self):
        geocodeDictAIAN = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 3: 'State', 0: 'US'}
        n1 = makeNode(np.zeros((1, 5)), '006', geocodeDictAIAN)
        n2 = makeNode(np.ones((2, 5)), '106', geocodeDictAIAN)
        n3 = makeNode(np.tile([0, 1, 1, 30, 1], (3, 1)), '044', geocodeDictAIAN)
        n4 = makeNode(np.ones((4, 5)), '144', geocodeDictAIAN)
        n5 = makeNode(np.ones((5, 5)), '024', geocodeDictAIAN)
        n6 = makeNode(np.ones((6, 5)), '134', geocodeDictAIAN)
        chlist = [n1, n2, n3, n4, n5, n6]
        cg = mn.makeChildGroups(chlist)
        assert len(cg) == 4

        for cgroup, summand1, summmand2 in [(cg[0], n1, n2), (cg[1], n3, n4)]:
            assert len(cgroup[0]) == 2
            assert chlist.index(summand1) in cgroup[0]
            assert chlist.index(summmand2) in cgroup[0]
            assert cgroup[1] == summand1.invar['tot'] + summmand2.invar['tot']

        for cgroup, lonenode in [(cg[2], n5), (cg[3], n6)]:
            assert len(cgroup[0]) == 1
            assert chlist.index(lonenode) in cgroup[0]
            assert cgroup[1] == lonenode.invar['tot']


@pytest.mark.parametrize("data", [np.zeros((10, 10)), np.arange(120), np.arange(100).reshape(10,10)])
def test_zipRaw(data):
    n = makeNode(np.zeros((5, 5)), '02543', geocodeDict, dpq_make=True)

    n.dp.DPanswer = data
    dp_answers_orig = n.dp.DPanswer

    # Check that it zips
    n = n.zipNoisy()
    zipped_answer = n.dp.DPanswer

    assert isinstance(zipped_answer, bytes), 'Zipped answer should be of type bytes'
    # Compare against the original answer bytes to ensure the result was compressed at least somewhat
    assert len(zipped_answer) < len(bytes(dp_answers_orig )), 'The answer was not compressed at all'

    # Check that trying to zip zipped doesn't change anything
    n = n.zipNoisy()
    assert np.array_equal(zipped_answer, n.dp.DPanswer)

    # Check that it unzips properly
    n = n.unzipNoisy()
    unzipped_answer = n.dp.DPanswer
    assert np.array_equal(unzipped_answer, data)

    # Check that unzipping unzipped doesn't modify the original
    n = n.unzipNoisy()
    unzipped_answer = n.dp.DPanswer
    assert isinstance(unzipped_answer, np.ndarray), 'Unzipped answer should be of type numpy.ndarray'
    assert np.array_equal(unzipped_answer, data)
    assert bytes(unzipped_answer) == bytes(dp_answers_orig ), 'The byte string was modified during zipping or unzipping'
