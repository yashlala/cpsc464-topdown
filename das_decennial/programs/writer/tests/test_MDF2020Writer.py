import pytest
import numpy as np

from configparser import ConfigParser
from pyspark.sql import SparkSession

from programs.constraints.tests import Household2010_testdata
from programs.invariants.invariantsmaker import InvariantsMaker
from programs.constraints.Constraints_Household2010 import ConstraintsCreator as HHConstraintsCreator
from programs.constraints.Constraints_DHCP_HHGQ import ConstraintsCreator as PConstraintsCreator
from programs.nodes.nodes import GeounitNode
from programs.writer.hh2010_to_mdfunit2020 import Household2010ToMDFUnit2020Recoder
from programs.writer.dhcp_hhgq_to_mdfpersons2020 import DHCPHHGQToMDFPersons2020Recoder
from programs.writer.rowtools import makeHistRowsFromMultiSparse
from programs.writer.mdf2020writer import MDF2020HouseholdWriter, addEmptyAndGQ, MDF2020PersonWriter
from programs.schema.schemas.schemamaker import SchemaMaker

from programs.tests.spark_fixture import spark  # Might show up as not used, but it is a fixture, it's used in the function arguments. Do not remove.
spark_fixture = spark  # This line is so that the previous import is formally used

from das_utils import table2hists
from das_constants import CC


def get_reader_stub():
    class ReaderStub:
        def __init__(self):
            # TODO: Inverts it here because it will be reinverted
            self.modified_geocode_dict = {v: k for k, v in CC.DEFAULT_GEOCODE_DICT.items()}
    return ReaderStub()

hhdata = {
        'households': [
            # each row is a household
            # (shape (2, 9, 2, 7, 8, 24, 2, 4, 2) + unit UID)
            #columns: 'hhsex', 'hhage', 'hisp', 'race', 'size', 'hhtype', 'elderly', 'multi', 'unit UID'
                        [1,     8,       1,      0,      1,      20,        0,        0,          0],
                        [0,     6,       0,      2,      1,      1,         2,        1,          1],
                        [0,     6,       0,      2,      1,      1,         2,        1,          2],
                        [0,     6,       0,      2,      1,      1,         2,        1,          3],
                        [0,     6,       0,      2,      1,      1,         2,        1,          4],
                        [1,     3,       0,      4,      1,      18,        1,        0,          5],
                        [0,     3,       0,      3,      7,      15,        3,        1,          6],
                        [1,     2,       0,      6,      0,      15,        1,        0,          7]
                ],
        'answers': {
            'GQTYPE': ["000"] * 8 + ["000"] * 3 + ["101", "104", "701"],  # 8 households, 3 vacant units and 2->101 (Fed detention), 5->104 (Local jail), 20->701 (shelter)
        },
        'units' : [
                        # each row is a unit
                        # columns: 'hhgq','unit UID',
                        [0, 0],
                        [0, 1],
                        [0, 2],
                        [0, 3],
                        [0, 4],
                        [0, 5],
                        [0, 6],
                        [0, 7],
                        [1, 8],
                        [1, 9],
                        [1, 10],
                        [2, 11],
                        [5, 12],
                        [20, 13]
                   ]
    }

pdata = {
        'persons': [   # columns: 'hhgq', 'sex', 'age', 'hispanic', 'cenrace', 'citizen', 'unit UID' (shape (8, 2, 116, 2, 63, 1) + unit UID)
                # each row is a person
                [0, 1, 10, 1, 20, 1, 0],
                [0, 0, 20, 0,  1, 0, 1],
                [3, 1, 45, 0, 10, 1, 2],
                [3, 0, 80, 0, 15, 0, 2],
                [2, 1, 90, 0, 15, 1, 3]
            ]
}




class TestMDF2020HouseholdWriter():

    config = f"""
[reader]
input_data_vintage=1000

[setup]
spark.name: DAS
spark.loglevel: ERROR

[schema]
schema: Household2010

[geodict]
geolevel_names: Stub
geolevel_leng: 1
aian_areas:{CC.DAS_AIAN_AREAS_CNSTAT}

[budget]
geolevel_budget_prop: 1
strategy: DetailedOnly
global_scale: 1

[writer]
produce_flag: 1
output_path:
"""
    schema = SchemaMaker.fromName(CC.DAS_Household2010)
    unit_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10)

    def makeNode(self, hholds, units, geocode='1234567891234567'):
        hhold_hist, unit_hist = table2hists(np.array(hholds), self.schema), table2hists(np.array(units), self.unit_schema, CC.ATTR_HHGQ, units=True)

        invar = InvariantsMaker.make(schema=CC.SCHEMA_HOUSEHOLD2010, raw=hhold_hist, raw_housing=unit_hist, invariant_names=('tot', 'gqhh_vect'))
        cons = HHConstraintsCreator(hist_shape=(hhold_hist.shape, unit_hist.shape), invariants=invar,
                                  constraint_names=('no_vacant', 'living_alone', 'size2')).calculateConstraints().constraints_dict
        node = GeounitNode(raw=hhold_hist, raw_housing=unit_hist, invar=invar, cons=cons, geocode_dict={16: 'Stub'}, geocode=geocode)
        node.syn = node.raw

        return node

    @pytest.mark.parametrize("recode", [True, False])
    @pytest.mark.parametrize("as_dict", [True, False])
    @pytest.mark.parametrize("hholds, units", [
        (Household2010_testdata.data['data'], Household2010_testdata.data['units']),
        (hhdata['households'], hhdata['units']),
    ])
    def test_makeHistRowsFromMultiSparse(self, hholds, units, as_dict, recode):
        node = self.makeNode(hholds, units)
        if as_dict:
            node = node.toDict(keep_attrs=(CC.SYN, CC.GEOCODE, CC.INVAR))
        if recode:
            rows = makeHistRowsFromMultiSparse(node, self.schema, row_recoder=Household2010ToMDFUnit2020Recoder)
            assert len(rows) == len(hholds)
            rows = addEmptyAndGQ(node, self.schema, rows, row_recoder=Household2010ToMDFUnit2020Recoder)
            assert len(rows) == len(units)
        else:
            rows = makeHistRowsFromMultiSparse(node, self.schema, add_schema_name=False)

        input_rows = ["|".join(map(str, row[:-1])) for row in hholds]

        if not recode:
            match_cnt = 0
            for row in rows:
                row_str = "|".join([row[var] for var in self.schema.dimnames])
                for inp_row in input_rows:
                    if row_str == inp_row:
                        match_cnt += 1
                        input_rows.remove(inp_row)
                        break
            assert match_cnt == len(rows) == len(hholds)

        else:
            assert len(rows) == len(units)

    @pytest.mark.parametrize("hholds, units", [
        (Household2010_testdata.data['data'], Household2010_testdata.data['units']),
        (hhdata['households'], hhdata['units']),
    ])
    def test_makeHistRowsFromMultiSparseRecode(self, hholds, units):
        node = self.makeNode(hholds, units)
        rows = makeHistRowsFromMultiSparse(node.toDict(keep_attrs=(CC.SYN, CC.GEOCODE, CC.INVAR)), self.schema, row_recoder=Household2010ToMDFUnit2020Recoder)
        ## TODO: Some testing of the MDF spec output should probably be done here. Maybe on just one case. Maybe not, and just test it within the
        #   writer test below
        assert len(rows) == len(hholds)
        rows = addEmptyAndGQ(node, self.schema, rows, row_recoder=Household2010ToMDFUnit2020Recoder)
        assert len(rows) == len(units)
        pass

    def test_transformRDDForSaving(self, spark, dd_das_stub):
        dd_das_stub.reader = get_reader_stub()

        config = ConfigParser()
        config.read_string(self.config)
        import programs.das_setup as ds
        setup_instance = ds.DASDecennialSetup(config=config, name='setup', das=dd_das_stub)
        w = MDF2020HouseholdWriter(config=setup_instance.config, setup=setup_instance, name='writer', das=dd_das_stub)

        hholds = hhdata['households']
        units = hhdata['units']
        node1 = self.makeNode(hholds[:4], units[:4], geocode='0123456789123456')
        node2 = self.makeNode(hholds[4:], units[4:], geocode='0123456789123456')
        spark = SparkSession.builder.getOrCreate()
        node_rdd = spark.sparkContext.parallelize([node1, node2])
        df = w.transformRDDForSaving(node_rdd)
        df.show()

        assert df.count() == len(units)

        for val in df.select('P18').collect():
            assert val['P18'] == 9

        for val in df.select('PAC').collect():
            assert val['PAC'] == '9'

        def len_cond(cond):
            return len(np.where(cond)[0])

        num_gq = len_cond(np.array(units)[:,0] > 1)

        rtype = np.array(df.select('RTYPE').collect())

        assert len_cond(rtype[:,0] == '4') == num_gq
        assert len_cond(rtype[:, 0] == '2') == len(units) - num_gq


class TestMDF2020PersonWriter():

    config = f"""
[reader]
input_data_vintage=1000

[setup]
spark.name: DAS
spark.loglevel: ERROR

[schema]
schema: DHCP_HHGQ

[geodict]
geolevel_names: Block,Block_Group,Tract,Tract_Group,County,State,US
geolevel_leng: 16,12,11,9,5,2,0
aian_areas:{CC.DAS_AIAN_AREAS_CNSTAT}

[budget]
geolevel_budget_prop: 1/7,1/7,1/7,1/7,1/7,1/7,1/7
strategy = DetailedOnly
global_scale=1

[writer]
produce_flag: 1
output_path:
"""
    schema = SchemaMaker.fromName(CC.DAS_DHCP_HHGQ)

    def makeNode(self, persons, geocode='0123456789abcdef'):
        person_hist, unit_hist = table2hists(np.array(persons), self.schema, housing_varname=CC.ATTR_HHGQ)

        invar = InvariantsMaker.make(schema=CC.DAS_DHCP_HHGQ, raw=person_hist, raw_housing=unit_hist, invariant_names=('tot', 'gqhh_tot', 'gqhh_vect'))
        cons = PConstraintsCreator(hist_shape=(person_hist.shape, unit_hist.shape), invariants=invar,
                                  constraint_names=('hhgq_total_lb', 'hhgq_total_ub', 'nurse_nva_0')).calculateConstraints().constraints_dict
        node = GeounitNode(raw=person_hist, raw_housing=unit_hist, invar=invar, cons=cons, geocode_dict={16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State', 1: 'US'}, geocode=geocode )
        node.syn = node.raw

        return node

    @pytest.mark.parametrize("recode", [True, False])
    @pytest.mark.parametrize("as_dict", [True, False])
    @pytest.mark.parametrize("persons", [
        pdata['persons'],
    ])
    def test_makeHistRowsFromMultiSparse(self, persons, as_dict, recode):
        node = self.makeNode(persons)
        if as_dict:
            node = node.toDict(keep_attrs=(CC.SYN, CC.GEOCODE, CC.INVAR))
        if recode:
            rows = makeHistRowsFromMultiSparse(node, self.schema, row_recoder=DHCPHHGQToMDFPersons2020Recoder)
            assert len(rows) == len(persons)
        else:
            rows = makeHistRowsFromMultiSparse(node, self.schema, add_schema_name=False)

        input_rows = ["|".join(map(str, row[:-1])) for row in persons]

        if not recode:
            match_cnt = 0
            for row in rows:
                row_str = "|".join([row[var] for var in self.schema.dimnames])
                for inp_row in input_rows:
                    if row_str == inp_row:
                        match_cnt += 1
                        input_rows.remove(inp_row)
                        break
            assert match_cnt == len(rows) == len(persons)

        else:
            assert len(rows) == len(persons)

    @pytest.mark.parametrize("persons", [
        #(Household2010_testdata.data['data'], Household2010_testdata.data['units']),
        pdata['persons'],
    ])
    def test_makeHistRowsFromMultiSparseRecode(self, persons):
        node = self.makeNode(persons)
        rows = makeHistRowsFromMultiSparse(node.toDict(keep_attrs=(CC.SYN, CC.GEOCODE, CC.INVAR)), self.schema, row_recoder=DHCPHHGQToMDFPersons2020Recoder)
        ## TODO: Some testing of the MDF spec output should probably be done here. Maybe on just one case. Maybe not, and just test it within the
        #   writer test below
        assert len(rows) == len(persons)

    def test_transformRDDForSaving(self, spark, dd_das_stub):
        dd_das_stub.reader = get_reader_stub()

        config = ConfigParser()
        config.read_string(self.config)
        import programs.das_setup as ds
        setup_instance = ds.DASDecennialSetup(config=config, name='setup', das=dd_das_stub)
        w = MDF2020PersonWriter(config=setup_instance.config, setup=setup_instance, name='writer', das=dd_das_stub)

        persons = pdata['persons']
        node1 = self.makeNode(persons[:2], geocode='0123456789abcdef')
        node2 = self.makeNode(persons[2:], geocode='0123456789abcdeg')
        spark = SparkSession.builder.getOrCreate()
        node_rdd = spark.sparkContext.parallelize([node1, node2])
        df = w.transformRDDForSaving(node_rdd)
        df.show()

        assert df.count() == len(persons)

        for val in df.select('EPNUM').collect():
            assert val['EPNUM'] == 999999999

        for val in df.select('RELSHIP').collect():
            assert val['RELSHIP'] == '99'

        def len_cond(cond):
            return len(np.where(cond)[0])

        num_gq = len_cond(np.array(persons)[:,0] > 0)

        rtype = np.array(df.select('RTYPE').collect())

        assert len_cond(rtype[:, 0] == '5') == num_gq
        assert len_cond(rtype[:, 0] == '3') == len(persons) - num_gq
