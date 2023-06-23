import numpy as np
from programs.invariants.invariantsmaker import InvariantsMaker
from programs.schema.schemas.schemamaker import SchemaMaker, _unit_schema_dict
from das_utils import table2hists
from das_constants import CC


class InvariantTestGenericNoUnits:
    """
        Implements functions generic to invariant testing
    """

    schema_name = None

    d = None

    invariant_names = None

    hhgq_attr = CC.ATTR_HHGQ

    def p_h_data1(self):
        return table2hists(self.d, SchemaMaker.fromName(self.schema_name), self.hhgq_attr)

    def get_inv_dict(self, p_h_data, inv_names):
        return InvariantsMaker.make(schema=self.schema_name, raw=p_h_data[0], raw_housing=p_h_data[1], invariant_names=inv_names)

    def test_create(self):
        p_h, h_h = self.p_h_data1()
        assert np.sum(p_h.toDense()) == len(self.d)
        id = InvariantsMaker.make(schema=self.schema_name,raw=p_h, raw_housing=h_h, invariant_names=self.invariant_names)

    def test_total(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('tot', ))
        assert inv['tot'] == len(self.d)


class InvariantTestGeneric(InvariantTestGenericNoUnits):
    """
    Implements functions generic to invariant testing
    """

    units = None

    def p_h_data1(self):
        return table2hists(self.d, SchemaMaker.fromName(self.schema_name)), table2hists(self.units,
                                                                                        SchemaMaker.fromName(_unit_schema_dict[self.schema_name]),
                                                                                        self.hhgq_attr, units=True)

    def test_create(self):
        p_h, h_h = self.p_h_data1()
        assert np.sum(p_h.toDense()) == len(self.d)
        assert np.sum(h_h.toDense()) == len(self.units)
        id = InvariantsMaker.make(schema=self.schema_name,raw=p_h, raw_housing=h_h, invariant_names=self.invariant_names)
