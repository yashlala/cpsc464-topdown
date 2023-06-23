import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints
from das_constants import CC
from programs.constraints.Constraints_DHCH import ConstraintsCreator as DHCHConstraintsCreator


class ConstraintsCreator(DHCHConstraintsCreator):
    schemaname = CC.SCHEMA_DHCH_TEN_3LEV

    def owned(self, name):
        main_query = self.schema.getQuery(CC.TEN_3LEV)
        unit_query = self.unit_schema.getQuery(CC.TEN_3LEV)
        self.addToConstraintsDictMultiHist(name, (main_query, unit_query), (1, -1), np.array([0, 0, 0]), "=")
