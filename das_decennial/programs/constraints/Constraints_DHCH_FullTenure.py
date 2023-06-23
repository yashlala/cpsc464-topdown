import numpy as np
import programs.constraints.Constraints_DHCH
from das_constants import CC

class ConstraintsCreator(programs.constraints.Constraints_DHCH.ConstraintsCreator):
    """
       This class is what is used to create constraint objects from what is listed in the config file, for household schema
       see base class for keyword arguments description
       Schema: HH_SEX, HH_AGE, HH_HISPANIC, HH_RACE, HH_ELDERLY, TENUre, HH_HHTYPE
       Schema dims: 2, 9, 2, 7, 4, 4, 522
    """

    schemaname = CC.SCHEMA_DHCH_FULLTENURE

    def tot_hh(self, name):
        """
        Total number of households is equal to total number of units in the unit table that are tenured
        (Implemented as the difference is equal to zero)
        """
        # TODO: Check
        main_query = self.schema.getQuery("total")
        unit_query = self.unit_schema.getQuery(CC.HHGQ_HOUSEHOLD_TOTAL)
        self.addToConstraintsDictMultiHist(name, (main_query, unit_query), (1, -1), np.array(0), "=")

    def owned(self, name):
        raise ValueError(f"Owned constraint not applicable for {CC.SCHEMA_DHCH_FULLTENURE} schema!")