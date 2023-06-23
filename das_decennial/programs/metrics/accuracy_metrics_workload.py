from typing import Dict

import numpy as np
from programs.metrics.accuracy_metrics import AccuracyMetrics
import programs.workload.make_workloads as make_workloads
from das_constants import CC

class AccuracyMetricsWorkload(AccuracyMetrics):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        workload = list(self.gettuple(CC.WORKLOAD, section=CC.WORKLOAD, sep=CC.REGEX_CONFIG_DELIM))
        self.workload_dict = make_workloads.WorkloadQueriesCreator(self.setup.schema_obj, workload).workload_queries_dict

    def printErrors(self, error_geoleveldict: Dict, total_population: int) -> None:
        self.log_and_print("########################################")
        print("Total Population: ", total_population)
        self.log_and_print("Workload Error for each geolevel:")
        for geolevel in reversed(self.setup.levels):
            self.log_and_print(f"{geolevel}: {error_geoleveldict[geolevel][0]}", cui=True)
        self.log_and_print("########################################")

    def L1Sum(self, orig: np.ndarray, protected: np.ndarray) -> int:
        """
        """
        error = 0
        for query in self.workload_dict.values():
            error = error + int(np.sum(np.abs(query.answer(protected) - query.answer(orig))))
        return error
