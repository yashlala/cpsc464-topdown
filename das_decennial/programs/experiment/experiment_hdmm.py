# experiment
# Brett Moran
# 07 March 2019


import logging

from programs.experiment.experiment import experiment
import programs.workload.census_workloads as census_workloads
from das_constants import CC


class ExperimentHDMM(experiment):

    def setWorkloadToConfig(self, budgetgroup, config):
        config.set(section=CC.WORKLOAD, option=CC.WORKLOAD, value=",".join(map(str, budgetgroup.workload_keys)))

    def makeBudgetGroup(self, bg, epsilon_budget_total, geolevel_budget_prop):
        workload_keys = list(self.gettuple(f"{bg}.{CC.WORKLOAD}"))
        logging.info(f"Workload config list: {workload_keys}")
        bg_workload = census_workloads.getWorkload(workload_keys)
        logging.info(f"Budget group workload: {bg_workload}")
        return HDMMBudgetGroup(bg, epsilon_budget_total, geolevel_budget_prop, bg_workload, workload_keys)


class HDMMBudgetGroup:
    def __init__(self, name, epsilon_budget_total, geolevel_budget_prop, workload, workload_keys):
        self.name = name
        self.epsilon_budget_total = epsilon_budget_total
        self.geolevel_budget_prop = geolevel_budget_prop
        self.workload = workload
        self.workload_keys = workload_keys

    def getBudgetsAsString(self):
        return ", ".join([str(b) for b in self.geolevel_budget_prop])

    def __repr__(self):
        """
        """
        items = [
            f"Name: {str(self.name)}",
            f"epsilon_budget_total: {str(self.epsilon_budget_total)}",
            f"geolevel_budget_prop: {self.getBudgetsAsString()}",
            f"workload: {self.workload}",
            f"workload_keys: {self.workload_keys}"
        ]

        return " | ".join(items)

    def __str__(self):
        return "\n".join(self.__repr__().split("|"))
