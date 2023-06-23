# experiment
# Brett Moran
# 07/11/2018

import logging
import time
import copy
import gc
from configparser import ConfigParser
from das_framework.driver import AbstractExperiment, DAS, WRITER
from das_constants import CC
OUTPUT_PATH = CC.OUTPUT_PATH
BUDGET_GROUPS = "budget_groups"
NUM_RUNS = "num_runs"
SAVE_ORIGINAL_DATA = "save_original_data_flag"
ORIGINAL_DATA_SAVELOC = "original_data_saveloc"
EXPERIMENT_SAVELOC = "experiment_saveloc"


class experiment(AbstractExperiment):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        # get the experiment's save location
        experiment_saveloc = self.getconfig(EXPERIMENT_SAVELOC)
        logging.debug("Experiment Save Location: {}".format(experiment_saveloc))

        self.save_original_data = self.getboolean(SAVE_ORIGINAL_DATA)
        if self.save_original_data:
            self.original_data_saveloc = self.getconfig(ORIGINAL_DATA_SAVELOC)

        # get the budget groups
        budget_groups = self.gettuple(BUDGET_GROUPS)

        # get the number of runs for each budget group
        self.bg_runs = self.getint(NUM_RUNS)

        # for each budget group:
        # 1. get the budgets and workloads
        self.budget_groups = []
        for bg in budget_groups:
            epsilon_budget_total = self.getfloat(f"{bg}.{CC.EPSILON_BUDGET_TOTAL}")
            geolevel_budget_prop = self.gettuple_of_floats(f"{bg}.{CC.GEOLEVEL_BUDGET_PROP}")
            self.budget_groups.append(self.makeBudgetGroup(bg, epsilon_budget_total, geolevel_budget_prop))

    def runExperiment(self):

        # get the config file
        config: ConfigParser = self.das.config
        saved_config = copy.copy(config)

        # load the original data
        original_data = self.das.runReader()
        logging.debug("Finished Reading")

        # if the save original data flag is 1 (on), save the original data
        if self.save_original_data:
            original_data.saveAsPickleFile(self.original_data_saveloc)

        # get the experiment's save location
        experiment_saveloc = self.getconfig(EXPERIMENT_SAVELOC)
        logging.debug("Experiment Save Location: {}".format(experiment_saveloc))

        # for each budget group:
        # 2. alter the config file so it reflects the current budgets
        # 3. run the engine and save the results "number of runs" times in the specified save location
        # e.g. .../experiment_name/budget_group_name/runs/run_* where * ranges from 0 to "number of runs"-1
        for budgetgroup in self.budget_groups:
            for i in range(self.bg_runs):

                logging.debug(budgetgroup)

                ################################################################
                # Altering the config file to include the current run's settings
                #
                # Note that while it's easier to just copy config to config,
                # parsing the variables in __init__ makes sure config is correct
                # and makes it known in the beginning of the experiment rather than in the middle
                # and that values logged in the log file are actually the one used
                ################################################################
                # alter the config file to include the current set of budgets
                config.set(section=CC.BUDGET, option=CC.EPSILON_BUDGET_TOTAL, value=str(budgetgroup.epsilon_budget_total))

                config.set(section=CC.BUDGET, option=CC.GEOLEVEL_BUDGET_PROP, value=",".join(map(str, budgetgroup.geolevel_budget_prop)))

                # alter the config file to set workload
                self.setWorkloadToConfig(budgetgroup, config)

                # this small block of code makes it so that the experiment_saveloc can
                # end with or without a / (slash) and it won't change the way
                # the experiments are saved
                if experiment_saveloc[-1] == "/":
                    experiment_saveloc = experiment_saveloc[:-1]

                # f"run_{i:04}" means to format the string so it pads i up to 4 places
                # examples:
                # i = 56  => run_0056
                # i = 3   => run_0003
                # i = 172 => run_0172
                # the purpose is to assist with proper "alphabetical ordering" in s3
                run_save_loc = f"{experiment_saveloc}/{budgetgroup.name}/run_{i:04}"

                # alter the config file to include the save location of the run
                # this move is important so that the engine can grab the run's directory
                # and be able to save the measurement rdds if that option is turned on
                config.set(section=WRITER, option=OUTPUT_PATH, value=run_save_loc)
                self.das.writer.output_path = run_save_loc

                logging.debug("Current budget values: {}".format(budgetgroup))

                # run the engine
                # protected_data_i is an rdd of block nodes
                self.das = DAS(config=config)
                t0 = time.time()
                protected_data_i = self.das.runEngine(original_data)
                t1 = time.time()
                self.log_and_print(f"--ENGINE TIMER-- | {t0} | {t1} | {t1 - t0} | {budgetgroup.name} | run_{i:04}")

                # save the data via the experiment writer
                # this run's save_loc

                print(f"Saving run here: {run_save_loc}")

                t0 = time.time()
                written_data_i = self.das.runWriter((protected_data_i))
                t1 = time.time()
                self.log_and_print(f"--WRITER TIMER-- | {t0} | {t1} | {t1 - t0} | {budgetgroup.name} | run_{i:04}")

                # levels = self.setup.levels
                # for level in levels:
                #     protected_data_i[level].unpersist()
                protected_data_i.unpersist()
                # del protected_data_i

                written_data_i.unpersist()
                del written_data_i

                collected = gc.collect()
                print("Garbage collector: collected objects.", collected)
        # experiment_metadata = { "Timestamp": datetime.datetime.now().isoformat()[0:19],
        #                       "Runs": {} }

        self.das = DAS(config=saved_config)
        self.config = saved_config
        return None

    def setWorkloadToConfig(self, budgetgroup, config):

        # alter the config file to include the current budget group's dpqueries
        config.set(section=CC.BUDGET, option=CC.DPQUERIES, value=",".join(budgetgroup.dpqueries))

        # alter the config file to include the current budget group's queries prop
        config.set(section=CC.BUDGET, option=CC.QUERIESPROP, value=",".join(map(str, budgetgroup.queriesprop)))

    def makeBudgetGroup(self, bg, epsilon_budget_total, geolevel_budget_prop):
        dpqueries = self.gettuple(f"{bg}.{CC.DPQUERIES}", default=())
        queriesprop = self.gettuple_of_floats(f"{bg}.{CC.QUERIESPROP}", default=()) if dpqueries else ()
        return ManualBudgetGroup(bg, epsilon_budget_total, geolevel_budget_prop, dpqueries, queriesprop)


class ManualBudgetGroup:
    def __init__(self, name, epsilon_budget_total, geolevel_budget_prop, dpqueries, queriesprop):
        self.name = name
        self.epsilon_budget_total = epsilon_budget_total
        self.geolevel_budget_prop = geolevel_budget_prop
        self.dpqueries = dpqueries
        self.queriesprop = queriesprop

    def getBudgetsAsString(self):
        return ", ".join([str(b) for b in self.geolevel_budget_prop])

    def __repr__(self):
        """
        """
        items = [
            f"Name: {str(self.name)}",
            f"epsilon_budget_total: {str(self.epsilon_budget_total)}",
            f"geolevel_budget_prop: {self.getBudgetsAsString()}",
            "dpqueries: " + str(self.dpqueries),
            "queriesprop: " + str(self.queriesprop)
        ]

        return " | ".join(items)

    def __str__(self):
        return "\n".join(self.__repr__().split("|"))
