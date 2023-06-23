"""
Experiment module for DAS Decennial that uses config file variable loops implemented in das_framework
"""
from typing import Tuple
import pandas as pd
from configparser import NoOptionError
from contextlib import ExitStack
import datetime
import numpy as np
from programs.writer.writer import DASDecennialWriter
from das_framework.driver import AbstractDASExperiment

LEVEL = "level"
TVD = "1mTVD"


class ConfigLoopsExperiment(AbstractDASExperiment):
    """ Runs the whole DAS multiple times in experimental loops, including reader and takedown (but not setup)"""
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        assert isinstance(self.das.writer, DASDecennialWriter)
        self.odfname = self.das.writer.output_datafname
        self.das.writer.unsetOverwriteFlag()
        # self.exp_params_names = [loop[1] for loop in self.loops]

    def getParamStr(self) -> Tuple[dict, str]:
        exp_params = {}
        for loop in self.loops:
            try:
                exp_params[loop[1]] = self.getfloat(loop[1], section=loop[0])
            except ValueError:
                exp_params[loop[1]] = self.getconfig(loop[1], section=loop[0])
        return exp_params, "-".join([f"{k}{v}" for k, v in exp_params.items()])

    def runExperiment(self):

        while self.state is not None:
            # NOTE: The config substitution affect only the variables that are set outside of __init__ methods of das_framework modules (reader, engine etc.)
            self.substitute_config()

            exp_params, exp_params_str = self.getParamStr()

            self.das.writer.setOutputFileDataName(f"{self.odfname}-{exp_params_str}")
            self.das.run()
            self.increment_state()

        return None


class ConfigLoopsExperimentEngineWriter(ConfigLoopsExperiment):
    """ Runs reader once, and engine and writer multiple times in the experimental loop. No other modules"""

    def runExperiment(self):
        original_data = self.das.runReader()

        while self.state is not None:
            # NOTE: The config substitution affect only the variables that are set outside of __init__ methods of das_framework modules (reader, engine etc.)
            self.substitute_config()

            exp_params, exp_params_str = self.getParamStr()

            self.das.writer.setOutputFileDataName(f"{self.odfname}-{exp_params_str}")
            protected_data = self.das.runEngine(original_data)
            # Added error metrics as a sanity check
            self.das.runErrorMetrics(protected_data)
            self.das.runWriter(protected_data)
            self.increment_state()

        return None


class ConfigLoopsExperimentByLevel(ConfigLoopsExperiment):
    """
    Runs reader once, and engine, accuracy metrics and writer multiple times in the experimental loop. No other modules.
    Saves accuracy metrics in CSV by geolevel
    Plots the saved CSV data at the end of the run
    """

    error_metrics_file = None
    dtnow = datetime.datetime.now()
    date = str(dtnow.date())
    timestamp = int(dtnow.timestamp())
    EMFNAME = f"experiment_{date}_result-{timestamp}.csv"
    EMFNAME_ERBIN = f"error_binned_experiment_{date}_result-{timestamp}.csv"
    EMFNAME_POPERBIN = f"pop_erro_experiment_{date}_result-{timestamp}.csv"

    def runExperiment(self):
        original_data = self.das.runReader()

        while self.state is not None:
            # NOTE: The config substitution affect only the variables that are set outside of __init__ methods of das_framework modules (reader, engine etc.)
            self.substitute_config()

            exp_params, exp_params_str = self.getParamStr()
            protected_data = self.das.runEngine(original_data)

            error_metrics_data = self.das.runErrorMetrics(protected_data)
            self.saveErrorMetricsData(exp_params, error_metrics_data)

            self.das.writer.setOutputFileDataName(f"{self.odfname}-{exp_params_str}")
            self.das.runWriter(protected_data)

            self.increment_state()

        try:
            param_name = self.getconfig('plotx')
            self.graphErrorMetricsData(param_name)
        except NoOptionError:
            pass
        except (RuntimeError, AttributeError) as e:
            self.log_warning_and_print(f"Graphing experiment data failed: {str(e)}")


        return None

    def saveErrorMetricsData(self, exp_params: dict, em_data: Tuple[dict, float]):
        if self.error_metrics_file is None:
            with open(self.EMFNAME, "w") as f:
                f.write(",".join(list(exp_params.keys()) + [LEVEL, "QueryError", "L1", f"{TVD}\n"]))
            with open(self.EMFNAME_ERBIN, "w") as f:
                f.write(",".join(list(exp_params.keys()) + [LEVEL, "QueryError", "Bin", "Count\n"]))
            with open(self.EMFNAME_POPERBIN, "w") as f:
                f.write(",".join(list(exp_params.keys()) + [LEVEL, "QueryError", "Popbin", "Bin", "Count\n"]))
            self.error_metrics_file = True

        error_geoleveldict, total_population = em_data
        # with open(self.EMFNAME, "a") as f:
        with ExitStack() as stack:
            f = stack.enter_context(open(self.EMFNAME, "a"))
            file4erbin = stack.enter_context(open(self.EMFNAME_ERBIN, "a"))
            file4poper = stack.enter_context(open(self.EMFNAME_POPERBIN, "a"))
            # for level, error in error_geoleveldict.items():
            #     f.write(",".join([str(v) for v in exp_params.values()]) + f",{level},{error},{1. - error / (2. * total_population)}\n")
            param_csv_string =",".join([str(v) for v in exp_params.values()])
            for level, errors in error_geoleveldict.items():
                for ierr, error in enumerate(errors):
                    if isinstance(error, dict):
                        for qn, qerr in error.items():
                            if isinstance(qerr, np.ndarray):
                                for i in range(qerr.shape[1]):
                                    file4erbin.write(f"{param_csv_string},{level},{qn},{qerr[0][i]},{qerr[1][i]}\n")
                            elif isinstance(qerr, dict):
                                for popbin in sorted(qerr.keys()):
                                    for bin in sorted(qerr[popbin]):
                                        file4poper.write(f"{param_csv_string},{level},{qn},{popbin},{bin},{qerr[popbin][bin]}\n")
                            elif not isinstance(qerr, tuple):
                                f.write(f"{param_csv_string},{level},{qn},{qerr},{1. - qerr / (2. * total_population)}\n")
                    elif isinstance(error, (float, int)):
                        f.write(f"{param_csv_string},{level},Err#{ierr},{error},{1. - error/(2. * total_population)}\n")

    def graphErrorMetricsData(self, param_name):

        self.plotTVD(param_name, self.EMFNAME)

        self.plotBinnedError(param_name, self.getParamStr()[0], self.EMFNAME_ERBIN)

        self.plotPopBinnedError(param_name, self.getParamStr()[0], self.EMFNAME_POPERBIN)

    @staticmethod
    def readAndFilterOtherParams(exp_pars, filename, param_name):
        """
        Read CSV file as Pandas DF and
        select only one instance of each parameter (usually, just one run), because every run
        will have its own bins, they're not plottable together without confusion
        :param exp_pars: iterable of all experimental parameter names
        :param filename: name of the CSV to load
        :param param_name: parameter the dependence on which the plots are supposed to reflect (x-axis or color curve), usually, epsilon
        :return: Pandas Dataframe subset to single values of parameters other than param_name
        """
        df = pd.read_csv(filename)
        for exp_par in exp_pars:
            if exp_par in df and exp_par != param_name:
                exp_par_values = df[exp_par].unique()
                df = df[df[exp_par] == exp_par_values[0]]
        return df

    @staticmethod
    def plotTVD(param_name, filename):
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plt.style.use('ggplot')
        matplotlib.rcParams['mathtext.fontset'] = 'cm'
        matplotlib.rcParams['font.family'] = 'sans-serif'

        dfm = pd.read_csv(filename).groupby([param_name, LEVEL, "QueryError"]).mean().reset_index()
        for query in dfm.QueryError.unique():
            plt.clf()
            for level in dfm.level.unique():
                dfml = dfm[dfm.level == level][dfm.QueryError == query].sort_values(by=param_name)
                #plt.plot(dfml[param_name], dfml[TVD], 'o-', label=level)
                # plt.semilogx(pow(2, dfml[param_name]), dfml[TVD], 'o-', label=level)
                plt.semilogx(dfml[param_name], dfml[TVD], 'o-', label=level)
                # plt.plot(list(map(str, dfml[param_name])), dfml[TVD], label=level)
            plt.title(f"Query '{query}' L1 1-TVD")
            plt.legend()
            plt.ylabel("1 - TVD")
            plt.xlabel(f"{param_name}")
            plt.savefig(f"{filename}-byq-{query}.pdf")
            plt.savefig(f"{filename}-byq-{query}.png")
        for level in dfm.level.unique():
            plt.clf()
            for query in dfm.QueryError.unique():
                dfml = dfm[dfm.level == level][dfm.QueryError == query].sort_values(by=param_name)
                #plt.plot(dfml[param_name], dfml[TVD], 'o-', label=level)
                # plt.semilogx(pow(2, dfml[param_name]), dfml[TVD], 'o-', label=level)
                plt.semilogx(dfml[param_name], dfml[TVD], 'o-', label=query)
                # plt.plot(list(map(str, dfml[param_name])), dfml[TVD], label=query)
            plt.title(f"{level} L1 1-TVD for queries")
            plt.legend()
            plt.ylabel("1 - TVD")
            plt.xlabel(f"{param_name}")
            plt.savefig(f"{filename}-byl-{level}.pdf")
            plt.savefig(f"{filename}-byl-{level}.png")

    @staticmethod
    def plotBinnedError(param_name, exp_pars, filename):

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plt.style.use('ggplot')
        matplotlib.rcParams['mathtext.fontset'] = 'cm'
        matplotlib.rcParams['font.family'] = 'sans-serif'

        df_erbin = ConfigLoopsExperimentByLevel.readAndFilterOtherParams(exp_pars, filename, param_name)
        dfm = df_erbin.groupby([param_name, "level", "QueryError", "Bin"]).mean().reset_index()
        for level in dfm.level.unique():
            for query in dfm.QueryError.unique():
                plt.clf()
                fig, ax = plt.subplots()
                dfmlq = dfm[dfm.level == level][dfm.QueryError == query].sort_values(by=param_name)
                params = sorted(dfmlq[param_name].unique())
                # fig, axes = plt.subplots(1, len(params), sharey='row', sharex=True)
                # for ax, param in zip(axes, params):
                #     dfmlqp = dfmlq[dfmlq[param_name] == param]
                #     ax.barh(dfmlqp.Bin, dfmlqp.Count) # , height=.9 * np.diff(dfmlqp.Bin)[0])
                #     for c, b in zip(dfmlqp.Count, dfmlqp.Bin):
                #         ax.annotate(int(c), (0, b))
                for i, param in enumerate(params):
                    dfmlqp = dfmlq[dfmlq[param_name] == param].sort_values(by='Bin')
                    # w = 0.9 / len(params) * findBinSize(dfmlqp.Bin)
                    # w = 0.9 * findBinSize(dfmlqp.Bin)
                    w = 0.1
                    ax.bar(dfmlqp.Bin + i, dfmlqp.Count, width=w, alpha=0.7)  # , label=f"{param}")
                    ax.plot(dfmlqp.Bin, dfmlqp.Count, 'o-', label=f"{param}")
                ax.legend(title=f"{param_name}")
                ax.set_yscale('log')
                ax.set_xlabel("Query answers L1 error $|q_i(CEF) - q_i|$ for each $i$")
                ax.set_ylabel("Count")
                ax.set_title(f"Query '{query}' L1 error for {level}")
                plt.savefig(f'{filename}-{query}-{level}.pdf')
                plt.savefig(f'{filename}-{query}-{level}.png')

    @staticmethod
    def plotPopBinnedError(param_name, exp_pars, filename):

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plt.style.use('ggplot')
        matplotlib.rcParams['mathtext.fontset'] = 'cm'
        matplotlib.rcParams['font.family'] = 'sans-serif'

        df_poperbin = ConfigLoopsExperimentByLevel.readAndFilterOtherParams(exp_pars, filename, param_name)
        dfm = df_poperbin.groupby([param_name, "level", "QueryError", "Bin", "Popbin"]).mean().reset_index()
        for level in dfm.level.unique():
            for query in dfm.QueryError.unique():
                plt.clf()

                dfmlq = dfm[dfm.level == level][dfm.QueryError == query].sort_values(by=param_name)
                params = sorted(dfmlq[param_name].unique())
                popbins = sorted(dfmlq.Popbin.unique())
                layouts = {
                    1: (1, 1),
                    2: (1, 2),
                    3: (1, 3),
                    4: (2, 2),
                    5: (2, 3),
                    6: (2, 3),
                    7: (3, 3)
                }
                fig, axes = plt.subplots(*layouts[len(popbins)], sharey='all', sharex='all')
                axes_iter = axes.ravel() if len(popbins) > 1 else [axes]
                for popbin, ax in zip(popbins, axes_iter):
                    dfmlqpb = dfmlq[dfmlq.Popbin == popbin]
                    for i, param in enumerate(params):
                        dfmlqp = dfmlqpb[dfmlq[param_name] == param].sort_values(by='Bin')
                        # w = 0.9 / len(params) * findBinSize(dfmlqp.Bin)
                        # w = 0.9 * findBinSize(dfmlqp.Bin)
                        w = 0.1
                        ax.bar(dfmlqp.Bin + i, dfmlqp.Count, width=w, alpha=0.7)  # , label=f"{param}")
                        ax.plot(dfmlqp.Bin, dfmlqp.Count, 'o-', label=f"{param}")
                    ax.set_yscale('log')
                    # ax.set_xscale('log')

                    ax.set_title(f"{popbin}+")
                if len(axes_iter) > 1:
                    upper_left_ax = axes[0, 0] if len(axes.shape) > 1 else axes[0]
                    upper_right_ax = axes[0, -1] if len(axes.shape) > 1 else axes[-1]
                    lower_left_ax = axes[-1, 0] if len(axes.shape) > 1 else axes[0]
                else:
                    upper_left_ax = axes
                    upper_right_ax = axes
                    lower_left_ax = axes
                upper_left_ax.set_ylabel("Count")
                upper_right_ax.legend(title=f"{param_name}")
                lower_left_ax.set_xlabel("Query answers L1 error $|q_i(CEF) - q_i|$ for each $i$")
                fig.suptitle(f"Query '{query}' L1 error for {level}")
                plt.savefig(f'{filename}-{query}-{level}.png')
                plt.savefig(f'{filename}-{query}-{level}.pdf')


if __name__ == '__main__':
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("--tvd_csv", help=".CSV with average errors and 1-TVD")
    parser.add_argument("--erbin_csv", help=".CSV with errors binned by error value")
    parser.add_argument("--poperbin_csv",help=".CSV with errors binned by node population and error value")
    parser.add_argument("--exp_par", help="List of experimental parameters", default='run')
    parser.add_argument("--param_name", help="List of experimental parameters", default='epsilon')
    args = parser.parse_args()

    exp_pars = args.exp_par.split(',')

    if args.tvd_csv:
        ConfigLoopsExperimentByLevel.plotTVD(args.param_name, args.tvd_csv)

    if args.erbin_csv:
        ConfigLoopsExperimentByLevel.plotBinnedError(args.param_name, exp_pars, args.erbin_csv)

    if args.poperbin_csv:
        ConfigLoopsExperimentByLevel.plotPopBinnedError(args.param_name, exp_pars, args.poperbin_csv)
