"""
This scripts clones out the das_decennial repo from TI Github, then checks out desired commit.
Then creates the query strategy object of desired name and outputs it as a CSV which can
subsequently be used for visualizing and editing with the JavaScript visual tool creating python code
for the edited strategy.

usage: strat2df.py [-h] [--csvname CSVNAME] [--levels LEVELS]
                   [--total_budget TOTAL_BUDGET]
                   [--geolevel_prop GEOLEVEL_PROP] [--max_denom MAX_DENOM]
                   commit strategy_name

Examples to run:
    python das_decennial/scripts/strat2df.py 2f81f6e test_strategy --geolevel_prop=1/8,1/8,3/16,1/16,1/8,1/8,1/8,1/8 --total_budget=334/100 --max_denom=10000
    python das_decennial/scripts/strat2df.py 2f81f6e test_strategy_dhch_20220323_pt3
    python das_decennial/scripts/strat2df.py 2f81f6e test_strategy --levels=State,County,Tract,Block_Group,Block

"""
import tempfile
import os
import sys
import subprocess
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from fractions import Fraction
import math

DAS_GITROOT = 'DAS_GITROOT'
DEFAULT_DAS_GITROOT = 'git@{GIT_HOST_NAME}:DAS'

def makeStrategyCSV(cwd, csvname, strategy_name, levels, total_budget, geolevel_prop_dict, max_denom):
    from programs.strategies.strategies import StrategySelector
    from functools import reduce
    from collections import defaultdict
    import fractions
    from math import gcd
    from das_constants import CC
    import pandas as pd

    def makeDataFrame(strategy_name, levels, total_budget, geolevel_prop_dict):
        strategy = StrategySelector.strategies[strategy_name]().make(levels)
        d = defaultdict(dict)
        for level in strategy[CC.GEODICT_GEOLEVELS]:
            for q, qp in zip(strategy[CC.DPQUERIES][level], strategy[CC.QUERIESPROP][level]):
                # If direct budget is specified, then just use qp (and total_budget is supposedly 1), otherwise multiply by geolevel_prop and total_budget
                d[level][q] = qp * total_budget * (geolevel_prop_dict[level] if geolevel_prop_dict is not None else 1)
            for q, qp in zip(strategy[CC.UNITDPQUERIES][level], strategy[CC.UNITQUERIESPROP][level]):
                # If direct budget is specified, then just use qp (and total_budget is supposedly 1), otherwise multiply by geolevel_prop and total_budget
                d[level][q] = qp * total_budget * (geolevel_prop_dict[level] if geolevel_prop_dict is not None else 1)
        return pd.DataFrame(d).filter(reversed(levels))

    df = makeDataFrame(strategy_name, levels, total_budget, geolevel_prop_dict).transpose()
    if max_denom is None:
        # Find the common denominator, multilpy by it, leaving only numerators the data frame
        # denom =  reduce(fractions.gcd, filter(lambda d: hasattr(d, 'denominator'), df.values.ravel())).denominator

        # Not quite sure the above is equivalent to below, and fractions.gcd is being deprecated anyway. Python 3.9 has lcm, but if not 3.8 then below
        t = list(filter(lambda d: hasattr(d, 'denominator'), df.values.ravel()))
        lcm = t[0].denominator
        for f in t[1:]:
            d = f.denominator
            lcm = lcm * d // gcd(lcm, d)
        denom = lcm

        print(denom, lcm)
        print(df)
        # import numpy as np
        # print(np.unique(df.values.ravel()))
        print(df * denom)
        df_num = df * denom
    else:
        # Limit denominator, just by multiplying by it and rounding the numerators. This will change the total budget slightly.
        denom = max_denom
        #df_num = (df * denom).applymap(round, na_action='ignore').astype(int)
        df_num = (df * denom).applymap(lambda d: round(d) if not math.isnan(d) else "None")

    df_num = df_num.reset_index()
    # print(df_num)
    output_df = df_num.rename(columns={'index':'Level'})  # Column with levels has to be called "Level", that's what JS tool expects
    output_df.to_csv(os.path.join(cwd, csvname),index=None)
    print("\n\nHere's the CSV to copy into stratalloc.js textarea input:\n\n")
    print(output_df.to_csv(index=None))
    print(f"Denominator: {denom}\n\n")


if __name__ == "__main__":
    gitroot = os.environ[DAS_GITROOT] if DAS_GITROOT in os.environ else DEFAULT_DAS_GITROOT
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("--csvname", help="Name of the CSV output file", default='strategy.csv')
    parser.add_argument("commit", help="Commit containing the strategy")
    parser.add_argument("strategy_name", help="Name of the query strategy in programs/strategies/strategies.py Strategy Selector to crete CSV with allocation")
    parser.add_argument('--levels', help="List of geolevels to create strategy for", default='US,State,County,Prim,Tract_Subset_Group,Tract_Subset,Block_Group,Block')
    parser.add_argument('--total_budget', help="If proportions rather than budget itself is indicated, they get multiplied by this. (You'll also need geolevel proportions. Can also be used to rescale a strategy with direct allocation, but in that case this should be a ration of new budget to old budget)", default=1)
    parser.add_argument('--geolevel_prop', help="If proportions rather than budget itself is indicated, the geolevel proportions, to get the budget itself")
    parser.add_argument('--max_denom', help="Limit the resulting denominator to this value")
    args = parser.parse_args()
    levels = args.levels.split(',')
    geolevel_prop = args.geolevel_prop
    total_budget = Fraction(args.total_budget)
    max_denom = int(args.max_denom) if args.max_denom is not None else None
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as td:
        subprocess.run(['git', 'clone', '--recursive', f'{gitroot}/das_decennial.git', td])
        os.chdir(td)
        subprocess.run(['git', 'checkout', args.commit])
        sys.path.insert(1, td)  # Add the newly checked out repo to sys.path, in front, so imports in makeStrategyCSV work and properly
        if geolevel_prop is not None:
            geolevel_prop_dict = dict(zip(levels, map(Fraction, geolevel_prop.split(","))))
            print(f"\n\nGeolevel budget proportons: {geolevel_prop_dict}\n\n")
        else:
            geolevel_prop_dict = None
        makeStrategyCSV(cwd, args.csvname, args.strategy_name, levels, total_budget, geolevel_prop_dict, max_denom)
        os.chdir(cwd)
