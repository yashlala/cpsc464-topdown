"""
    Explanatory header.
"""

from collections import defaultdict
import sys
sys.path.append("..")

from fractions import Fraction
from copy import deepcopy
from das_constants import CC
import numpy as np
import pandas as pd
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('display.width', None)
import programs.strategies.strategies as strategies

FRAC_BASE = 4096

def printV(msg, VERBOSE=False):
    if VERBOSE:
        print(msg)

def adjustRho(mult_add=None, table="Persons", geo="US", VERBOSE=False, INCR_MODE="multiplicative"):
    assert mult_add is not None, "Received None mult_add"
    global_rho_dict = {
        "Persons": 1/((2564/3765)**2), # Option E, US Persons [as of 6/4/21]
        "Units":   1/((4499/959)**2),
    }
    global_rho = global_rho_dict[table]
    geolevels_dict = {
        "US": ["US", "State", "County", "Tract", "Block_Group", "Block"],
        "PR": ["State", "County", "Tract", "Block_Group", "Block"],
    }
    geolevels = geolevels_dict[geo]
    geolevel_props_dict = {
        "US" : {
            "Persons" : [123/4100, 1708/4100, 530/4100, 434/4100, 1110/4100, 195/4100], # US Per E
            "Units"   : [1/1024, 1/1024, 18/1024, 75/1024, 725/1024, 204/1024], # US Units A
        },
        "PR" : {
            "Persons" : [41393/207788, 313181/1558410, 406901/3116820, 438141/1038940, 39/820], # PR Per E [mod'd to = US gs, B rho]
            "Units"   : [1/1024, 15/1024, 63/1024, 602/1024, 343/1024] # PR Units A
        }
    }
    geolevel_props = geolevel_props_dict[geo][table]
    print(f"For {geo}, using geolevels: {geolevels}")
    strategy_dict = {
        "US" : {
            "Persons" : strategies.ProductionCandidate20210527US_mult8.make(levels=geolevels_dict["US"]), # US Persons E
            "Units"   : strategies.DetailedOnly.make(levels=geolevels)
        },
        "PR" : {
            "Persons" : strategies.ProductionCandidate20210527PR_mult8.make(levels=geolevels_dict["PR"]), # PR Persons E
            "Units"   : strategies.DetailedOnly.make(levels=geolevels)
        }
    }

    geos_qs_props_dict = strategy_dict[geo][table]
    dpqnames = geos_qs_props_dict[CC.DPQUERIES]
    qprops = geos_qs_props_dict[CC.QUERIESPROP]

    from functools import reduce # Only used here
    import operator # Only used here
    dpqnames_collapsed = set(reduce(operator.add, dpqnames.values())) # Useful for writing as if queries-to-include (vs exclude)
    queries_not_to_adjust_dict = {
        "Persons": [dpqn for dpqn in dpqnames_collapsed if dpqn != "hispanic * cenrace"],
        "Units": []
    }

    queries_not_to_adjust = queries_not_to_adjust_dict[table]
    levels_to_adjust = ["Tract", "Block_Group"] # Works for US and PR, despite 'US' not in PR levels

    # --- end of within-fxn control parameters ---

    cur_rhos = defaultdict(list)
    printV("<--- current qprops --->", VERBOSE=VERBOSE)
    for geolevel, gprop in zip(geolevels, geolevel_props):
        printV(f"\t{geolevel} :::", VERBOSE=VERBOSE)
        for qname, qprop in zip(dpqnames[geolevel], qprops[geolevel]):
            printV(f"\t\t{qname} --> {qprop}", VERBOSE=VERBOSE)
            cur_rhos[geolevel].append(gprop * qprop * global_rho)

    printV("<--- current rhos --->", VERBOSE=VERBOSE)
    for geolevel, gprop in zip(geolevels, geolevel_props):
        printV(f"\t{geolevel} :::", VERBOSE=VERBOSE)
        for qname, qrho in zip(dpqnames[geolevel], cur_rhos[geolevel]):
            printV(f"\t\t{qname} --> {qrho}", VERBOSE=VERBOSE)

    new_rhos = defaultdict(list)
    printV("<--- new rhos --->", VERBOSE=VERBOSE)
    for geolevel, gprop in zip(geolevels, geolevel_props):
        printV(f"\t{geolevel} :::", VERBOSE=VERBOSE)
        no_change = 1.0 if INCR_MODE=="multiplicative" else 0.0
        mult_add_tmp = mult_add if geolevel in levels_to_adjust else no_change
        new_rhos[geolevel] = deepcopy(cur_rhos[geolevel])
        for i, (qname, qrho) in enumerate(zip(dpqnames[geolevel], new_rhos[geolevel])):
            mult_add_q = mult_add_tmp if qname not in queries_not_to_adjust else no_change
            new_rhos[geolevel][i] = qrho * mult_add_q if INCR_MODE=="multiplicative" else qrho + mult_add_q
            #printV(f"\t\t{qname} --> {new_rhos[geolevel][i]}", VERBOSE=VERBOSE)
            printV(f"\t\t{qname} --old, new--> {float(qrho)} vs {new_rhos[geolevel][i]} (diff = {float(qrho) - new_rhos[geolevel][i]})", VERBOSE=VERBOSE)

    new_global_rho = 0.0
    new_geolevel_rhos = []
    for geolevel, gprop in zip(geolevels, geolevel_props):
        printV(f"\t{geolevel} :::", VERBOSE=VERBOSE)
        new_geolevel_rho = 0.0
        for i, (qname, qrho) in enumerate(zip(dpqnames[geolevel], new_rhos[geolevel])):
            new_global_rho += qrho
            new_geolevel_rho += qrho
        new_geolevel_rhos.append(new_geolevel_rho)
    mult_add_symbol = "*" if INCR_MODE == "multiplicative" else "+"
    printV(f"global_rho was: {global_rho}. After {mult_add_symbol} {mult_add} in selected queries, it became: {new_global_rho}", VERBOSE=VERBOSE)
    printV(f"global_scale was: {1./np.sqrt(global_rho)}. After {mult_add_symbol} {mult_add} in selected queries, it became: {1./np.sqrt(new_global_rho)}", VERBOSE=VERBOSE)
    printV(f"old_geolevel_rhos: {[gprop * global_rho for gprop in geolevel_props]}", VERBOSE=VERBOSE)
    printV(f"new_geolevel_rhos: {new_geolevel_rhos}", VERBOSE=VERBOSE)

    new_geolevel_props = [Fraction(int(np.ceil(grho / sum(new_geolevel_rhos) * FRAC_BASE)), FRAC_BASE) for grho in new_geolevel_rhos]
    printV(f"Old geolevel proportions: {geolevel_props}", VERBOSE=VERBOSE)
    printV(f"New geolevel proportions: {new_geolevel_props}", VERBOSE=VERBOSE)

    new_qprops = defaultdict(list)
    printV("<--- new qprops --->", VERBOSE=VERBOSE)
    for geolevel, gprop in zip(geolevels, geolevel_props):
        printV(f"\t{geolevel} :::", VERBOSE=VERBOSE)
        new_qprops[geolevel] = deepcopy(new_rhos[geolevel])
        for i, (qname, qrho) in enumerate(zip(dpqnames[geolevel], new_rhos[geolevel])):
            new_qprops[geolevel][i] = qrho / sum(new_rhos[geolevel])
            printV(f"\t\t{qname} --old, new--> {float(qprops[geolevel][i])} vs {new_qprops[geolevel][i]}", VERBOSE=VERBOSE)

    new_frac_qprops = defaultdict(list)
    printV("<--- new frac qprops --->", VERBOSE=VERBOSE)
    for geolevel, gprop in zip(geolevels, geolevel_props):
        printV(f"\t{geolevel} :::", VERBOSE=VERBOSE)
        new_frac_qprops[geolevel] = deepcopy(new_qprops[geolevel])
        for i, (qname, qrho) in enumerate(zip(dpqnames[geolevel], new_qprops[geolevel])):
            new_frac_qprops[geolevel][i] = Fraction(int(np.ceil(new_qprops[geolevel][i] * FRAC_BASE)), FRAC_BASE)
            of = Fraction(int(np.ceil(qprops[geolevel][i] * FRAC_BASE)), FRAC_BASE)
            printV(f"\t\t{qname} --old, new--> {of} vs {new_frac_qprops[geolevel][i]}", VERBOSE=VERBOSE)
        printV(f"\t\tsum: {sum(new_frac_qprops[geolevel])}", VERBOSE=VERBOSE)

    frac_geolevel_props = [Fraction(int(np.ceil(gprop * FRAC_BASE)), FRAC_BASE) for gprop in new_geolevel_props]
    printV(f"New fractional geolevel props: {frac_geolevel_props}, sum: {sum(frac_geolevel_props)}", VERBOSE=VERBOSE)
    return Fraction(1/np.sqrt(new_global_rho)).limit_denominator(FRAC_BASE), new_geolevel_props, new_frac_qprops

if __name__ == "__main__":
    VERBOSE = False
    INCR_MODE = "additive" # additive, multiplicative [way in which to increase rho allocations by mult_add]
    a = {
        'geo': [],
        'table': [],
        'mult_add': [],
        'scale': [],
        #'gprops': [],
        #'gprops_sum' : [],
        'gprops_normalized' : [],
        #'qprops': [],
        #'qprops_sum' : [],
        'qprops_normalized' : [],
    }
    for geo in ["US", "PR"]:
        #for table in ["Persons", "Units"]:
        for table in ["Persons"]:
            #for mult_add in [0.5, 1., 2., 4., 8.]: # Original multiplier-based sequence [generated 'A-E' options]
            for mult_add in [0.05, 0.1, 0.2, 0.3, 0.4]: # Additive increments following June 3 DSEP POP/DEMO asks
                print(f"<------------------>")
                print(f"INITIATING {geo} {table} {mult_add} calculations....")
                a['geo'].append(geo)
                a['table'].append(table)
                a['mult_add'].append(mult_add)
                res = adjustRho(mult_add=mult_add, table=table, geo=geo, VERBOSE=VERBOSE, INCR_MODE=INCR_MODE)
                a['scale'].append(res[0])
                print(f"scale: {res[0]}")
                #a['gprops'].append(res[1])
                #a['gprops_sum'].append(sum(res[1]))
                a['gprops_normalized'].append([gprop/sum(res[1]) for gprop in res[1]])
                print(f"gprops_normalized: {[gprop/sum(res[1]) for gprop in res[1]]}")
                #a['qprops'].append(res[2])
                #print("qprops: ", res[2])
                #qprops_sum = {}
                #for lev, qprops in res[2].items():
                #    qprops_sum[lev] = sum(qprops)
                #a['qprops_sum'].append(qprops_sum)
                qprops_normalized = {}
                for lev, qprops in res[2].items():
                    this_lev_normalized = [qprop/sum(qprops) for qprop in qprops]
                    qprops_normalized[lev] = tuple(this_lev_normalized)
                a['qprops_normalized'].append(qprops_normalized)
                print(f"\n\n------\n\n{geo} {table} {mult_add} qprops_normalized: \n")
                #for k, v in qprops_normalized.items():
                #    print(k, "--->\t\t", v, "\n")

                print("<--------------------->\n\n\n")
    if VERBOSE:
        print(pd.DataFrame(a))
