"""
Quick, crude script for checking frequencies of errors of different magnitudes in .csv files produced by analysis.
"""
from collections import defaultdict
import glob

#fname = "cnstatDdpSchema_multiL2_singlePassRounder_nested_accuracyTest_noDetailed_optThresh0.1_eps500Only_VA_total_EPT1_P1.csv"
fnames = glob.glob("**/*.csv")
for fname in sorted(fnames):
    lines = open(fname, 'r').readlines()
    counts = defaultdict(int)
    for l in lines:
        if l.split(',')[1]=='BLOCK':
            counts[l.split(',')[-2]] += 1
    print(f"\n\n----- {fname} -----")
    for err, frequency in sorted([item for item in counts.items()], key=lambda item: int(float(item[0]))):
        print(f"{err} ::: {frequency}")
