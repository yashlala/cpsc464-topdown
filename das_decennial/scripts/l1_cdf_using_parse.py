import sys, csv, os
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from das_constants import CC

# This script provides the CDF of the L1 error, evaluated from 0 to MAX_L1_ERROR in intervals of DOMAIN_ELEM_MULT:
MAX_L1_ERROR = 25
DOMAIN_ELEM_MULT = 5
# As well as the CDF evaluate at THRESHOLD_FOR_PROP:
#THRESHOLD_FOR_PROP = 5
THRESHOLD_FOR_PROP = 25
# prefix = "AIAN_State_Total_L1_Errors_Are:["
# prefix = "County_votingage_L1_Errors_Are:["
# prefix = "County_total_L1_Errors_Are:["
prefix = "County_Occupied_Counts_L1_Errors_Are:["
simulation_paths = {"a_strategy_global_scale_and_spine_combination":["das_output1.out", "das_output2.out"]}

def compute_metrics(paths):
    l1_errors = parese_stdout(paths)
    elements_in_domain = [k * DOMAIN_ELEM_MULT for k in range(MAX_L1_ERROR // DOMAIN_ELEM_MULT)]
    if elements_in_domain[-1] < np.max(l1_errors):
        elements_in_domain.append(np.max(l1_errors))
    cdf_vals = [(element, np.round(np.mean(l1_errors <= element), 2)) for element in elements_in_domain]
    prop_lt_thresh = np.round(np.mean(l1_errors < THRESHOLD_FOR_PROP), 2)
    count = len(l1_errors) // len(paths)
    res = [np.mean(l1_errors), prop_lt_thresh, count, cdf_vals]
    return res

def parese_stdout(paths):
    l1_errors = []
    for path in paths:
        with open(path, "r") as stdout:
            for line in stdout:
                if prefix in line:
                    # The output is formatted similar to:
                    # 000622 AIAN_State_Total_L1_Errors_Are:[ 0 1 2 3 4 5 6\n
                    # 000623  7 8 9 10 11 12]\n

                    # First get the line with the keyword in it, and parse it
                    split_line = line[:-1].split(prefix)
                    array_elms_str = split_line[-1]

                    if array_elms_str.endswith(']'):
                        l1_errors.extend([int(x) for x in array_elms_str[:-1].split(" ") if x])
                    else:
                        l1_errors.extend([int(x) for x in array_elms_str.split(" ") if x])
                        while True:
                            next_line = stdout.readline()
                            next_line = next_line.split(" ")[2:]
                            if not next_line[-1].endswith(']\n'):
                                l1_errors.extend([int(x) for x in next_line if x])
                            else:
                                print(next_line)
                                l1_errors.extend([int(x) if k+1 != len(next_line) else int(x[:-2]) for k,x in enumerate(next_line) if x])
                                break
                    break
    print(len(l1_errors))
    print(l1_errors)
    return np.array(l1_errors)

# def main():
#     with open('l1_cdf.csv', 'w') as csvfile:
#         writer = csv.writer(csvfile)
#         new_row = ["Run ID", f"Proportion of Geounits with L1 error <= {THRESHOLD_FOR_PROP}", "Number of Geounits", "Values of CDF"]
#         print(new_row)
#         writer.writerow(new_row)
#         for run_id, paths in simulation_paths.items():
#             new_row = [run_id] + compute_metrics(paths)
#             print(new_row)
#             writer.writerow(new_row)

def main():
    # good
    # DAS-2021-03-15_1037_THIEVISH_OPERATION-DSEP-DEC2020-H1-strategy2b-MultipassRounder-opt_spine-scale941_760-dynamic_geolevel-20210315-153002-trial2-Step2.certificate.pdf
    # 0   1    2    3                        4    5       6  7          8                9         10           11               12       13     14     15
    #                                                        strat                      spine     scale                          date-----tm
    # bad
    # DAS-2021-03-16_386_WELL-PREPARED_SNOW-DSEP-DEC2020-H1-strategy1b-MultipassRounder-opt_spine-scale1028_861-dynamic_geolevel-20210316-015020-trial2-Step2.stdout
    # 0   1    2  3           4             5    6       7  8          9                10        11            12               13       14     15     16
    #                                                       8          9                                        12               13

    output_files = []
    uids = {}
    dirname = '/mnt/gits/das-vm-config/das_decennial/logs/'
    ext = '.stdout'

    for filename in os.listdir(dirname):
        if ext in filename:
            parsed_filename = filename.split('-')
            if len(parsed_filename) >= 16:
                strategy = parsed_filename[7][8:]
                spine = parsed_filename[9]
                scale = parsed_filename[10][5:]
                datetime = parsed_filename[12] + '-' + parsed_filename[13]
                if "dynamic_geolevel" in datetime:
                    strategy = parsed_filename[8][8:]
                    spine = parsed_filename[10]
                    scale = parsed_filename[11][5:]
                    datetime = parsed_filename[13] + '-' + parsed_filename[14]

                uid = spine + '-' + strategy + '-' + scale

                if uid not in uids:
                    uids[uid] = []
                uids[uid].append(dirname + filename)

    with open('l1_cdf.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        new_row = ["Run ID", "Number of Trials", "MAE", f"Proportion of Geounits with L1 error <= {THRESHOLD_FOR_PROP}", "Number of Geounits", "Values of CDF"]
        writer.writerow(new_row)
        for uid, paths in uids.items():
            new_row = [uid, len(paths)] + compute_metrics(paths)
            print(new_row)
            writer.writerow(new_row)



if __name__ == "__main__":
    main()
