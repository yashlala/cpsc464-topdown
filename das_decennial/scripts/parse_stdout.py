"""
parse_stdout.py
author: Some Human
created: 4/8/2021
Parses stdout for experimental run of DAS, and formats output in a way easily uploadable to relevant trackers
"""

# TODO add columns
#       timestamp?
#       specific config/release?
# TODO run analysis (relative_l1_error.py) on stdout
# TODO change generic filename from stdout-i.stdout to filename.stdout to decouple from list order
# TODO incorporate rules for formatting into IdentifyingStrings?


import os
import sys
import csv

# begin tragesty. need to mess with paths and envs to allow access to das_framework :-/
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DAS_FRAMEWORK_DIR = os.path.join(os.path.dirname(THIS_DIR), "das_framework")
assert os.path.exists(DAS_FRAMEWORK_DIR), f"cannot find {DAS_FRAMEWORK_DIR}"
sys.path.append(DAS_FRAMEWORK_DIR)
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
# end tragesty

import das_framework.ctools.s3 as s3
import subprocess

# get das root env
DAS_S3ROOT = os.getenv("DAS_S3ROOT")


class IdentifyingStrings:
    """
    Container for sets of identifying strings in the stdouts
    """

    def __init__(self):
        # identifying strings where we're collecting all lines between the start line and the stop line
        self.multiline = {
            "eps_single": {
                "start": "INFO: For single attr/dim",
                "stop": "For geolevel semantics protecting"
            },
            "eps_gl": {
                "start": "For geolevel semantics protecting",
                "stop": "-DP)"
            },
            "gp": {
                "start": "Place cenrace_7lev_two_comb * hispanic geounit proportion with L1 relative error less than 0.05, binned by CEF total population",
                "stop": "OSE geounit counts in each total population bin"
            },
            "vacant": {
                "start": "Place vacant_count geounit proportion with L1 relative error less than 0.05, binned by CEF total population",
                "stop": "OSE geounit counts in each total population bin"
            },
        }

        # identifying strings where we're looking for a set number of lines below the ID string. n inclusive of start
        self.n_lines_directly_below = {
            "output_files": {
                "start": "DAS OUTPUT FILES",
                "n": 3
            }
        }

        # identifying strings where we're looking for only one value on a single line somwhere below the identifying string
        self.single_line_below = {
            "tl1qe": {
                "start": "total_L1 query L1 error for each geolevel",
                "stop": "INFO: (CUI//SP-CENS)  County"
            },
            "val1qe": {
                "start": "votingage_L1 query L1 error for each geolevel",
                "stop": "INFO: (CUI//SP-CENS)  County"
            },
            "state_query_props": {
                "start": "Sending dp_query_prop to Curve",
                "stop": "State:"
            },
            "county_query_props": {
                "start": "Sending dp_query_prop to Curve",
                "stop": "County:"
            }
        }

        # identifying strings where we're looking for a value that will be on the same line as the identifying string
        self.single_line = {
            "global_scale": "INFO: Global scale",
            "total_budget": "INFO: Total budget",
            "spine": "SPINE=",
            "strategy": "Setting STRATEGY",
            "geolevel_props": "Setting GEOLEVEL_BUDGET_PROP",
            "schema_keyword": "INFO: schema keyword",
            "us_v_pr": "config="
        }


def get_files_list(stdouts=[], cluster_steps=[], zips=[]):
    DASHBOARD_URL=census_getenv("DAS_DASHBOARD_URL")
    stdouts += [f"{DASHBOARD_URL}/app/logs/zip/contents/{t[0]}/steps/{t[1]}/stdout.gz"
                for t in cluster_steps]
    stdouts = [string.replace(f"{DASHBOARD_URL}/app/logs/zip/contents/", DAS_S3ROOT + "-logs/")
               for string in stdouts]

    zips = [string.replace(f"{DASHBOARD_URL}/app/s3/zip/download", DAS_S3ROOT) for string in zips]

    return stdouts + zips


def get_files(files, stdouts_dir, outfile_prefix):
    """
    get files by filenames from s3, move to stdouts dict, and cleanup
    :param files:
    :param stdouts_dir:
    :param outfile_prefix:
    :return: nothing
    """
    # grab files from s3, put in a stdouts dir, and delete original file(s)
    subprocess.run(["mkdir", stdouts_dir])
    subprocess.run(["mkdir", "zips"])
    buckets_keys = [s3.get_bucket_key(file) for file in files]
    for i, filename in enumerate(files):
        # TODO: handle get file error
        print("getting file", i + 1)
        s3.get_object(buckets_keys[i][0], buckets_keys[i][1], filename.split("/")[-1])
        if filename[-3:] == ".gz":
            subprocess.run(["gunzip", 'stdout.gz'])
            subprocess.run(['mv', 'stdout', f'{outfile_prefix}{i}'])
        elif filename[-4:] == ".zip":
            file_name = filename.split("/")[-1][:-4]
            subprocess.run(["unzip", file_name + ".zip", "-d", "./zips"])
            subprocess.run(["mv", "./zips/" + file_name + ".stdout", f'{outfile_prefix}{i}'])
            subprocess.run(["rm", file_name + ".zip"])
    subprocess.run(["rm", "-rf", "./zips"])


def parse_stdout(name, id_strs):
    """
    Read and parse the stdout file given by name, return lines of interest
    :param name: name of file to read
    :return: list of relevant lines, unformatted (epsilon lines bunched together
    """
    # TODO grab spine, strategy, budgets, etc.
    with open(name) as file:
        lines = file.readlines()
    ret_lines = {}

    for i, line in enumerate(lines):
        # get multiline with start/stop strings
        for id in id_strs.multiline:
            if id not in ret_lines and id_strs.multiline[id]["start"] in line:
                print("\t", id, "START")
                lines_str = ""
                j = i
                while id_strs.multiline[id]["stop"] not in lines[j] and j < i + 100:
                    lines_str += " " + lines[j].strip()
                    j += 1
                if j < i + 100:
                    print("\t", id, "DONE")
                    lines_str += " " + lines[j].strip() + " " + lines[j + 1].strip()
                    ret_lines[id] = lines_str
        # get multilines of defined size
        for id in id_strs.n_lines_directly_below:
            if id not in ret_lines and id_strs.n_lines_directly_below[id]["start"] in line:
                print("\t", id, "START")
                lines_str = ""
                for j in range(id_strs.n_lines_directly_below[id]["n"]):
                    lines_str += " " + lines[i + j].strip()
                print("\t", id, "STOP")
                ret_lines[id] = lines_str
        # get single lines where we're looking for a value from a line below
        for id in id_strs.single_line_below:
            if id not in ret_lines and id_strs.single_line_below[id]["start"] in line:
                print("\t", id, "START")
                j = i
                found = False
                while not found and j < i + 100:
                    if id_strs.single_line_below[id]["stop"] in lines[j]:
                        print("\t", id, "DONE")
                        ret_lines[id] = lines[j].strip()
                        found = True
                    j += 1
        # get single lines
        for id in id_strs.single_line:
            if id not in ret_lines and id_strs.single_line[id] in line:
                print("\t", id, "DONE")
                ret_lines[id] = line.strip()

    return ret_lines


def format_parsed_stdout(parsed_stdout):
    """
    from the parsed lines, isolate only the relevant information and return in dict form
    :param parsed_stdout: dict of parsed-out lines from stdout
    :return: dict of relevant data from stdout
    """
    values = {}
    # get votingage_l1 query error
    if "val1qe" in parsed_stdout:
        values["val1qe"] = float(parsed_stdout['val1qe'].split()[-2])
    # get total_l1 query error
    if "tl1qe" in parsed_stdout:
        values["tl1qe"] = float(parsed_stdout['tl1qe'].split()[-2])
    # get single attr/dim epsilons
    if "eps_single" in parsed_stdout:
        split = parsed_stdout["eps_single"].split()
        key = None
        for i, word in enumerate(split):
            if word == "attr/dim":
                key = split[i + 1]
            elif word == "(approx":
                val = split[i + 1]
                if key is not None:
                    values[key] = float(val[:val.find(')')])
                    key = None
    # get geoelevel semantics
    if "eps_gl" in parsed_stdout:
        split = parsed_stdout["eps_gl"].split()
        key = None
        for i, word in enumerate(split):
            if word == "protecting":
                key = "_".join([split[i + j] for j in range(1,4)]).strip(",")
            elif word == "(approx":
                val = split[i + 1]
                if key is not None:
                    values[key] = float(val[:val.find(')')])
                    key = None
    if "gp" in parsed_stdout:
        split = parsed_stdout["gp"].split("INFO: ########################################")
        for line in split:
            if len(line) > 100:
                key = line[line.find(")")+1:line.find("[")]
                key = key.split()
                if key[1] == "gqlevels":
                    key = key[0] + "_" + key[1]
                elif key[1] == "geounit":
                    key = "_".join([key[i] for i in range(3)])
                else:
                    key = key[0] + "_" + key[3]
                values[key] = line[line.find("["):line.find("]")+1]
    if "vacant" in parsed_stdout:
        split = parsed_stdout["vacant"].split("INFO: ########################################")
        for line in split:
            if len(line) > 100:
                key = line[line.find(")")+1:line.find("[")]
                key = key.split()
                if key[1] == "vacant_count":
                    key = key[0] + "_" + key[1]
                elif key[1] == "geounit":
                    key = "_".join([key[i] for i in range(3)])
                values[key] = line[line.find("["):line.find("]")+1]
    if "global_scale" in parsed_stdout:
        split = parsed_stdout["global_scale"].split(":")
        values["global_scale"] = split[-1]
    if "total_budget" in parsed_stdout:
        split = parsed_stdout["total_budget"].split(":")
        values["total_budget"] = split[-1]
    if "spine" in parsed_stdout:
        split = parsed_stdout["spine"].split("=")
        values["spine"] = split[-1]
    if "strategy" in parsed_stdout:
        split = parsed_stdout["spine"].split("=")
        values["strategy"] = split[-1]
    if "geolevel_props" in parsed_stdout:
        split = parsed_stdout["geolevel_props"].split("=")
        values["geolevel_props"] = split[-1]
    if "schema_keyword" in parsed_stdout:
        split = parsed_stdout["schema_keyword"].split(":")
        values["schema_keyword"] = split[-1]
    if "us_v_pr" in parsed_stdout:
        split = parsed_stdout["us_v_pr"].split("/")
        values["us_v_pr"] = split[-1].strip(".ini")
    if "output_files" in parsed_stdout:
        split = parsed_stdout["output_files"].split()
        for word in split:
            if word[:5] == "s3://" and word[-1] == "/":
                values["blocknodedicts"] = word
            elif word[:5] == "s3://" and word[-4:] == ".txt":
                values["mdf_location"] = word
    if "state_query_props" in parsed_stdout:
        split = parsed_stdout["state_query_props"].split("\t")
        values["state_query_props"] = split[-1]
    if "county_query_props" in parsed_stdout:
        split = parsed_stdout["county_query_props"].split("\t")
        values["county_query_props"] = split[-1]


    return values


if __name__ == "__main__":

    # define stdout filenames. example:
    # "https://{HOST_NAME}/app/logs/zip/contents/j-18WF1XUP180X0/steps/s-38VEY96CEUZZ8/stdout.gz",

    stdouts = [
    ]

    # define cluster/step pairs
    cluster_steps = [
        ("j-3EQEINWCN0Q2K", "s-2JDH93P5LFH53"),
        ("j-KW5IBQ6YE8DK", "s-2S0544SVNOZST"),
        ("j-BGLWB6OHR9GH", "s-15BGHZIEUY6B1"),
        ("j-21YJXSJCGFIO7", "s-O3LCLQK4RFLD"),
        ("j-3M8OV4HUBVFKS", "s-2I4AWU87FTQA2"),
        ("j-3M8OV4HUBVFKS", "s-1LDK2RW45W720"),
        ("j-3M8OV4HUBVFKS", "s-313ZLKMRATZB3"),
        ("j-3M8OV4HUBVFKS", "s-EQQGGKIS093W"),
        ("j-2232JSFR0WE0Z", "s-CFF2PGI5MQ71")
    ]

    # define zip files to get. example:
    # "https://{HOST_NAME}/app/s3/zip/download/rpc/upload/logs/DAS-2021-04-09_1042_PLUCKY_ASPECT.zip",
    zips = [
    ]

    # get appropriate urls
    files = get_files_list(stdouts, cluster_steps, zips)

    # get identifying strings for stdout
    id_strs = IdentifyingStrings()

    # get files from s3
    stdouts_dir = "stdouts"
    outfile_prefix = f"./{stdouts_dir}/stdout-"
    get_files(files, stdouts_dir, outfile_prefix)

    # parse files
    parsed_lines = {}
    for i, filename in enumerate(files):
        stdoutname = f"{outfile_prefix}{i}"
        print(filename)
        parsed_lines[filename] = parse_stdout(stdoutname, id_strs)

    print("~~~~")

    # get captured columns (and resulting types) for each file
    for filename, results_dict in parsed_lines.items():
        print(filename)
        for attr, value in results_dict.items():
            print("\t", attr, type(value))

    print("~~~~~")

    # get formatted parsed lines or each file
    final_data = {}
    for filename, results_dict in parsed_lines.items():
        values = format_parsed_stdout(results_dict)
        values["filename"] = filename
        final_data[filename] = values

    # make default value dictionary for all expected columns
    csv_columns_l = [
        "filename", "spine", "strategy", "schema_keyword", "us_v_pr",
        "global_scale", "total_budget", "geolevel_props",
        "state_query_props", "county_query_props",
        "blocknodedicts", "mdf_location",
        "hispanic", "cenrace", "votingage", "hhgq",
        "Block_within_Block_Group", "Block_within_Tract", "Block_within_County", "Block_within_State", "Block_within_US",
        "OSE_hispanic", "Block_Group_hispanic", "Place_hispanic",
        "OSE_gqlevels", "Block_Group_gqlevels", "Place_gqlevels",
        "OSE_vacant_count", "Block_Group_vacant_count", "Place_vacant_count",
        "OSE_geounit_counts", "Block_Group_geounit_counts", "Place_geounit_counts",
        "val1qe", "tl1qe"]

    # go through results dictionaries and add any unanticipated columns
    csv_columns = {key: "" for key in csv_columns_l}
    for filename, results_dict in final_data.items():
        for key in results_dict:
            if key not in csv_columns:
                print(f"\tUNANTICIPATED COLUMN: {key}")
                csv_columns[key] = ''

    # write csv
    with open("captured_data.csv", 'w') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=";", fieldnames=csv_columns)
        writer.writeheader()
        for filename, results_dict in final_data.items():
            writer.writerow(results_dict)

    # delete local files
    subprocess.run(["rm", "-rf", f"./{stdouts_dir}/"])
