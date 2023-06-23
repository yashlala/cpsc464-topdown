#!/usr/bin/env python3
import collections
import sys
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from das_framework.ctools.hierarchical_configparser import HierarchicalConfigParser
from configparser import ConfigParser



deliminator = '`'

# execute from top to down
scrub_strings = \
    [
     ("s3://uscb-decennial-ite-das", "${DAS_S3ROOT}"),                                      # S3 Root
     ("s3://v-s3-das-ite-tda-devtest", "${DAS_S3ROOT}"),         # S3 Root
     ("s3://v-s3-das-ite-sourcedata", "${DAS_S3INPUTS}"),        # S3 Inputs
     ("s3://v-s3-das-common-drps", "${DAS_S3MGMT}"),             # S3 MGMT
     ("s3://v-s3-das-ite-logs", "${DAS_S3LOGS}"),                # S3 Logs

        # Scrub GRFC
     ("${DAS_S3INPUTS}/2010-convert/grfc/grfc_tab20_*.txt", "${GRFC_US_PATH}"),             # GRFC_US_PATH

        # Scrube Gurobi
     ("/usr/local/lib64/python3.6/site-packages/gurobipy/gurobi_client.lic", "$GUROBI_HOME/gurobi_client.lic"),
     ("/usr/local/lib64/python3.6/site-packages/gurobipy/linux64/lib/python3.7_utf32/", "$GUROBI_HOME/linux64/lib/${PYTHON_VERSION}_utf32/"),
     ("{HOST_NAME}", "${GRB_TOKENSERVER}"),
     ("{HOST_NAME}", "${GRB_TOKENSERVER}"),
     ("{PORT}", "${GRB_TOKENSERVER_PORT}")
    ]


def configparser_to_set(config):
    section_option_value_list = []
    for section in config.sections():
        for option in config[section]:
            section_option_value_list.append(f"{section}{deliminator}{option}{deliminator}{config[section][option]}")

    return set(section_option_value_list)

def set_to_configparser(section_option_value_list):
    config = ConfigParser()

    for sovl in section_option_value_list:
        [sec, opt, val] = sovl.split(deliminator)
        if sec not in config.sections():
            config.add_section(sec)
        config[sec][opt]=val
    return config

if __name__=="__main__":
    """Configuration Tool Main"""

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("--compare", action='store_true', help="compare 2 configuration files")
    parser.add_argument("--dump", action='store_true', help="Dumps the given files to the console one at a time. (i.e. config flattening tool)")
    parser.add_argument("--scrub", action='store_true', help="Replaces give strings with env variable strings.")
    parser.add_argument("--delete", type=str, help="delete variable from given config files. given format 'section:option'")
    parser.add_argument("--set", type=str, help="set variable with value. given format 'section:option:value' in the given config file")
    parser.add_argument("--sort", action='store_true', help="Rewrite the given config files as a sorted configuration file.  Just for ease of use.")

    args, unknownargs = parser.parse_known_args()

    if len(unknownargs) == 0:
        print("Please provide files for analysis")

    if args.compare:
        configs_sets = []

        for arg in unknownargs:
            unique = HierarchicalConfigParser()
            if not os.path.exists(arg):
                raise RuntimeError("{} does not exist".format(arg))
            else:
                print(f"Processing File: {arg} ...")

            config = HierarchicalConfigParser()
            config.read(arg)

            configs_sets.append(configparser_to_set(config))


        # Use some magic with sets to find common elements, and unique elements per list
        matching_elements = configs_sets[0].copy()
        for s in configs_sets:
            matching_elements.intersection_update(s)
        differences = []
        # Generate a list of sets of elements that contain only the elements that are NOT present in both sets, which when
        # combined with matching elements, will result in a set of items that are not the matching elements.
        for s in configs_sets:
            differences.append(matching_elements.symmetric_difference(s))


        # Print out matching parameter sets
        print("Matching Parameters:")
        print("---------------------------------------------------------------------------------------------------------------------------")
        config = set_to_configparser(matching_elements)
        config.write(sys.stdout)
        with open("common.ini", "w") as fp:
            config.write(fp)

        # Print out differences
        for idx, diff in enumerate(differences):
            print(f"\n\nUnique Parameters [{unknownargs[idx]}]")
            print("---------------------------------------------------------------------------------------------------------------------------")
            if len(diff) == 0:
                print("No Differences found")
            else:
                config = set_to_configparser(diff)
                config.write(sys.stdout)
                with open(f"{unknownargs[idx]}.unique", "w") as fp:
                    config.write(fp)

    elif args.dump:
        # load files
        for arg in unknownargs:
            if not os.path.exists(arg):
                raise RuntimeError("{} does not exist".format(arg))

            config = HierarchicalConfigParser()
            config.read(arg)
            print("Flattened Config File:")
            print("-----------------------------------------------------------------------------")
            for section in config.sections():
                for option in config[section]:
                    print(f"[{section}][{option}]:{config[section][option]}")
    elif args.scrub:
        for idx, arg in enumerate(unknownargs):
            unique = HierarchicalConfigParser()
            if not os.path.exists(arg):
                raise RuntimeError("{} does not exist".format(arg))
            else:
                print(f"Processing File: {arg} ...")

            config = HierarchicalConfigParser()
            config.read(arg)

            scrubbed_list=[]
            config_list = list(configparser_to_set(config))
            for conf in config_list:
                scrub_conf = conf
                for scrub in scrub_strings:
                    scrub_conf = scrub_conf.replace(scrub[0], scrub[1])
                scrubbed_list.append(scrub_conf)
            config = set_to_configparser(set(scrubbed_list))

            with open(f"{unknownargs[idx]}.scrubbed", "w") as fp:
                config.write(fp)
    elif args.delete:
        if len(args.delete.split(':')) != 2:
            raise RuntimeError("Invalid formatting.  Expected <section>:<option>")
        [sec, opt] = args.delete.split(':')
        print(f"Deleting [{sec}][{opt}]")
        for arg in unknownargs:
            if not os.path.exists(arg):
                raise RuntimeError("{} does not exist".format(arg))

            print(f"Processing File: {arg} ...")

            config = ConfigParser()
            config.read(arg)

            config.remove_option(sec,opt)

            with open(f"{arg}", "w") as fp:
                config.write(fp)
    elif args.set:
        if len(args.set.split(':')) != 3:
            raise RuntimeError("Invalid formatting.  Expected <section>:<option>:<value>")
        [sec, opt, val] = args.set.split(':')
        print(f"Setting Value [{sec}][{opt}]")
        for arg in unknownargs:
            if not os.path.exists(arg):
                raise RuntimeError("{} does not exist".format(arg))

            print(f"Processing File: {arg} ...")

            config = ConfigParser()
            config.read(arg)


            if sec not in config.sections():
                config.add_section(sec)
            config[sec][opt] = val
            with open(f"{arg}", "w") as fp:
                config.write(fp)
    elif args.sort:
        for arg in unknownargs:
            if not os.path.exists(arg):
                raise RuntimeError("{} does not exist".format(arg))

            print(f"Sorting File: {arg} ...")

            config = ConfigParser({}, collections.OrderedDict)
            config.read(arg)

            # Order the content of each section alphabetically
            for section in config._sections:
                config._sections[section] = collections.OrderedDict(sorted(config._sections[section].items(), key=lambda t: t[0]))

            # Order all sections alphabetically
            config._sections = collections.OrderedDict(sorted(config._sections.items(), key=lambda t: t[0]))

            with open(f"{arg}", "w") as fp:
                config.write(fp)
    else:
        print("User must specify compare or dump")
