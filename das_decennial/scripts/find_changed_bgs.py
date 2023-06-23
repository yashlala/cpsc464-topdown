import os, sys
import csv
import numpy as np

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.functions as sf

# columns for each spine will be:
# "geocode16", "gq_ose", "das_geoid", "state", "is_aian", "county", "place_mcd_ose", "tract", "block", "das_aian_code"] + ["das_" + widths_dict[wdth] for wdth in widths])

col_names = ['das_Block_Group', 'das_aian_code', 'place_mcd_ose', 'das_geoid', 'geocode16']
def collect_spine_info(path, entity="das_aian_code", not_an_entity_code="9999"):
    spark = SparkSession.builder.getOrCreate()
    spine_df_init = spark.read.csv(path, header=True).filter(sf.col(entity) != not_an_entity_code).persist()
    spine_df = spine_df_init.select(col_names)
    spine_df.show()
    spine_rdd = spine_df.rdd.map(lambda row: (row['das_Block_Group'], tuple(row[name] for name in col_names[1:])))
    spine_rdd_grouped = spine_rdd.groupByKey().mapValues(list)
    rdd2 = spine_rdd_grouped.map(lambda row: tuple(row[1]))
    res = list(rdd2.collect())
    return res, spine_rdd, spine_df_init

orig_spine, orig_spine_rdd, orig_spine_df = collect_spine_info('${DAS_S3ROOT}/users/user007/spine_from_ppmf05042021.csv')
alt_spine, alt_spine_rdd, alt_spine_df = collect_spine_info('${DAS_S3ROOT}/users/user007/spine_isolating_aians_in_bg05042021.csv')

das_num_aian_areas_orig = [len(np.unique([block[0] for block in bg])) for bg in orig_spine]
bg_hist = np.unique(das_num_aian_areas_orig, return_counts=True)
print(bg_hist)

aians = np.unique(list(orig_spine_rdd.map(lambda row: row[1][0]).collect()))
das_aian_areas_in_bgs_orig = [sum([1 if np.any([block[0] in aian_k for block in bg]) else 0 for bg in orig_spine]) for aian_k in aians]
aian_hist = np.unique(das_aian_areas_in_bgs_orig, return_counts=True)
print(aian_hist)

def make_inputs(spine_df, entity):
    geolevels = ["das_Block", "das_Block_Group", "das_Tract", "das_County", "das_State", "das_US"]
    levels_dict = dict()
    adjacency_dict = dict()
    entity_dict = dict()
    unique_entities = list(spine_df.select(entity).distinct().rdd.map(lambda row: row[entity]).collect())
    for k, geolevel in enumerate(geolevels):
        levels_dict[k] = list(spine_df.select(geolevel).distinct().rdd.map(lambda row: row[geolevel]).collect())
    levels_dict[5] = ['']
    for k in range(1, 6):
        for parent in levels_dict[k]:
            width = len(parent)
            adjacency_dict[parent] = [child for child in levels_dict[k - 1] if child[:len(parent)] == parent]
    blocks_and_entities = list(spine_df.select(["das_Block", entity]).distinct().rdd.map(lambda row: [row["das_Block"], row[entity]]).collect())
    for entity_k in unique_entities:
        entity_dict[entity_k] = [block[0] for block in blocks_and_entities if block[1] == entity_k]
    return adjacency_dict, levels_dict, entity_dict

def find_entity_osed_hist(spine_df, entity="das_aian_code"):
    adjacency_dict, levels_dict, entity_dict = make_inputs(spine_df, entity)
    oseds = [recursion_for_entity_k(adjacency_dict, levels_dict, entity_dict[entity_k]) for entity_k in entity_dict.keys()]
    return np.unique(oseds, return_counts=True)

def recursion_for_entity_k(adjacency_dict, levels_dict, entity_k):
    c_k_all_geos = [dict()]
    c_notk_all_geos = [dict()]
    # Initialize recursive formula described in the Alternative Geographic Spine Document at the block-group geolevel.
    for bg in levels_dict[1]:
        total_in_k = sum([block in entity_k for block in adjacency_dict[bg]])
        total_not_in_k = len(adjacency_dict[bg]) - total_in_k
        c_k_all_geos[0][bg] = min(total_in_k, total_not_in_k + 1)
        c_notk_all_geos[0][bg] = min(total_in_k + 1, total_not_in_k)

    for geolevel in range(2, 6):
        c_k_all_geos.append(dict())
        c_notk_all_geos.append(dict())
        for unit in levels_dict[geolevel]:
            total_in_k = sum([c_k_all_geos[-2][child] for child in adjacency_dict[unit]])
            total_not_in_k = sum([c_notk_all_geos[-2][child] for child in adjacency_dict[unit]])
            c_k_all_geos[-1][unit] = min(total_in_k, total_not_in_k + 1)
            c_notk_all_geos[-1][unit] = min(total_in_k + 1, total_not_in_k)
    return c_k_all_geos[-1]['']

with open('aian_oseds_orig_spine.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    xs, counts = find_entity_osed_hist(orig_spine_df)
    for x, count in zip(xs, counts):
        writer.writerow([x, count])
with open('aian_oseds_alt_spine.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    xs, counts = find_entity_osed_hist(alt_spine_df)
    for x, count in zip(xs, counts):
        writer.writerow([x, count])
# Note that we only find the impact on portions of Places that are within the AIAN portion of the spine, partly for computational feasibility.
orig_spine_df = orig_spine_df.filter(sf.col("place_mcd_ose") != "99999")
alt_spine_df = alt_spine_df.filter(sf.col("place_mcd_ose") != "99999")
with open('pme_oseds_orig_spine.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    xs, counts = find_entity_osed_hist(orig_spine_df, "place_mcd_ose")
    for x, count in zip(xs, counts):
        writer.writerow([x, count])
with open('pme_oseds_alt_spine.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    xs, counts = find_entity_osed_hist(alt_spine_df, "place_mcd_ose")
    for x, count in zip(xs, counts):
        writer.writerow([x, count])
exit(0)
