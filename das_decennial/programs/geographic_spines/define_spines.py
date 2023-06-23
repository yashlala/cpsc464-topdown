import numpy as np
from programs.geographic_spines.optimize_spine import call_optimize_prim_spine, call_optimize_non_prim_spine
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from das_constants import CC
from collections import OrderedDict
from fractions import Fraction
import csv

from das_framework.driver import DAS

def create_prims(state, county, place, cousub, cousubfs, placefs, mcd_counties):
    # Each prim geocode has format: [2 digit state FIPS code][3 digit county FIPS code][5 digit county subdivision code][5 digit place code]
    state_county = state + county
    if state == '72':
        # The only primitive geounits in Puerto Rico are equivalent to county:
        return state_county + '9' * 10
    cousub = cousub if state_county in mcd_counties else '9' * 5
    # TODO: Add the following hard-coded tuples to das_constants:
    if (placefs not in ('A', 'B', 'F')) and ((state, place) not in (('22', '36255'), ('11', '50000'), ('15', '71550'), ('20', '71450'))):
        place = '9' * 5
    return state_county + cousub + place


def make_grfc_ids(aian_areas, redefine_counties, grfc_path, aian_ranges_path, strong_mcd_states, prim_crosswalk, use_prim_crosswalk):
    """
    Provides an RDD with rows that are formatted as: (geocode16, (state, aian, county, mcd_or_place, tract, block)),
    where each row corresponds to a block.
    :param aian_areas: a specification of AIANNHCE code groups that should be used to define AIAN areas
    :param redefine_counties: specifies that counties inside of AIAN areas should be redefined as incorporated places or
    MCDs "in_strong_MCDs", "everywhere", or "nowhere"
    :return geoid_dict: dictionary with format {geocode16_geoid:DAS_geoid}
    :return plb_mapping: elements are given by (geocode_in_optimized_spine, PLB_allocation)
    :return geocode_dict: dictionary with format {integer_fixed_width_of_geolevel:name_of_geolevel}
    :param grfc_path: the path of the GRFC
    :param aian_ranges_path: the path of the AIANNHCE range for each catagory of AIAN area
    :param strong_mcd_states: a tuple of the state geoids that are strong MCD states
    :param prim_crosswalk: a dictionary with format {geocode15:prim_geoid}
    :param use_prim_crosswalk: whether to use prim crosswalk provided by Pop or create prim geocodes manually from GRFC

    The recommendation from the Geography Division for the choice parameter aian_areas is:
    Legal_Federally_Recognized_American_Indian_Area,American_Indian_Joint_Use_Area,Hawaiian_Home_Land,
    Alaska_Native_Village_Statistical_Area,State_Recognized_Legal_American_Indian_Area,Oklahoma_Tribal_Statistical_Area,
    Joint_Use_Oklahoma_Tribal_Statistical_Area
    AIAN area types can be converted to AIANNHCE codes using the following intervals.
    Legal_Federally_Recognized_American_Indian_Area: from 0001 to 4799
    American_Indian_Joint_Use_Area: from 4800 to 4989
    Hawaiian_Home_Land: from 5000 to 5499
    Oklahoma_Tribal_Statistical_Area: from 5500 to 5899
    Joint_Use_Oklahoma_Tribal_Statistical_Area: from 5900 to 5999
    Alaska_Native_Village_Statistical_Area: from 6000 to 7999
    Tribal_Designated_Statistical_Area: from 8000 to 8999
    State_Recognized_Legal_American_Indian_Area: from 9000 to 9499
    State_Designated_Tribal_Statistical_Area: from 9500 to 9998
    Note that blocks are assigned an AIANNHCE values of 9999 when they are not in any of these AIAN area categories.
    """
    assert redefine_counties in ['in_strong_MCDs', 'everywhere', 'nowhere'], 'redefine_counties must be in_strong_MCDs, everywhere, or nowhere'
    aian_ranges = make_aian_ranges_dict(aian_ranges_path, aian_areas)

    sc = SparkSession.builder.getOrCreate()

    cols_grfc = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK', 'AIANNHCE', 'COUSUBFP', 'PLACEFP', 'SDUNILEA', 'SDSECLEA', 'SDELMLEA', 'COUSUBFS', 'PLACEFS']
    grfc = sc.read.csv(grfc_path, sep='|', header=True)

    grfc = grfc.select(*cols_grfc)

    mcd_counties = grfc.filter(sf.col('COUSUBFS').isin('A', 'B', 'C')).withColumn('st_cou', sf.concat(sf.col('TABBLKST'), sf.col('TABBLKCOU'))).select('st_cou').distinct().collect()
    mcd_counties = [row_k['st_cou'] for row_k in mcd_counties]

    grfc = grfc.rdd.map(lambda row: (row['TABBLKST'], row['AIANNHCE'], row['TABBLKCOU'], row['PLACEFP'], row['COUSUBFP'], row['TABTRACTCE'], row['TABBLK'], row['SDUNILEA'], row['SDSECLEA'], row['SDELMLEA'], row['COUSUBFS'], row['PLACEFS']))

    grfc = grfc.map(lambda row: (row[0] + row[2] + row[5] + row[6][0] + row[6], geocode_mapping(row, aian_ranges, aian_areas, redefine_counties, strong_mcd_states, mcd_counties)))
    # Format: (geocode16, (state, aian, ['0'][4 digit AIANNHCE] or ['10']+[3 digit county], [5 digit MCD] or [5 digit Place], tract, block, DAS AIAN area code, SD, prim code))

    # Append the primitive geocode onto the values of each row. Note that row[0][:11] + row[0][-4:] is a block geocode in 15 digit format because row[0]
    # is a block geocode in geocode16 format:
    # The following results in each prim geocode being defined as '' when prim_spine=False:
    if use_prim_crosswalk:
        prim_crosswalk = prim_crosswalk if prim_crosswalk is not None else dict()
        grfc = grfc.map(lambda row: (row[0], tuple(row[1])[:-1] + (('' if ((row[0][:11] + row[0][-4:]) not in prim_crosswalk.keys()) else prim_crosswalk[row[0][:11] + row[0][-4:]]),)))
    # Format: (geocode16, (state, aian, ['0'][4 digit AIANNHCE] or ['10']+[3 digit county], [5 digit MCD] or [5 digit Place], tract, block, DAS AIAN area code, SD, prim code))
    return grfc


def make_aian_ranges_dict(aian_ranges_path, aian_areas):
    with open(aian_ranges_path, newline='') as ranges_file:
        aian_ranges = {definition:(lower, upper) for lower, upper, definition in csv.reader(ranges_file, delimiter=',')}
    all_aian_types = list(aian_ranges.keys())
    for user_aian_area in aian_areas:
        assert user_aian_area in all_aian_types, f'Could not find {user_aian_area} in {all_aian_types}. Found instead: {aian_areas}'
    return aian_ranges


def gq_off_spine_entities(ignore_gqs_in_block_groups, gqs):
    """
    Creates a string that can uniquely identify each combination of group quarters (GQs) types (or a subset)
    within a block.

    :return prefix: a string that indicates the combination of GQ types in the block
    """
    if ignore_gqs_in_block_groups:
        return "0000000"
    error_message = f"Array of major GQ types is length {len(gqs)} rather than 7."
    assert len(gqs) == 7, error_message
    return ''.join(["1" if gq > 0 else "0" for gq in gqs])


def geocode_mapping(row, aian_ranges, aian_areas, redefine_counties, strong_mcd_states, mcd_counties):
    """
    Maps an RDD row to a tuple with format (state, AIAN_bool, AIANNHCE, county, place/MCD, tract, block), where
    place/MCD is the five digit MCD in MCD-strong states and 5 digit place otherwise
    AIAN_bool is '1' if the block is inside the AIAN area and '0' otherwise.

    :param row: An RDD row with format (state, AIANNHCE, county, place, MCD, tract, block)
    :param aian_ranges: a dictionary with keys given by the AIAN type and values given by a tuple with two elements
    that indicate the starting and ending AIANNHCE values for the AIAN area catagory.
    :param aian_areas: a specification of AIANNHCE code groups that should be used to define AIAN areas; see also
    make_grfc_ids().
    :param redefine_counties: specifies that counties inside of AIAN areas should be redefined as incorporated places or
    MCDs "in_strong_MCDs", "everywhere", or "nowhere"
    :param strong_mcd_states: a tuple of the state geoids that are strong MCD states

    :return res: a tuple with format (state, AIAN_bool, AIANNHCE, county, place/MCD, tract, block, school_dist)
    """

    state, aiannhce, county, place, cousub, tract, block, school_dist_unified, school_dist_sec, school_dist_elem, cousubfs, placefs = row
    prim = create_prims(state, county, place, cousub, cousubfs, placefs, mcd_counties)

    county = '10' + county
    is_strong_MCD = state in strong_mcd_states
    school_dist = ''.join([school_dist_unified, school_dist_sec, school_dist_elem])

    # The following AIANNHCE values are not in the universe of possible AIANNHCE codes:
    assert aiannhce not in [str(x) for x in range(4990, 5000)], "AIANNHCE codes cannot be between 4990 and 4999"

    if aiannhce == '9999':
        # Not in any of the AIAN area catagories:
        aian = '0'
    else:
        # Check if AIAN area catagory is included in the user's specification of AIAN areas:
        for aian_definition, aian_range in aian_ranges.items():
            if aiannhce <= aian_range[1] and aiannhce >= aian_range[0]:
                aian = '1' if aian_definition in aian_areas else '0'
                # If the user wishes to bypass from the county geounit to the individual AIAN areas, do so here:
                if aian_definition in aian_areas and ((redefine_counties == 'in_strong_MCDs' and is_strong_MCD) or redefine_counties == 'everywhere'):
                    county = '0' + aiannhce
                break

    # An alternative would be to remove the second condition in the next if statement to increase accuracy in MCDs:
    if is_strong_MCD and aian == '0':
        mcd_or_place = cousub
    else:
        mcd_or_place = place

    das_aian_area_code = aiannhce if (aian == '1') else '9999'
    return state, aian, county, mcd_or_place, tract, block, das_aian_area_code, school_dist, prim


def change_width_of_rdd_row(new_widths, old_widths, which_element, row):
    """
    Zero pads the fixed width of each geolevel in a DAS_GEOID to ensure that the number of additional digits required to
    represent each of the geolevels are given by new_widths rather than old_widths.
    :param new_widths: the geolevel widths in the initial DAS_GEOID
    :param old_widths: the geolevel widths in the DAS_GEOID in the output row
    :param which_element: the index of the input row that has the DAS_GEOID
    :param row: an RDD row formatted as (x, DAS_GEOID) if which_element == 1 and (DAS_GEOID, x) if which_element == 0

    :return new_row: an RDD row formatted as (x, new_DAS_GEOID) if which_element == 1 and (new_DAS_GEOID, x) if
    which_element == 0
    """
    # Zero pads the fixed width of each geolevel in a geocode to ensure that the number of additional digits required to
    # represent each of the geolevels are given by new_widths rather than old_widths.
    # new_widths and old_widths are tuples of integers.
    # row is a tuple formatted as: (x_i, DAS_id_i) if which_element == 1 and (DAS_id_i, x_i) if which_element == 0.
    DAS_id = row[which_element]

    message = f'change_width_of_rdd_row can only increase number of additional digits required for a geolevel.'
    for old_width, new_width in zip(old_widths, new_widths):
        assert old_width <= new_width, message

    cur_index = 0
    for k in range(len(old_widths)):
        if len(DAS_id) < (cur_index + old_widths[k]):
            break
        if old_widths[k] < new_widths[k]:
            id_left = DAS_id[:cur_index]
            id_right = DAS_id[cur_index:]
            id_center = '0' * (new_widths[k] - old_widths[k])
            DAS_id = id_left + id_center + id_right
            cur_index += new_widths[k]
        else:
            cur_index += old_widths[k]

    if which_element == 0:
        return DAS_id, row[1]
    return row[0], DAS_id


def call_opt_spine(user_plbs, geocode16, geocode_dict, fanout_cutoff, epsilon_delta, aian_areas, entity_threshold,
                   redefine_counties, bypass_cutoff, grfc_path, aian_ranges_path, strong_mcd_states, target_orig_bgs,
                   target_aians, prim_spine, group_type, prim_crosswalk, target_school_dists, use_prim_crosswalk):
    """
    Calls routines that optimize the spine and PLB alloations. Note that block-groups in the input spine may conform
    with the standard Census definition; however, this is not the case for the spine that is output from this
    function. Instead, these optimization routines redefine these geounits by their optimized counterparts, or
    geounits in the geolevel block-group-custom. (The standard Census definition is defined by:
    [2 digit state][3 digit county][6 digit tract][first digit of block ID].)
    :param user_plbs: The user-specified PLB allocations for each geolevel
    :param geocode16: an RDD containing geoids in geocode16 format of blocks with units
    :param geocode_dict: dictionary with format {integer_fixed_width_of_geolevel:name_of_geolevel}
    :param fanout_cutoff: the fanouts of the block-groups and tract-groups will be no more than
    int(np.sqrt(number_of_tracts)) + fanout_cutoff at the end of the first optimization routine
    :param aian_areas: a specification of AIANNHCE code groups that should be used to define AIAN areas; see also
    make_grfc_ids().
    :param epsilon_delta: True if and only if an approximate DP primitive will be used in the engine
    :param entity_threshold: all entities that have an off-spine entity distance that can be bounded above by
    entity_threshold will be ignored when optimizing over the definition of tract-groups
    :param redefine_counties: specifies that counties inside of AIAN areas should be redefined as incorporated places or
    MCDs "in_strong_MCDs", "everywhere", or "nowhere"
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :param grfc_path: the path of the GRFC
    :param aian_ranges_path: the path of the AIANNHCE range for each catagory of AIAN area
    :param strong_mcd_states: a tuple of the state geoids that are strong MCD states
    :param target_orig_bgs: whether to target accuracy in the original block-groups in spine optimization routines
    :param target_aians: whether to target accuracy in the AIAN areas in spine optimization routines
    :param prim_spine: whether to create the prim_spine (ie: a spine that includes the prim geolevel)
    :param group_type: indicates which method to use to group child geounits to form parent geounit
    :param prim_crosswalk: a dictionary with format {geocode15:prim_geoid}
    :param target_school_dists: whether to target accuracy in school districts in spine optimization routines
    :param use_prim_crosswalk: whether to use prim crosswalk provided by Pop or create prim geocodes manually from GRFC

    :return geoid_dict: dictionary with format {geocode16_geoid:DAS_geoid}
    :return plb_mapping: dictionary with format {geocode_in_optimized_spine:PLB_allocation}
    :return geocode_dict: dictionary with format {integer_fixed_width_of_geolevel:name_of_geolevel}
    """
    for x in user_plbs:
        assert type(x) is Fraction, "user_plbs must be a tuple with elements of class Fraction."

    # We assume the geoids are in geocode16 format and the user wishes to include at least state, county, tract,
    # block-group, and block geounits on the spine. In addition we also assume the user does not include additional
    # geolevels other than these at the county geolevel and below, with the only possible exception of tract-group.
    geocode_dict_keys = list(geocode_dict.keys())
    includes_nation = 0 in geocode_dict_keys

    block_width = CC.DEFAULT_GEOCODE_DICT[CC.GEOLEVEL_BLOCK]
    bg_width = CC.DEFAULT_GEOCODE_DICT[CC.GEOLEVEL_BLOCK_GROUP]
    tract_width = CC.DEFAULT_GEOCODE_DICT[CC.GEOLEVEL_TRACT]
    county_width = CC.DEFAULT_GEOCODE_DICT[CC.GEOLEVEL_COUNTY]
    state_width = CC.DEFAULT_GEOCODE_DICT[CC.GEOLEVEL_STATE]

    # Note that geocode_dict_keys is ordered from block to the root geolevel, but user_plbs is ordered from the root to block geolevel.

    assert state_width in geocode_dict_keys, "State must be included in the geolevels."
    assert county_width in geocode_dict_keys, "County must be included in the geolevels."

    print(f"User precision proportions input to spine optimization routines (should be ordered from root to block): {[float(xi) for xi in user_plbs]}")
    print(f"Geolevel geocode lengths input to spine optimization routines (should be ordered from block to root): {geocode_dict_keys}")

    if prim_spine:
        includes_bg = True
        assert len(geocode_dict_keys) == 7 + int(includes_nation)
    else:
        includes_bg = sum([id_len < block_width and id_len > tract_width for id_len in geocode_dict_keys]) == 1
        assert tract_width in geocode_dict_keys, "Tract must be included in the geolevels."
        assert block_width in geocode_dict_keys, "Block must be included in the geolevels."
        assert includes_bg, "Block-group must be included in the geolevels."

        includes_tg = sum([id_len < tract_width and id_len > county_width for id_len in geocode_dict_keys]) > 0
        assert includes_bg, "Block-group must be included in the geolevels."

    assert len(user_plbs) == len(geocode_dict_keys), "Length of geolevel PLB allocations does not match number of geolevels."
    if (not prim_spine) and (not includes_tg):
        # The optimization routines expect a tract group PLB allocation:
        user_plbs = user_plbs[:-3] + (Fraction(0, 1),) + user_plbs[-3:]
    user_plbs_above_county = user_plbs[:-5] if not prim_spine else user_plbs[:-6]

    # Make user_plbs_above_county_dict corresponding to user_plbs_above_county, getting order from geocode_dict
    sorted_geolevel_names = [geocode_dict[length] for length in sorted(geocode_dict.keys())]
    user_plbs_above_county_dict = {}
    i = 0
    while True:
        user_plbs_above_county_dict[sorted_geolevel_names[i]] = user_plbs_above_county[i]
        i += 1
        if sorted_geolevel_names[i] == CC.GEOLEVEL_COUNTY:
            break

    if prim_spine:
        geolevels = [CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_PRIM, CC.GEOLEVEL_TRACT_SUBSET_GROUP, CC.GEOLEVEL_TRACT_SUBSET, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_BLOCK]
        user_plbs = user_plbs[-6:]
    else:
        geolevels = [CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_TRACT_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_BLOCK]
        user_plbs = user_plbs[-5:]
    user_plb_dict = OrderedDict(zip(geolevels, user_plbs))

    geocode_rdd = make_grfc_ids(aian_areas, redefine_counties, grfc_path, aian_ranges_path, strong_mcd_states, prim_crosswalk, use_prim_crosswalk)
    # Format: (geocode16, (state, aian, county/AIANNHCE, Place/MCD, tract, block, DAS AIAN area code, SD, prim))

    geocode_rdd = geocode16.leftOuterJoin(geocode_rdd)
    # Format: (geocode16, (gqose, (state, AIAN, [county_bool][county/AIANNHCE], Place/MCD, tract, block, DAS AIAN area code, SD, prim)))

    geocode_rdd = geocode_rdd.map(lambda row: (row[0], tuple(row[1][1])[:3] + (row[1][0],) + tuple(row[1][1])[3:]))
    # Format: (geoid, (state, AIAN, [county_bool][county/AIANNHCE], gq_OSE, Place/MCD, tract, block, DAS AIAN area code, SD, prim))

    if use_prim_crosswalk and prim_spine:
        prim_lens = geocode_rdd.map(lambda row: len(row[1][-1])).distinct().collect()
        assert len(prim_lens) == 1, f"Only a proper subset of the block geocodes with HUs or GQs were mapped to primitive geounits."

    if prim_spine:
        # To ignore the Place/MCD OSE in this case:
        geocode_rdd = geocode_rdd.map(lambda row: (row[0], tuple(row[1])[:4] + ("99999",) + tuple(row[1])[5:]))
        # Format: (geoid, (state, AIAN, [county_bool][county/AIANNHCE], gq_OSE, Place/MCD (="99999"), tract, block, DAS AIAN area code, SD, prim))

    if target_school_dists and target_orig_bgs:
        # To avoid school districts and BGs impacting Tract_Group or Tract_Subset_Group geounit definitions in optimize_spine.py,
        # while still impacting optimized Block_Group definitions, define optimized_BG_only_target_OSE as [gq_OSE][SD][tabulation_BG_geoid] in this case:
        geocode_rdd = geocode_rdd.map(lambda row: (row[0], tuple(row[1])[:3] + (row[1][3] + row[1][8] + row[0][:bg_width],) + tuple(row[1])[4:8] + (row[1][9],)))
        # Note that the remaining cases follow a similar pattern; see line below final else condition for format string:
    elif target_school_dists:
        geocode_rdd = geocode_rdd.map(lambda row: (row[0], tuple(row[1])[:3] + (row[1][3] + row[1][8],) + tuple(row[1])[4:8] + (row[1][9],)))
    elif target_orig_bgs:
        geocode_rdd = geocode_rdd.map(lambda row: (row[0], tuple(row[1])[:3] + (row[1][3] + row[0][:bg_width],) + tuple(row[1])[4:8] + (row[1][9],)))
    else:
        geocode_rdd = geocode_rdd.map(lambda row: (row[0], tuple(row[1])[:8] + (row[1][9],)))
    # Format: (geoid, (state, AIAN, [county_bool][county/AIANNHCE], optimized_BG_only_OSE, Place/MCD, tract, block, DAS AIAN area code, prim))

    # In contrast, the definitions for optimized block group, tract group, and tract subset group all depend on the DAS AIAN area code when target_aians is True:
    if target_aians:
        geocode_rdd = geocode_rdd.map(lambda row: (row[1][1] + row[1][0] + row[1][2], (row[1][5], row[0], (row[1][4], row[1][7]), row[1][3], row[1][8])))
        # Format: (state_AIAN_county, (tract, block geocode16, OSEs, optimized_BG_only_OSE, prim)), where OSEs is defined as (Place/MCD, DAS AIAN area code)
    else:
        geocode_rdd = geocode_rdd.map(lambda row: (row[1][1] + row[1][0] + row[1][2], (row[1][5], row[0], (row[1][4],), row[1][3], row[1][8])))
        # Format: (state_AIAN_county, (tract, block geocode16, OSEs, optimized_BG_only_OSE, prim)), where OSEs is defined as (Place/MCD,)

    geocode_rdd = geocode_rdd.groupByKey().persist()
    # Format: (state_county, ((tract, block geocode16, OSEs, gq_OSE), ...))

    geocode_rdd = geocode_rdd.repartition(geocode_rdd.count()).persist()
    DAS.instance.log_and_print("Collecting unique geocodes above county")

    widths_above_county = sorted([gcd_key for gcd_key in geocode_dict_keys if gcd_key < county_width])
    first_width_above_county = widths_above_county[-1]
    # the geolevel corresonding to first_width_above_county will either correspond to state, in which case the new width will be 3, or to a
    # geolevel between state and county:
    new_width = first_width_above_county + 3 if first_width_above_county > state_width else 3

    unique_geocodes_above_county_rdd = geocode_rdd.map(lambda row: row[0][:new_width]).distinct()
    unique_geocodes_above_county     = unique_geocodes_above_county_rdd.collect()
    for width in widths_above_county:
        if width < first_width_above_county:
            if width > state_width:
                new_width = width + 3
            elif width == state_width:
                new_width = 3
            else:
                new_width = width
            unique_geocodes_above_county = unique_geocodes_above_county + [geocode[:new_width] for geocode in unique_geocodes_above_county]
    unique_geocodes_above_county = np.unique(unique_geocodes_above_county).tolist()

    DAS.instance.log_and_print("Starting spine optimization")
    if prim_spine:
        geocode_rdd = geocode_rdd.map(lambda row: call_optimize_prim_spine(row[0], tuple(row[1]), user_plb_dict, fanout_cutoff, epsilon_delta,
                                                                           entity_threshold, bypass_cutoff, group_type)).persist()
    else:
        geocode_rdd = geocode_rdd.map(lambda row: call_optimize_non_prim_spine(row[0], tuple(row[1]), user_plb_dict, fanout_cutoff, epsilon_delta,
                                                                               entity_threshold, bypass_cutoff, includes_tg, group_type)).persist()
    # Format: (geounit_plb_dict, geoid_mapping, widths)

    # Split first two of three outputs of call_optimize_spine into RDDs. Each with format:
    # Format: (widthsi, outputi)
    plb_dicts = geocode_rdd.map(lambda row: ((3,) + row[2], row[0]))
    maps_rdd = geocode_rdd.map(lambda row: ((3,) + row[2], row[1]))
    widths_rdd = geocode_rdd.map(lambda row: np.array(row[2]))

    # We only actually need widths_rdd to create the tuple:
    new_widths = widths_rdd.reduce(lambda row1, row2: np.maximum(row1, row2))

    # Note that we did not include the first geolevel(s) in new_widths (state/nation are not included); add state here:
    new_widths = (3,) + tuple(new_widths)

    plb_dicts = plb_dicts.flatMap(lambda row: [change_width_of_rdd_row(new_widths, row[0], 0, new_row) for new_row in row[1]])
    # Format: (DAS_IDi, PLBi)

    maps_rdd = maps_rdd.flatMap(lambda row: [change_width_of_rdd_row(new_widths, row[0], 1, new_row) for new_row in row[1]])
    # Format: (geocode16i, block_level_DAS_IDi)

    if (not prim_spine) and (not includes_tg):
        # Since each tract group has only one child geounit in this case:
        assert new_widths[3] == 1
        # Remove current tracts, which have a PLB or precision proportion of zero at this point,
        # and then redefine tract groups to be the new tract geolevel:
        width_tract = sum(new_widths[:4])

        plb_dicts = plb_dicts.filter(lambda row: len(row[0]) != width_tract)
        new_widths_prime = new_widths[:3] + (new_widths[4] + 1, new_widths[5])
    else:
        new_widths_prime = new_widths

    # At this point elements of new_widths_prime give the additional geoid digits for each geolevel relative to its parent geolevel;
    # convert this to the absolute geoid widths of each geolevel, ordered from block to state:
    new_widths_prime = np.cumsum(new_widths_prime).tolist()[::-1]

    widths_dict = dict()
    # Add widths for geolevels below county to widths_dict:
    widths_below_county = [gcd_key for gcd_key in geocode_dict_keys if gcd_key >= county_width]
    for cumsum_width, gcd_key in zip(new_widths_prime[:-1], widths_below_county):
        widths_dict[cumsum_width] = geocode_dict[gcd_key]
    # Now add widths for geolevels above county:
    for gcd_key in geocode_dict_keys:
        if gcd_key < state_width:
            widths_dict[gcd_key] = geocode_dict[gcd_key]
        elif gcd_key == state_width:
            widths_dict[gcd_key + 1] = geocode_dict[gcd_key]
        elif gcd_key < county_width:
            widths_dict[gcd_key + 3] = geocode_dict[gcd_key]

    # Transform to dictionaries before return statement:
    full_plb_dict = plb_dicts.collectAsMap()
    full_maps_dict = maps_rdd.collectAsMap()

    # Add geounits above county to full_plb_dict:
    plbs_above_county = dict()
    widths_dict_keys = list(widths_dict.keys())
    widths_above_county = sorted([key_i for key_i in widths_dict_keys if key_i < new_widths_prime[-2]])
    for k, width in enumerate(widths_above_county):
        geocodes = [geocode for geocode in unique_geocodes_above_county if len(geocode) == width]
        geolevel_plb = user_plbs_above_county[k]
        for geocode in geocodes:
            plbs_above_county[geocode] = geolevel_plb
    full_plb_dict.update(plbs_above_county)

    # Check PLBs above county to be corresponding to the ones requested in config (before bypassing)
    # TODO: It is checked against user_plbs_above_county. To be less trivial check, should be checked against budget.geolevel_prop_budgets_dict
    for geocode, budget in plbs_above_county.items():
        level_name = widths_dict[len(geocode)]
        assert budget == user_plbs_above_county_dict[level_name]

    print("Bypassing geounits with only one child above the county geolevel")
    county_geocodes_rdd = plb_dicts.map(lambda row: row[0]).filter(lambda row: len(row) == new_widths_prime[-2]).distinct()
    county_geocodes = list(county_geocodes_rdd.collect())
    widths_at_county_and_above = widths_above_county + [new_widths_prime[-2]]
    full_plb_dict = bypass_above_county(full_plb_dict, widths_at_county_and_above, county_geocodes)

    print(f"Dictionary of DAS GEOID widths after spine optimization is: {widths_dict}")
    geocode_rdd.unpersist()

    return full_maps_dict, full_plb_dict, widths_dict


def bypass_above_county(plb_dict, widths_at_county_and_above, child_geocodes):
    """
    Reallocates child geounit PLB/precision proportions to parent geounits, for each child geounit in the county geolevel and above without siblings.
    :param plb_dict: dictionary with format {geocode_in_optimized_spine:PLB_allocation}
    :param widths_at_county_and_above: DAS GEOID widths for each geolevel at county and above
    :param child_geocodes: a list of county geolevel DAS GEOIDs

    :return plb_dict: dictionary with format {geocode_in_optimized_spine:PLB_allocation}, after reallocating PLB/precision proportions as described above.
    """
    # To start iterations at the county geolevel and work upward, use a reverse sort:
    widths_at_county_and_above = sorted(widths_at_county_and_above, reverse=True)
    for child_width, parent_width in zip(widths_at_county_and_above[:-1], widths_at_county_and_above[1:]):
        parent_geocodes, inds, counts = np.unique([geocode[:parent_width] for geocode in child_geocodes], return_index=True, return_counts=True)
        for parent_geocode, child_index, num_children in zip(parent_geocodes, inds, counts):
            if num_children == 1:
                # When the parent only has one child, reallocate PLB/precision proportion of the child to the parent:
                plb_dict[parent_geocode] = plb_dict[child_geocodes[child_index]] + plb_dict[parent_geocode]
                plb_dict[child_geocodes[child_index]] = Fraction(0, 1)
        # The parent geounits in this iteration will be the child geounits in the next iteration:
        child_geocodes = parent_geocodes
    return plb_dict


def aian_spine(geocode16, widths, aian_areas, redefine_counties, grfc_path, aian_ranges_path, strong_mcd_states):
    """
    Creates the AIAN spine.
    :param geocode16: an RDD containing geoids in geocode16 format of blocks with units
    :param widths: dictionary with format {integer_fixed_width_of_geolevel:name_of_geolevel}
    :param aian_areas: a specification of AIANNHCE code groups that should be used to define AIAN areas; see also
    make_grfc_ids().
    :param redefine_counties: specifies that counties inside of AIAN areas should be redefined as incorporated places or
    MCDs "in_strong_MCDs", "everywhere", or "nowhere"
    :param grfc_path: the path of the GRFC
    :param aian_ranges_path: the path of the AIANNHCE range for each catagory of AIAN area
    :param strong_mcd_states: a tuple of the state geoids that are strong MCD states

    :return geoid_dict: dictionary with format {geocode16_geoid:DAS_geoid}
    :return geocode_dict: dictionary with format {integer_fixed_width_of_geolevel:name_of_geolevel}
    """
    num_partitions = geocode16.getNumPartitions()
    message = f'2 must be included in geolevel lengths'
    assert 2 in list(widths.keys()), message
    new_widths = dict()
    # Change width of state to 3 because an extra digit is required for the binary variable that indicates whether the
    # state-geolevel geounit is inside of an AIAN area or not. Likewise, county geocodes will require 3 extra geocodes
    # because they are now either '10' + [3 digit COUNTY] or '0' + [4 digit AIANNHCE]. Blocks will have an additional
    # width of 16 digits to convert these temporary DAS_GEOIDs back to geocode16 format after the DAS engine is run.
    for num,level in widths.items():
        if num < 2:
            new_widths[num] = level
        elif num < 5:
            new_widths[num + 1] = level
        elif num < 16:
            new_widths[num + 3] = level
        else:
            assert num == 16, "The config option spine:aian_spine assumes block geounits have length 16."
            new_widths[35] = level

    geocode_rdd = make_grfc_ids(aian_areas, redefine_counties, grfc_path, aian_ranges_path, strong_mcd_states)
    # Format: (geocode16, (state, aian, county/AIANNHCE, MCD/Place, tract, block, DAS AIAN area code))

    geocode16 = geocode16.map(lambda row: (row, 1))
    # Format: (geoid, 1)

    geocode_rdd = geocode16.leftOuterJoin(geocode_rdd).repartition(num_partitions)
    # Format: (geoid, (1, (state, AIAN, county/AIANNHCE, Place/MCD, tract, block, DAS AIAN area code)))

    geocode_rdd = geocode_rdd.map(lambda row: (row[0], row[1][1]))
    # Format: (geoid, (state, AIAN, county/AIANNHCE, Place/MCD, tract, block, DAS AIAN area code))

    geocode_rdd = geocode_rdd.map(lambda row: (row[0], row[1][1] + row[1][0] + row[1][2] + row[1][4] + row[1][5][0] + row[1][5] + row[0]))
    # Format: (geocode16, DAS_ID)

    maps_rdd = geocode_rdd.collectAsMap()

    return maps_rdd, new_widths
