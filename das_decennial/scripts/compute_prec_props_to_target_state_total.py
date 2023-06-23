import numpy as np
from fractions import Fraction

target_geo_index = 2

def compute_new_props(orig_global_scale, orig_query_props, orig_geo_props, iteration=0, new_target_geo_total_prec=None, denom_limit=1024, do_print=True):
    # This function assumes that original query proportion for total is orig_query_props[0]
    # and that original geolevel proportion for target_geo is: ogirg_geo_props[target_geo_index]
    assert np.abs(sum(orig_query_props) - 1) < 1e-5
    assert np.abs(sum(orig_geo_props) - 1) < 1e-5

    orig_prec = 1 / orig_global_scale ** 2
    orig_target_geo_total_prec = orig_geo_props[target_geo_index] * orig_query_props[0] * orig_prec

    # Assume we double precision of the total query at the target_geo geolevel until we meet the error target:
    new_target_geo_total_prec = orig_target_geo_total_prec * (2 ** iteration) if new_target_geo_total_prec is None else new_target_geo_total_prec

    orig_target_geo_geolevel_prec = orig_geo_props[target_geo_index] * orig_prec
    new_target_geo_geolevel_prec = orig_target_geo_geolevel_prec - orig_target_geo_total_prec + new_target_geo_total_prec
    new_prec = orig_prec - orig_target_geo_total_prec + new_target_geo_total_prec

    # If geolevel is not target_geo, set new_geo_props so that:
    # new_geo_prop * new_prec == orig_geo_prop * orig_prec,
    # and, if geolvel is a target_geo, so that:
    # new_geo_prop * new_prec == new_target_geo_geolevel_prec.
    new_geo_props = [g_prop * orig_prec / new_prec if k != target_geo_index else new_target_geo_geolevel_prec / new_prec for k, g_prop in enumerate(orig_geo_props)]
    assert orig_target_geo_total_prec <= new_target_geo_total_prec
    assert np.abs(sum(new_geo_props) - 1) < 1e-5, f"{sum(new_geo_props)} \n {new_geo_props}"

    # If query is not total, set new_target_geo_query_props so that:
    # new_target_geo_query_prop * new_target_geo_geolevel_prec == orig_query_prop * orig_prec * orig_geo_props[target_geo_index],
    # and, if query is total, so that:
    # new_target_geo_query_prop * new_target_geo_geolevel_prec == new_target_geo_total_prec.
    new_target_geo_query_props = [q_prop * orig_prec * orig_geo_props[target_geo_index] / new_target_geo_geolevel_prec if k != 0 else new_target_geo_total_prec / new_target_geo_geolevel_prec for k, q_prop in enumerate(orig_query_props)]
    assert np.abs(sum(new_target_geo_query_props) - 1) < 1e-5, f"{sum(new_target_geo_query_props)} \n {new_target_geo_query_props}"

    new_global_scale = Fraction(np.sqrt(1/new_prec)).limit_denominator(denom_limit)

    # round results:
    new_target_geo_query_props = [int(np.round(x * denom_limit)) for x in new_target_geo_query_props]
    new_geo_props = [int(np.round(x * denom_limit)) for x in new_geo_props]

    new_target_geo_query_props = [x if x >= 1 else 1 for x in new_target_geo_query_props]
    new_geo_props = [x if x >= 1 else 1 for x in new_geo_props]

    ind = np.argmax(new_target_geo_query_props)
    new_target_geo_query_props[ind] = denom_limit - sum([x for k, x in enumerate(new_target_geo_query_props) if k != ind])
    ind = np.argmax(new_geo_props)
    new_geo_props[ind] = denom_limit - sum([x for k, x in enumerate(new_geo_props) if k != ind])

    new_target_geo_query_props = [Fraction(x, denom_limit) for x in new_target_geo_query_props]
    new_geo_props = [Fraction(x, denom_limit) for x in new_geo_props]

    new_target_geo_total_prec = float(new_geo_props[target_geo_index] * new_target_geo_query_props[0] / new_global_scale**2)

    assert sum(new_geo_props) == 1
    assert sum(new_target_geo_query_props) == 1

    if do_print:
        print(f"set new value of global scale to: {new_global_scale}")
        print(f"set new geolevel proportions to: {new_geo_props}")
        print(f"For target_geo geolevel only, set new query proportions to {new_target_geo_query_props}")

    return new_global_scale, new_target_geo_query_props, new_geo_props, new_target_geo_total_prec

def print_query_props(input_list):
    return str([f'{val.numerator}/{val.denominator}' for val in input_list]).replace(' ', '').replace('[', '').replace(']', '').replace('\'', '').replace(',', ' ')

def print_geolevel(input_list):
    return str([f'{val.numerator}/{val.denominator}' for val in input_list]).replace(' ', '').replace('[', '').replace(']', '').replace('\'', '').replace(',', ' ')


state_candidates = {
    'opt_h1': {
        "orig_query_props":[1],
        "orig_geo_props" : [1/1024,1/1024,9/1024,76/1024,914/1024,23/1024],
        "state_queries_prop": [1],
        "orig_global_scale": 4681335625/993509888,
    },
    'aian_h1': {
        "orig_query_props":[1],
        "orig_geo_props" : [1/1024,1/1024,6/1024,58/1024,119/1024,839/1024],
        "state_queries_prop": [1],
        "orig_global_scale": 3558805/713216,
    },
    # 'opt_1a': {
    #     "orig_query_props":[1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11],
    #     "orig_geo_props" : [31/1024,207/1024,31/1024,31/1024,143/1024,581/1024],
    #     "state_queries_prop": [221/256,7/512,7/512,7/512,7/512,7/512,7/512,7/512,7/512,7/512,7/512],
    #     "orig_global_scale": 733/584,
    # },
    # 'aian_1a': {
    #     "orig_query_props":[1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11, 1/11],
    #     "orig_geo_props" : [9/256,119/512,9/256,9/256,51/512,9/16],
    #     "state_queries_prop": [221/256,7/512,7/512,7/512,7/512,7/512,7/512,7/512,7/512,7/512,7/512],
    #     "orig_global_scale": 893/744,
    # },
    # 'opt_2a': {
    #     "orig_query_props":[1/7, 1/7, 1/7, 1/7, 1/7, 1/7, 1/7],
    #     "orig_geo_props" : [29/1024,153/1024,29/1024,29/1024,85/1024,699/1024],
    #     "state_queries_prop": [431/512,27/1024,27/1024,27/1024,27/1024,27/1024,27/1024],
    #     "orig_global_scale": 977/910,
    # },
    # 'aian_2a': {
    #     "orig_query_props":[1/7, 1/7, 1/7, 1/7, 1/7, 1/7, 1/7],
    #     "orig_geo_props" : [21/512,227/1024,21/512,21/512,91/1024,145/256],
    #     "state_queries_prop": [431/512,27/1024,27/1024,27/1024,27/1024,27/1024,27/1024],
    #     "orig_global_scale": 589/473,
    # },
    # 'opt_1b': {
    #     "orig_query_props":[1/1024, 1/512, 1/1024, 1/1024, 1/1024, 1/1024, 5/1024, 5/1024, 1/1024, 17/1024, 989/1024],
    #     "orig_geo_props" : [29/512,173/1024,29/512,29/512,43/512,591/1024],
    #     "state_queries_prop": [339/512,1/1024,1/1024,1/1024,1/1024,1/1024,1/512,1/512,1/1024,3/512,165/512],
    #     "orig_global_scale": 293/260,
    # },
    # 'aian_1b': {
    #     "orig_query_props":[1/1024, 1/512, 1/1024, 1/1024, 1/1024, 1/1024, 5/1024, 5/1024, 1/1024, 17/1024, 989/1024],
    #     "orig_geo_props" : [79/1024,237/1024,79/1024,79/1024,29/256,217/512],
    #     "state_queries_prop": [339/512,1/1024,1/1024,1/1024,1/1024,1/1024,1/512,1/512,1/1024,3/512,165/512],
    #     "orig_global_scale": 514/521,
    # },
    # 'opt_2b': {
    #     "orig_query_props":[1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1018/1024],
    #     "orig_geo_props" : [55/1024,275/1024,55/1024,55/1024,51/512,241/512],
    #     "state_queries_prop": [815/1024,1/1024,1/1024,1/1024,1/1024,1/1024,51/256],
    #     "orig_global_scale": 124/113,
    # },
    # 'aian_2b': {
    #     "orig_query_props":[1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1018/1024],
    #     "orig_geo_props" : [97/1024,193/1024,97/1024,97/1024,111/1024,429/1024],
    #     "state_queries_prop": [509/1024,1/1024,1/1024,1/1024,1/1024,1/1024,255/512],
    #     "orig_global_scale": 27/26,
    # },
    # 'production/pl94/experiments_dec2020/dynamic_geolevel/user007_strategy_3_2aTotIso': [1/7, 1/7, 1/7, 1/7, 1/7, 1/7, 1/7],
    # 'production/pl94/experiments_dec2020/dynamic_geolevel/user007_strategy_3_1bTotIso': [1/1024, 1/512, 1/1024, 1/1024, 1/1024, 1/1024, 5/1024, 5/1024, 1/1024, 17/1024, 989/1024],
    # 'production/pl94/experiments_dec2020/dynamic_geolevel/user007_strategy_3_2bTotIso': [1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1018/1024]
}


if __name__ == "__main__":

    print(f"Configuration, Iteration, State Query Proportions, Global Scale, Geolevel Proportions, County Query Proportions")

    for candidate, params in state_candidates.items():
        orig_global_scale = params['orig_global_scale']
        orig_query_props = params['orig_query_props']
        orig_geo_props = params['orig_geo_props']

        state_queries_prop = [Fraction(prop) for prop in params['state_queries_prop']]
        for iteration in range(11):
            new_global_scale, new_target_geo_query_props, new_geo_props, new_target_geo_total_prec = compute_new_props(orig_global_scale, orig_query_props, orig_geo_props, iteration=iteration, do_print=False)

            print(f'"{candidate}","{iteration}","{print_query_props(state_queries_prop)}","{new_global_scale}","{print_geolevel(new_geo_props)}","{print_query_props(new_target_geo_query_props)}"')

        print('###################################################################################')
