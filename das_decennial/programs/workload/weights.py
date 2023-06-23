from das_constants import CC
import programs.workload.census_workloads as census_workloads

def make_weights(weightname, querynames):
    name2func = {
        CC.UNIFORM_WORKLOAD_WEIGHTS : uniform_weights,
        "old_manual_pl94_test": old_manual_pl94_test,
        CC.DEMO_PRODUCTS_WEIGHTS: demo_product_weights,
        "<xl_flag>phil_manual_workload_weights": (lambda x: census_workloads.WeightedWorkloads.<xl_flag>phil_manual_weighted_workload),
    }
    return name2func[weightname](querynames)


def uniform_weights(names):
    w = 1.0/len(names)
    weight_dict = {n : w for n in names}
    return weight_dict


def old_manual_pl94_test(names):
    weight_dict = {'detailed': 0.1,
                   'hhgq': 0.225,
                   'cenrace * hispanic * votingage': 0.675, }
    return weight_dict

def demo_product_weights(names):
    weights = [0.1, .2, .5, 0.05, 0.05, 0.05, 0.05]
    return dict(zip(census_workloads.getWorkloadByKey(CC.DEMO_PRODUCTS_WORKLOAD), weights))
