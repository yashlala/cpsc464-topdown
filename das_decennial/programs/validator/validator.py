# mdf to mdf test
#
# Compare input to output


import numpy as np
from pyspark.sql import DataFrame, Row

from das_framework.driver import AbstractDASValidator


class validator(AbstractDASValidator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def validate(self, original_data: DataFrame, written_data: DataFrame) -> bool:
        true_data = original_data.map(lambda node: ((node.geocode,), node.getDenseRaw()))
        noisy_data = written_data.map(lambda node: ((node.geocode,), node.getDenseSyn()))
        blk_error_rdd = true_data.join(noisy_data)

        def L1(pair) -> tuple:
            error = np.sum(np.abs(pair[0]-pair[1]))
            N_true = np.sum(pair[0])
            N_syn = np.sum(pair[1])
            #assert N_true == N_syn, "block population was not correct"
            return (error, 1-.5*error/N_true)
        blk_error_rdd = blk_error_rdd.mapValues(L1)

        def print_eeee(bin) -> Row:
            geocode, (count, john) = bin
            geocode = geocode[0]
            count = int(count)
            john = float(john)
            return Row(geocode=geocode, count=count, tot_var_measure=john)
        error_DF = blk_error_rdd.map(print_eeee).toDF().persist()
        error_DF.describe().show()
        #error_DF.write.csv("blk_error", mode="overwrite")
        return True
