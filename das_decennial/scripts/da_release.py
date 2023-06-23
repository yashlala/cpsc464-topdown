"""
Script for producing protected Sex by Single-Year-Age US level table for Demographic Analysis
"""


import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# The following imports were only needed for the function that reads 2010 full CEF from S3 and aggregates them using Spark
# if 'SPARK_HOME' not in os.environ:
#     os.environ['SPARK_HOME'] = '/usr/lib/spark'
# sys.path.append(os.path.join(os.environ['SPARK_HOME'],'python'))
# sys.path.append(os.path.join(os.environ['SPARK_HOME'],'python','lib', 'py4j-src.zip'))
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import when, lit
# spark = SparkSession.builder.getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")
# sc = spark.sparkContext

import numpy as np
import pandas as pd
from fractions import Fraction
import datetime
import pwd
import psutil
from programs.engine.primitives import RationalDiscreteGaussianMechanism

AGE = 'age'
SEX = 'sex'
PROTECTED = 'protected'
NOISY = 'noisy'
PROTECTED_UNROUNDED = 'protected_unrounded'
TABLE_SIZE = 86 * 2

RHO = Fraction(1, 500)

# def getDataFromFull2010CEF():
#     df = spark.read.csv('${DAS_S3INPUTS}/title13_input_data/table13/', header="True", sep="\t")
#     df = df.select(
#         [
#             when(df.QAGE>84,85).otherwise(df.QAGE).cast('int').alias('age'),
#             (df.sex+lit(1)).cast('int').alias('sex'),
#         ])\
#         .groupby('age', 'sex').count().alias('cef').orderBy('age').toPandas()
#     df.columns = ['age', 'sex', 'cef']
#     df.to_csv('da_sp_tab_2010_input.csv', index_label=False, index=False)
#     return df


def getDataFromSAS():
    return pd.read_sas('demo_cef_2020.sas7bdat').astype(int)


def getDataFromCSV(name):
    return pd.read_csv(name)


def getDGMNoise(table_size, rho):
    inverse_scale = Fraction(rho)
    return RationalDiscreteGaussianMechanism(inverse_scale, np.zeros(table_size, dtype=int)).protected_answer


def project(vector, new_mean):
    # L2 problem is the following (in Latex format):
    # \min_{y\in R^{table_size}} \sum_{i} (vector[i] - y[i])^2 such that:
    # \sum_{i} y[i] = new_mean * table_size

    # The solution is given by:
    return vector.values + new_mean - vector.mean()


# def round2one(table_size, vector, new_mean):
#     # Round to nearest integer while maintaining total sum invariant:
#     vector = np.around(vector)
#     new_sum = new_mean * table_size
#
#     indices = list(range(table_size))
#     np.random.shuffle(indices)
#
#     for k in indices:
#         if vector.sum() < new_sum:
#             vector[k] += 1
#         elif vector.sum() > new_sum:
#             vector[k] -= 1
#         else:
#             break
#
#     return vector.astype(int)


def round_to_1000(vector):
    # Round to nearest thousand and return answer as vector of ints:
    return np.around(vector, -3).astype(int)


if __name__ == '__main__':
    starttime = datetime.datetime.now().isoformat()
    # df = getDataFromFull2010CEF()
    df = getDataFromCSV('demo_cef_2020.csv')

    # The following is access to CEF (i.e. raw data), and all access to CEF should be noted and commented upon in a formally private DAS.
    # Since we have specified the agreed upon format of the input file, where zeros are explicitly listed (rather than omitted and then inputed),
    # in this case this access to CEF does not spend privacy.
    # If the shape (number of rows) in the table were possibly reflective of the data (e.g. zero cells would result in less rows), this could have
    # theoretically led to unaccounted-for spending of budget, depending on the human actions in response to failure of the assert.
    assert df.shape[0] == TABLE_SIZE

    df[NOISY] = df.cef + getDGMNoise(TABLE_SIZE, RHO)
    df[PROTECTED_UNROUNDED] = project(df[NOISY], df.cef.mean())
    # df['protected_rounded1'] = round2one(TABLE_SIZE, df[PROTECTED_UNROUNDED], df.cef.mean())
    df[PROTECTED] = round_to_1000(df[PROTECTED_UNROUNDED])
    for dataname in [NOISY, PROTECTED_UNROUNDED, PROTECTED]:
        with open(f'da_sp_tab_{dataname}_rho{RHO.numerator}over{RHO.denominator}.csv', 'w') as f:
            f.write("# DEMOGRAPHIC ANALYSIS SPECIAL TABULATION\n")
            f.write(f"# Classification: "+"C"+"U"+"I"+"//SP-CENS\n")
            f.write("# Created: {}\n".format(datetime.datetime.now().isoformat()))
            f.write("# Records: {}\n".format(df[dataname].count()))
            f.write("# Command line: {}\n".format(sys.executable + " " + " ".join(sys.argv)))
            f.write("# uid: {}\n".format(os.getuid()))
            f.write("# username: {}\n".format(pwd.getpwuid(os.getuid())[0]))
            f.write("# Boot Time: {}\n".format(datetime.datetime.fromtimestamp(psutil.boot_time()).isoformat()))
            f.write("# Start Time: {}\n".format(starttime))
            # f.write(f"# Git Repo Info:{}\n")
            f.write(f"# PLB allocation rho={RHO.numerator}/{RHO.denominator} ({float(RHO):.5f})\n")
            uname = os.uname()
            uname_fields = ['os_sysname', 'host', 'os_release', 'os_version', 'arch']
            for i in range(len(uname_fields)):
                f.write("# {}: {}\n".format(uname_fields[i], uname[i]))
            df.filter([AGE, SEX, dataname]).to_csv(f, index=False)
