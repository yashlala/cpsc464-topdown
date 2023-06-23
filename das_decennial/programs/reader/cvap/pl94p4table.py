from programs.reader.sql_spar_table import SQLSparseHistogramTable


class PL94P4Table(SQLSparseHistogramTable):
    """
    This is a class that reads PL94 MDF and converts it from Spark DataFrame with rows corresponding to records
    keyed by geocode, using Spark SQL up until creation of scipy.sparse COO matrices representing the histograms.

    In this case, records are filtered to people of voting age, i.e. where VOTING_AGE = '2'
    """

    # Instead of just counting records with COUNT(1), we filter out only the voting age people
    REC_COUNT_SQL_FXN = 'SUM(CAST(VOTING_AGE=2 AS INT))'
