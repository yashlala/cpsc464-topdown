from programs.reader.sql_spar_table import SQLSparseHistogramTable


class CVAPTable(SQLSparseHistogramTable):
    """
    This is a class that reads CVAP table and converts it from Spark DataFrame with rows corresponding to records
    (person records, or household records, or housing unit records) into RDD with histograms (as sparse matrices)
    keyed by geocode, using Spark SQL up until creation of scipy.sparse dok matrices representing the histograms.

    In this case, every record has a `pcitizen` variable which is probability that the record represents a citizen.
    To count citizens-of-voting-age population (CVAP) then, the probabilities are summed within the category and rounded
    """

    # Instead of just counting records with COUNT(1), we sum the citizenship probabilities within the category and round the sum
    REC_COUNT_SQL_FXN = 'CAST(ROUND(SUM(pcitizen)) AS INT)'
