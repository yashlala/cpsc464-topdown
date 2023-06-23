from programs.reader.sql_spar_table import SQLSparseHistogramTable


class FromMDFDHCHHouseholdTable(SQLSparseHistogramTable):
    def process(self, data):
        data = data.filter((data.GQTYPE == '000') & (data.VACS == '0'))  # Filter out GQs and vacant units, they are not histogrammable, having None as values
        return super().process(data)