"""
Creates several writers and runs them to output in several formats
"""
from typing import List

from programs.writer.mdf2020writer import MDF2020PersonWriter, MDF2020PersonAnyHistogramWriter, MDF2020HouseholdWriter, MDF2020HouseholdAnyHistogramWriter, DHCP_MDF2020_Writer, MDF2020PersonPL94HistogramWriter, MDF2020H1HistogramWriter, CVAPWriter
from programs.writer.mdf2020writerRevisedColumns import MDF2020PersonWriterRevisedColumnNames, MDF2020PersonAnyHistogramWriterRevisedColumnNames, MDF2020HouseholdWriterRevisedColumnNames, MDF2020HouseholdAnyHistogramWriterRevisedColumnNames, DHCP_MDF2020_WriterRevisedColumnNames, MDF2020PersonPL94HistogramWriterRevisedColumnNames, MDF2020H1HistogramWriterRevisedColumnNames, CVAPWriterRevisedColumnNames
from programs.writer.cef_2020.dhch_to_mdf2020_writer import DHCH_MDF2020_Writer, MDF2020HouseholdWriterFullTenure
from programs.writer.cef_2020.dhch_to_mdf2020_writer_revised_columns import DHCH_MDF2020_WriterRevisedColumnNames, DHCH_MDF2020_WriterRevisedColumnNamesTen3Lev, MDF2020HouseholdWriterFullTenureRevisedColumnNames
from programs.writer.pickled_block_data_writer import PickledBlockDataWriter
from programs.writer.ipums_1940.ipums_1940_writer import IPUMSPersonWriter
from programs.writer.h1writer import H1Writer
from programs.writer.cvap_writer import CVAP_Writer as CVAPCountsWriter
from programs.writer.block_node_writer import BlockNodeWriter
from programs.writer.writer import DASDecennialWriter
from exceptions import DASConfigError

from das_constants import CC

from pyspark import RDD


class MultiWriter(DASDecennialWriter):

    SUPPORTED_WRITERS = {
        'MDFPersonAnyRevisedColumnNames': MDF2020PersonAnyHistogramWriterRevisedColumnNames,
        'MDFPL942020RevisedColumnNames': MDF2020PersonPL94HistogramWriterRevisedColumnNames,
        'DHCP_MDFRevisedColumnNames': MDF2020PersonWriterRevisedColumnNames,
        'DHCP2020_MDFRevisedColumnNames': DHCP_MDF2020_WriterRevisedColumnNames,
        'MDFHouseholdAnyRevisedColumnNames': MDF2020HouseholdAnyHistogramWriterRevisedColumnNames,
        'MDF2020H1RevisedColumnNames': MDF2020H1HistogramWriterRevisedColumnNames,
        'DHCH_MDFRevisedColumnNames': MDF2020HouseholdWriterRevisedColumnNames,
        'DHCH2020_MDFRevisedColumnNames': DHCH_MDF2020_WriterRevisedColumnNames,
        'DHCH2020_MDFRevisedColumnNamesTen3Lev': DHCH_MDF2020_WriterRevisedColumnNamesTen3Lev,
        'DHCH2020_MDF_FullTenureRevisedColumnNames': MDF2020HouseholdWriterFullTenureRevisedColumnNames,
        'CVAPRevisedColumnNames': CVAPWriterRevisedColumnNames,

        'MDFPersonAny': MDF2020PersonAnyHistogramWriter,
        'MDFPL942020': MDF2020PersonPL94HistogramWriter,
        'DHCP_MDF': MDF2020PersonWriter,
        'DHCP2020_MDF': DHCP_MDF2020_Writer,
        'MDFHouseholdAny': MDF2020HouseholdAnyHistogramWriter,
        'MDF2020H1': MDF2020H1HistogramWriter,
        'DHCH_MDF': MDF2020HouseholdWriter,
        'DHCH2020_MDF': DHCH_MDF2020_Writer,
        'DHCH2020_MDF_FullTenure': MDF2020HouseholdWriterFullTenure,
        'BlockNodes': BlockNodeWriter,
        'BlockNodeDicts': PickledBlockDataWriter,
        '1940Persons': IPUMSPersonWriter,
        'H1': H1Writer,
        'CVAP': CVAPWriter,
        'CVAPCounts': CVAPCountsWriter
    }

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.writer_names = self.gettuple(CC.MULTIWRITER_WRITERS)
        self.writers = []
        for i, wname in enumerate(self.writer_names):
            try:
                writer_class = self.SUPPORTED_WRITERS[wname]
            except KeyError:
                raise DASConfigError(f"Writer {wname} is not supported in MultiWriter", CC.MULTIWRITER_WRITERS, CC.WRITER)
            postfix = self.getconfig(CC.OUTPUT_DATAFILE_NAME, default='data')
            w = writer_class(name=CC.WRITER, config=self.config, setup=self.setup, das=self.das)
            assert isinstance(w, DASDecennialWriter)

            w.setOutputFileDataName(f"{postfix}-{wname}")
            if i > 0:
                w.unsetOverwriteFlag()
            self.writers.append(w)

    def setOutputFileDataName(self, name: str):
        for i, w in enumerate(self.writers):
            assert isinstance(w, DASDecennialWriter)
            w.setOutputFileDataName(f"{name}-{self.writer_names[i]}")
            w.unsetOverwriteFlag()

    def unsetOverwriteFlag(self) -> None:
        for i, w in enumerate(self.writers):
            w.unsetOverwriteFlag()

    def write(self, protected_data) -> List:
        return [writer.write(protected_data) for writer in self.writers]

    def saveRDD(self, path: str, rdd: RDD):
        raise NotImplementedError("Multi-writer does not directly save RDDs (its contained writers do).")

    def transformRDDForSaving(self, rdd: RDD):
         raise NotImplementedError("Multi-writer does not directly transform RDDs (its contained writers do).")
