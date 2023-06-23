"""
    This is the reader module for the DAS-Decennial instance of the DAS-framework.
    It contains a reader class that is a subclass of AbstractDASReader.
    The class must contain a method called read.
    It also defines an AbstractTable class for tracking metadata.
"""
from typing import Dict, List, Tuple, Union
import xml.etree.ElementTree as ET
import os
import logging
import warnings
from collections import defaultdict, Counter
from configparser import ConfigParser, NoOptionError

import numpy as np
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql import SparkSession, Row, DataFrame

from das_framework.driver import AbstractDASReader, AbstractDASModule
import programs.nodes.nodes as nodes
import programs.sparse as sparse
from programs.das_setup import DASDecennialSetup
from programs.das_rdd import DASDataFrame
from programs.rdd_like_list import RDDLikeList
import das_utils
from programs.geographic_spines.define_spines import call_opt_spine, aian_spine, gq_off_spine_entities
from exceptions import Error, DASConfigError,NodeRDDValidationError
from das_constants import CC


class Interval(tuple):
    """
        Description:
            This class does the following:
                It creates a custom tuple for closed interval.
                Left and right endpoints included.
                Overloads contains the method.
                The tuple must be of length 2.
                Constructs a new Interval with the NewRange method.
    """
    @staticmethod
    def NewRange(left: Union[int, str], right: Union[int, str]):
        """
            Description:
                This method creates an interval object.

            Inputs:
                left (and also right): They must be either an integer,
                                       a string representation of integer,
                                       or a single character as a string.
                Note: left and right must each be the same type.

            Output:
                Interval object
        """
        assert left <= right
        assert isinstance(left, type(right))
        assert isinstance(right, type(left))
        return Interval((left, right))

    def __init__(self, tup: tuple):
        """
            This will assert the length of the tuple is exactly 2.
        """
        assert len(tup) == 2
        self.left, self.right = tup

    def __contains__(self, item) -> bool:
        """
            This checks if something is in a specific interval.
        """
        # TODO: "1BC" in Interval(("000","255")) returns true but we might want false.
        # some legal values include a mix of alphanumeric characters so maybe leave it as is.
        try:
            tmp = str(item)
        except TypeError:
            return False
        try:
            length_check = len(self.left) <= len(item) <= len(self.right)
        except TypeError:
            length_check = True
        return True if (self.left <= item <= self.right) and length_check else False

    def __len__(self) -> int:
        """
            This returns the length of an interval.
        """
        try:
            return ord(self.right) - ord(self.left) + 1
        except TypeError:
            return int(self.right) - int(self.left) + 1


class LegalList(list):
    """
        This is a subclass of list. It is intended to hold only Interval objects.
    """
    def __contains__(self, item) -> bool:
        for interval in self:
            if item in interval:
                return True
        return False

    def __len__(self) -> int:
        return sum([len(interval) for interval in self])

    def __min__(self) -> int:
        return min([interval.left for interval in self])


class TableVariable:
    """
        This is a Variable metadata holder.
        It stores information about each variable in the table, including:
        type, SQL type, legal values, and 2018 end-to-end specific values.
    """
    # pylint: disable=invalid-name
    def __init__(self, name: str, vtype: str = None, legal: LegalList = None):
        self.name = name
        self.vtype = vtype
        self.sql_type = self.get_sql_type()
        self.size = None
        self.end_to_end_value = None  # value for 2018
        self.legal_values = legal

    def make_from_config(self, config: ConfigParser):
        """
            This will set the attributes reading from the config file.
        """
        try:
            vtype = config.get(CC.READER, f"{self.name}.{CC.VAR_TYPE}")
            legal_values = config.get(CC.READER, f"{self.name}.{CC.LEGAL}")
        except NoOptionError as e:
            raise DASConfigError(f"Missing variable {self.name} specifications", *e.args)

        self.set_vtype(vtype)
        self.set_legal_values(legal_values)

        return self

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        """
            This will return the representation of a Variable.
        """
        return "TableVariable(name:{} vtype:{})".format(self.name, self.vtype)

    def set_vtype(self, var_type: str):
        """
            This defines the string representation of a variable type.

            Input:
                var_type: supports "str" or "int"
        """
        assert var_type in ["int", "str", "float", "NUMBER", "VARCHAR"]
        self.vtype = var_type
        self.sql_type = self.get_sql_type()  # Keeps types in sync.

    def get_sql_type(self) -> Union[IntegerType, StringType, FloatType]:
        """
            This returns the SparkSQL type of a variable.
        """
        return IntegerType() if (self.vtype == "int" or self.vtype == "NUMBER") else\
               StringType() if (self.vtype == "str" or self.vtype == "VARCHAR") else\
               FloatType() if (self.vtype == "float") else None

    def set_legal_values(self, legal_string: str):
        """
            This sets the "legal values."

            Input:
                legal_string: a string of one of the following forms:
                    3
                    2,3,4
                    2-4
                    3-8,4,5,9-12,
                    etc
        """
        ranges = legal_string.strip().split(",")
        legal = []
        for split_string in ranges:
            endpoints = split_string.strip().split("-")
            left = endpoints[0].strip()
            right = endpoints[-1].strip()
            if self.sql_type == IntegerType():
                legal.append(Interval.NewRange(int(left), int(right)))
            else:
                legal.append(Interval.NewRange(left, right))
        self.legal_values = LegalList(legal)

    def set_legal_values_from_ranges(self, legal_ranges: LegalList):
        """
            Sets the legal values based on ranges indicated by
            dictionaries where 'a' indicates the lower bound and 'b' indicates the upper bound

            Input:
                legal_string: a string of one of the following forms:
                '[{"a": "21", "b": "25"}, {"a": "47", "b": "47"}, {"a": "61", "b": "61"}]'
        """
        legal = []

        for value_tuple in legal_ranges:
            left = value_tuple['a'].strip()
            right = value_tuple['b'].strip()
            if self.sql_type == IntegerType():
                legal.append(Interval.NewRange(int(left), int(right)))
            else:
                legal.append(Interval.NewRange(left, right))
        self.legal_values = LegalList(legal)

    def __eq__(self, other) -> bool:
        return self.name == other.name


class AbstractTable(AbstractDASModule):
    """
        This class is a support object for storing table layout information.
    """
    reader: 'reader'  # Pointer to the reader instance, to get the variables that are common for all read tables
    location: str  # Location of the file with data for the table (usually, CEF as CSV)
    variables: List[TableVariable]  # List of variables
    recode_variables: List[TableVariable]  # List of recodes to make
    csv_file_format: Dict[str, Union[bool, str, StructType]]  # Options for reading the CSV file
    geography_variables: Tuple[str, ...]  # List of variables defining geography
    histogram_variables: Tuple[str, ...]  # List of variables on which the histogram is constructed
    data_shape: Tuple[int, ...]  # Shape of the histogram

    # pylint: disable=invalid-name
    def __init__(self, *, reader_instance: 'reader', **kwargs):
        super().__init__(**kwargs)

        self.reader = reader_instance

        #self.location = [os.path.expandvars(x) for x in re.split(CC.REGEX_CONFIG_DELIM, self.getconfigwsec(CC.PATH)) if len(x)>0]
        self.location = list(self.gettuplewsec(CC.PATH))

        self.variables = self.make_variables()

        self.recode_variables = [TableVariable(var_name).make_from_config(self.config) for var_name in self.gettuplewsec(CC.RECODE_VARS, default=())]

        self.csv_file_format = self.reader.csv_file_format.copy()

        # If we want these distinct for each table, then the option should include table name
        try:
            self.csv_file_format["sep"] = self.getconfig(f"{self.name}.{CC.DELIMITER}", section=CC.READER)
        except NoOptionError:
            pass

        self.csv_file_format['schema'] = self.set_schema()

        self.geography_variables = self.gettuplewsec(CC.GEOGRAPHY)
        self.histogram_variables = self.gettuplewsec(CC.HISTOGRAM)

        # Finally, set up the recoder, if there is any. This must be done in __init__() so the recoder is included in the BOM.

        recoder_name = f"{self.name}.{CC.PRE_RECODER}"
        if not self.config.has_option(CC.READER, recoder_name):
            self.recoder=None
        else:
            args = [self.gettuple(var.name, section=CC.READER, sep=" ") for var in self.recode_variables]
            if self.getboolean(f"{self.name}.{CC.NEWRECODER}", section=CC.READER, default=False):
                args = args + [self.recode_variables]
            try:
                self.recoder = das_utils.class_from_config(self.config, recoder_name, CC.READER)(*args)
            except TypeError as err:
                raise TypeError(f"Table {self.name} failed to create recoder, arguments: {args}, Error: {err.args}")

    def make_variables(self) -> List[TableVariable]:
        return [TableVariable(var_name).make_from_config(self.config) for var_name in self.gettuplewsec(CC.VARS)]

    # def getconfigwsec(self, key):
    #     """
    #     Get config option from the config file, from READER section
    #     :param key: option name
    #     :return: str value of config option
    #     """
    #     return super().getconfig(f"{self.name}.{key}", section=CC.READER)

    def gettuplewsec(self, key: str, default=None):
        """
        Get config option from the config file, from READER section, and parse as a tuple
        :param key: option name
        :param default: required by super().gettuple
        :return: tuple of str, listed in config option
        """
        return super().gettuple(f"{self.name}.{key}", default=default, section=CC.READER, sep=" ")

    def add_variable(self, new_var: TableVariable):
        """
            This adds/stores information for a new variable to a self object.

            Input:
                new_var: a TableVariable object
        """
        assert isinstance(new_var, TableVariable)
        if new_var in self.variables:
            raise Error(f"Trying to add TableVariable {new_var} which is already in table {self.name} (variables: {self.variables})")
        self.variables.append(new_var)
        # The following line is not needed, since add_variable is called either after reading the csv,
        # or, if before, it adds recode variables which don't exist in csv and thus not needed in the schema
        # self.csv_file_format["schema"] = self.set_schema()

    def get_variable(self, name: str):
        """
            This will get the name of a variable, if possible.

            Input:
                name: the name of variable to return

            Output: the variable with name "name" or not if no such variable
        """
        for var in self.variables:
            if var.name == name:
                return var
        return None

    def set_schema(self) -> StructType:
        """
            This will return the SQL schema/structure type for a self object.
        """
        return StructType([StructField(v.name, v.sql_type) for v in self.variables])

    def set_shape(self) -> None:
        """ Call after recodes to ensure vars have all been added to table """
        self.data_shape = tuple([len(self.get_variable(var).legal_values) for var in self.histogram_variables])

    def load(self, filter=None):
        """
            This loads the records from a csv (typically in S3) into a Spark dataframe.

            Input:
                spark: the SparkSession object

            Output: the Spark dataframe object

            Note: This code only runs on the head node, not on the workers
        """

        ## Add that this is an input to the XML file
        for loc in self.location:
            ET.SubElement(self.das.dfxml_writer.doc, CC.DAS_INPUT).text = loc

        ## Try to get a DAS_Singleton().
        ## Apparently when this is run in a Jenkins test there is no self.das object...
        ##
        DVS_Singleton = None
        try:
            if self.setup.dvs_enabled:
                from  programs.python_dvs.dvs import DVS_Singleton
        except AttributeError:
            pass
        if DVS_Singleton is not None:
            ds = DVS_Singleton()
            ds.add_s3_paths_or_prefixes(ds.COMMIT_BEFORE,self.location)

        spark = SparkSession.builder.getOrCreate()
        return spark.read.csv(self.location, **self.csv_file_format).repartition(self.reader.num_reader_partitions)

    def recode_meta_update(self) -> None:
        """
            This adds the recode variables to the list of table variables.
        """
        try:
            for v in self.recode_variables:
                self.add_variable(v)
        except TypeError:
            pass

    def pre_recode(self, data: DataFrame):
        """
            This applies predisclosure avoidance recodes.

            Inputs:
                data: a Spark dataframe (df)
                config: a ConfigParser where the recodes are specified

            Output:
                a Spark dataframe (df) with the recode columns added
        """
        # If recoder is not set, just return the data as is
        if self.recoder is not None:
            # nosparkdata = RDDLikeList(data.rdd.collect()).map(self.recoder.recode)
            # spark = SparkSession.builder.getOrCreate()
            # return spark.sparkContext.parallelize(nosparkdata.list).toDF()

            return data.rdd.map(self.recoder.recode).filter(lambda row: row != None).toDF()
        else:
            return data

    def filter(self, data: DataFrame, test_area):
        """ When using a test area, filter the RDD by removing geocodes outside the test area"""
        if test_area != "":
            data = data.rdd.filter(lambda row: "".join([str(row[code]) for code in self.geography_variables]).startswith(test_area)).toDF()
        return data

    def process(self, data_structure):
        """
            This function processes raw input, which must be implemented in child classes.

            Inputs:
                data_structure: a data object (eg: spark, df, or ordd)

            Output:
                defaults to the identity function
        """
        return data_structure

    def verify(self, data: DataFrame) -> bool:
        """
            This is a quick pass over the data to ensure that all values are as expected.

            Input:
                data - spark df
            Output:
                Either:
                (a) "True" if data is valid,
                otherwise,
                (b) an assertion error.
        """
        return data.rdd.filter(lambda row: self.check_row(row) == 0).isEmpty()

    def check_row(self, row: Row) -> bool:
        """
            Input:
                row: the row of the Spark dataframe

            Output:
                Either:
                (a) "True" if row is valid,
                otherwise,
                (b) an assertion error.
        """
        for var in self.variables:
            assert var.name in row, "{} is not in row: {}".format(var.name, row)
            assert row[var.name] in var.legal_values, "{} is not a legal value for {}, the bad row was {}".format(
                row[var.name], var.name, row)
        return True


class DASDecennialReader(AbstractDASReader):
    """
    The CEF reader object loads microdata and metadata into tables
    and then converts them into a usable form.

    NOTE: This class is subclassed by pickled_blocks_syn2raw_reader.
    """

    setup: DASDecennialSetup
    invar_names: List[str]  # Names of bottom (Block) level invariants
    cons_names: List[str]   # Names of bottom (Block) level constraints
    unit_table_name: str
    constraint_tables: Tuple[str, ...]
    main_table_name: str
    data_names: List[str]
    modified_geocode_dict: dict

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        assert self.config

        try:
            comment_start = self.getconfig(CC.CSV_COMMENT)
        except NoOptionError:
            comment_start = None

        self.csv_file_format = {
            "header": self.getboolean(CC.HEADER),
            "sep": self.getconfig(CC.DELIMITER),
            "comment": comment_start
        }

        self.num_reader_partitions = self.getint(CC.NUM_READER_PARTITIONS, default=100)
        self.range_partition       = self.getboolean(CC.RANGE_PARTITION, default=False)
        self.reader_partition_len  = self.getint(CC.READER_PARTITION_LEN, default=11)
        self.measure_rdd_times     = self.getboolean(CC.MEASURE_RDD_TIMES, default=False)

        # The [reader] section of the config file specifies all of the tables to read in.
        # For each table a path and a class are provided.
        # [reader]
        # PersonData.path: $DAS_S3ROOT/title13_input_data/table8/ri44.txt
        # PersonData.class: programs.reader.sql_spar_table.SQLSparseHistogramTable
        #

        # This gets all of the table names:
        table_names = self.gettuple(CC.TABLES, sep=" ")

        self.annotate("building table infrastructure")
        self.annotate(f'table names {table_names}')
        self.annotate("Reading table module and class names from config")

        # Create the reader classes for each table.
        # They are subclasses of the AbstractDASModule, so they need to be properly setup
        # They are stored in self.tables.
        # The class definitions of the table classes are in sql_spar_table.py and spar_table.py
        self.tables = {name: das_utils.class_from_config(self.config, f"{name}.{CC.TABLE_CLASS}", CC.READER)
                       (name=name, config=self.config, reader_instance=self, das=self.das)
                       for name in table_names}

        self.shape_dict = {}

        # Find out recode variables and their dimensions to know the set of histogram variables and its shape
        for table in self.tables.values():
            logging.info(f"recode meta for table {table.name}")
            table.recode_meta_update()
            table.set_shape()
            self.shape_dict[table.name] = table.data_shape

        # Bottom geographical level
        bottom: str = self.setup.levels[0]

        # Invariants and Constraints for the bottom level
        ic_bottom: Dict[str, List[str]] = self.setup.inv_con_by_level[bottom]

        # Get invariants names from setup
        self.invar_names = ic_bottom["invar_names"]

        # Get constraints names from setup
        self.cons_names = ic_bottom["cons_names"]

        # Update the data vintage variable to remove chars
        numeric_filter = filter(str.isdigit, self.setup.input_data_vintage)
        self.setup.input_data_vintage = "".join(numeric_filter)

        # Get the names of tables
        self.main_table_name    = self.getconfig(CC.PHTABLE).strip()  # Person or Household table
        self.unit_table_name    = self.getconfig(CC.UNITTABLE)    # Unit table
        self.constraint_tables = self.gettuple(CC.CTABLES, default=())
        self.data_names = [self.main_table_name, self.unit_table_name] + list(self.constraint_tables)

        # Shape of the person histogram (save it in setup object for further use)
        if self.setup.hist_shape != self.tables[self.main_table_name].data_shape:
            msg = (f"The histogram shape set in config file {self.tables[self.main_table_name].data_shape} that the data read is different from "
                   f"the shape of schema {self.setup.schema} {self.setup.hist_shape}")
            warnings.warn(msg)
            self.log_warning_and_print(msg)
            self.setup.hist_shape = self.tables[self.main_table_name].data_shape

        # Save person tables histogram variables in setup
        if tuple(self.setup.hist_vars) != tuple(self.tables[self.main_table_name].histogram_variables):
            msg = f"The histogram variables set in config file {self.tables[self.main_table_name].histogram_variables} that the data read are " \
                f"different from the variables of schema {self.setup.schema} {self.setup.hist_vars}"
            warnings.warn(msg)
            self.log_warning_and_print(msg)
            self.setup.hist_vars = self.tables[self.main_table_name].histogram_variables

        # Get geocode dict from setup object
        self.modified_geocode_dict = self.setup.geocode_dict.copy()

    def read(self) -> RDDLikeList:
        """
            This function performs the following steps:

            (1) Load input file into spark dataframe.
            (2) Convert dataframe into a rdd of ndarrays (one ndarray for each block).
        """

        if self.setup.postprocess_only:
            # Don't read, we are only testing postprocessing.
            # SLG Question: Where does the data come from in this case?
            # PZ Answer: It's loaded in the engine, as noisy measurements
            return None

        # Optionally filter to test area.
        # Specifying a test area can significantly shorten runtimes.
        try:
            test_area = self.getconfig("test_area")
        except NoOptionError:
            test_area = ""

        # Testpoint 013S for CEF Ingest Started
        self.das.delegate.log_testpoint('013S')

        try:
            logging.info("loading the data")
            tmp = defaultdict()
            for table in self.tables.values():
                self.annotate(f"loading table {table.name}")
                table_df = table.load()
                # if not table.verify(table_df):
                #    print("table contains invalid records")
                #    raise RuntimeError
                logging.info(f"applying filter to {table.name}")
                table_df = table.filter(table_df, test_area)

                self.annotate(f"recodes for {table.name}")
                table_df = table.pre_recode(table_df.repartition(self.num_reader_partitions))
                if self.measure_rdd_times:
                    table_df = DASDataFrame(table_df)
                logging.info("dict")
                # TODO: Why do we need a tuple? Why not put just the processed table?
                if table:
                    self.annotate(f"Histogramming {table.name}")
                    tmp[table.name] = (table, table.process(table_df))

            table_df_dict = tmp

            # Person table (for DHCP or PL94 Person) or Household table (for DHCH, derived from unit table; for H1 in PL94 it's the unit table)
            person_data = table_df_dict[self.main_table_name][1]
            # Unit table (for Person PL94 used for constraints, for DHCH used to take noisy measurements and do post-processing as well)
            unit_data = table_df_dict[self.unit_table_name][1]

            # print(f"Person 12-len max part: {das_utils.maxPartPerKey(person_data, lambda r: r[0][0][:12])}")
            # print(f"Unit 12-len max part: {das_utils.maxPartPerKey(unit_data, lambda r: r[0][0][:12])}")
            join_data = person_data.rightOuterJoin(unit_data)

            for tname in self.constraint_tables:
                join_data = join_data.fullOuterJoin(table_df_dict[tname][1]).mapValues(lambda d: d[0] + (d[1],))

            # print(f"Join 12-len max part: {das_utils.maxPartPerKey(join_data, lambda r: r[0][0][:12])}")

            # Checkpoint: save the recoded joint data (Person/Households + Units + additional constraint tables if any, as histograms, by geocode)
            if self.getboolean(CC.SAVE_READER_CHECKPOINTS, default=True):
                spark = SparkSession.builder.getOrCreate()
                path = os.path.join(self.das.writer.output_path, "CEFhistograms.pickle")
                self.annotate("Saving joined CEF in histogram format")
                das_utils.savePickledRDD(path, join_data, dvs_singleton=None)
                self.annotate("Reloading joined CEF in histogram format")
                join_data = spark.sparkContext.pickleFile(path)


            spine_type = self.setup.spine_type
            redefine_counties = self.getconfig(key=CC.REDEFINE_COUNTIES, section=CC.GEODICT, default='nowhere')

            user_plbs = self.setup.geolevel_prop_budgets
            sc = SparkSession.builder.getOrCreate().sparkContext
            if spine_type == CC.OPT_SPINE:
                self.annotate("Starting geographic spine redefinition and optimization")
                assert int(self.setup.input_data_vintage) >= 2010, f"The spine {CC.OPT_SPINE} requires input data vintage >= 2010 (Please: Ensure that the Input Data Vintage is an integer)"
                dp_mechanism = self.setup.dp_mechanism_name
                approximate_dp_mechanism = {CC.ROUNDED_CONTINUOUS_GAUSSIAN_MECHANISM:True, CC.GAUSSIAN_MECHANISM:True, CC.DISCRETE_GAUSSIAN_MECHANISM:True, CC.GEOMETRIC_MECHANISM:False}
                epsilon_delta = approximate_dp_mechanism[dp_mechanism]
                prim_spine = self.getboolean(CC.PRIM_SPINE, section=CC.GEODICT, default=False)

                group_type = self.getconfig(key="group_type", section=CC.GEODICT, default='orig')
                entity_threshold = self.getint(CC.ENTITY_THRESHOLD, section=CC.GEODICT, default=9)
                bypass_cutoff = self.getint(CC.BYPASS_CUTOFF, section=CC.GEODICT, default=150)
                fanout_cutoff = self.getint(CC.FANOUT_CUTOFF, section=CC.GEODICT, default=12)
                ignore_gqs_in_bgs = self.getboolean(CC.IGNORE_GQS_IN_BLOCK_GROUPS, section=CC.GEODICT, default=False)
                target_orig_bgs = self.getboolean(CC.TARGET_ORIG_BLOCK_GROUPS, section=CC.GEODICT, default=False)
                target_das_aian_areas = self.getboolean(CC.TARGET_DAS_AIAN_AREAS, section=CC.GEODICT, default=False)
                target_school_dists = self.getboolean(CC.TARGET_SCHOOL_DISTS, section=CC.GEODICT, default=False)
                use_prim_crosswalk = self.getboolean("use_prim_crosswalk", section=CC.GEODICT, default=True)

                gq_types_query = self.setup.unit_schema_obj.getQuery(CC.HHGQ_SPINE_TYPES)
                geocode16 = join_data.map(lambda row: (row[0][0], gq_off_spine_entities(ignore_gqs_in_bgs, gq_types_query.answer(row[1][1].toarray().flatten()))))

                prim_crosswalk = self.setup.make_prim_crosswalk_dict(self.setup.prim_geo_s3_path) if prim_spine else None

                geocode16_to_DASGeoid, plb_dict, new_widths = call_opt_spine(user_plbs, geocode16, self.modified_geocode_dict, fanout_cutoff, epsilon_delta,
                                                                             self.setup.aian_areas, entity_threshold, redefine_counties, bypass_cutoff,
                                                                             self.setup.grfc_path, self.setup.aian_ranges_path, self.setup.strong_mcd_states,
                                                                             target_orig_bgs, target_das_aian_areas, prim_spine, group_type,
                                                                             prim_crosswalk, target_school_dists, use_prim_crosswalk)

                join_data = join_data.map(lambda row: ((geocode16_to_DASGeoid[row[0][0]],), row[1]))

                # Check budgets allocated by spine optimization to the geolevels
                sorted_geocode_lengths = sorted(new_widths.keys())
                by_level_plbs = {new_widths[gllen]:[] for gllen in sorted_geocode_lengths}  # defaultdict(list)
                for das_geocode, plb in plb_dict.items():
                    if plb != 0: # We're excluding nodes that are single siblings and had their budget moved upwards
                        by_level_plbs[new_widths[len(das_geocode)]].append(plb)

                violation = False
                is_national_run = (sorted_geocode_lengths[0] == 0)
                is_puerto_rico = (not is_national_run) and (len(tuple(filter(lambda gc: not (gc.startswith('72') or gc.startswith('072') or gc.startswith('172')), plb_dict.keys()))) == 0)
                for gl, plb_list in by_level_plbs.items():
                    is_pr_county = is_puerto_rico and (gl == new_widths[sorted_geocode_lengths[1]])  # County length is the second element ([1]) of geocode_dict if state-size run like PR
                    if len(plb_list) == 0:  # If there are no elements, that means it's pass-though level, all budget reallocated up
                        continue
                    conf_plb = float(self.setup.budget.geolevel_prop_budgets_dict[gl])
                    max_plb = float(max(plb_list))
                    min_plb = float(min(plb_list))
                    most_common_plb = float(Counter(plb_list).most_common(1)[0][0])
                    print(f"<span style='color:blue; font-size:large'>Geolevel {gl} PLB: config-designated plb {conf_plb},  maximum plb {max_plb}, minimum plb {min_plb},  most common {most_common_plb}</span>")
                    if (abs(conf_plb - max_plb) > 1e-7) and (abs(conf_plb - min_plb) > 1e-7) and (abs(conf_plb - most_common_plb) > 1e-7):
                        if is_national_run or (is_puerto_rico and not is_pr_county):
                            violation = True
                        print(f"<span style='color:red; font-size:x-large'>Geolevel {gl} config-designated PLB does not correspond to minimal, maximal or most common PLB in {gl}</span>")
                if violation:
                    raise RuntimeError("Config-designated PLBs don't correspond to ones assigned in spine optimization. Check geolevel ordering.")

                self.log_and_print("Broadcasting PLB allocation...")
                self.setup.budget.fillPLBAllocation(sc.broadcast(plb_dict))
                self.log_and_print("Broadcasting PLB allocation done.")
                self.setup.modified_block_geoids = sorted(geocode16_to_DASGeoid.values())

            elif spine_type == CC.AIAN_SPINE:
                self.annotate("Starting geographic spine redefinition")
                assert int(self.setup.input_data_vintage) >= 2010, f"The spine {CC.AIAN_SPINE} requires input data vintage >= 2010 (Please: Ensure that the Input Data Vintage is an integer)"
                geocode16 = join_data.map(lambda row: row[0][0])
                geocode16_to_DASGeoid, new_widths = aian_spine(geocode16, self.modified_geocode_dict, self.setup.aian_areas, redefine_counties, self.setup.grfc_path,
                                                               self.setup.aian_ranges_path, self.setup.strong_mcd_states)
                join_data = join_data.map(lambda row: ((geocode16_to_DASGeoid[row[0][0]],), row[1]))
                self.setup.modified_block_geoids = sorted(geocode16_to_DASGeoid.values())

            else:
                new_widths = self.modified_geocode_dict  # Just keep the old geocode_dict
                message = f"spine type must be {'/'.join(CC.SPINE_TYPE_ALLOWED)} rather than {spine_type}"
                self.setup.modified_block_geoids = sorted(join_data.map(lambda row: row[0][0]).collect())
                assert spine_type == CC.NON_AIAN_SPINE, message

            # New geocode_dict. Note, self.setup.geocode_dict is still the old standard one, for assigning the geocodes back
            self.modified_geocode_dict = new_widths

            self.setup.modified_geoids_map = {}
            widths = sorted(list(self.modified_geocode_dict.keys()), reverse=True)
            distinct_ids = self.setup.modified_block_geoids
            for width in widths:
                level = new_widths[width]
                if width != widths[0]:
                    distinct_ids = np.unique([geoid[:width] for geoid in distinct_ids]).tolist()
                self.setup.modified_geoids_map[level] = sc.broadcast({k: i for i, k in enumerate(distinct_ids)})

            # make block nodes
            # from programs.rdd_like_list import RDDLikeList
            # block_nodes = RDDLikeList(join_data.collect()).map(self.makeBlockNode)
            block_nodes = join_data.map(self.makeBlockNode)

            part_size_noisy = self.setup.part_size_noisy
            if part_size_noisy > 0:
                block_geolevel_name = self.modified_geocode_dict[widths[0]]
                block_nodes = das_utils.repartitionNodesEvenly(block_nodes, 0, self.setup.modified_geoids_map[block_geolevel_name], is_key_value_pairs=False, partition_size=part_size_noisy)

            # Checkpoint: save the block data in DAS Geounit node format
            if self.getboolean(CC.SAVE_READER_CHECKPOINTS, default=True):
                spark = SparkSession.builder.getOrCreate()
                path = os.path.join(self.das.writer.output_path, "CEFgeonodes.pickle")
                self.annotate("Saving CEF in GeounitNode format")
                das_utils.savePickledRDD(path, block_nodes, dvs_singleton=None)
                self.annotate("Reloading CEF in GeounitNode format")
                block_nodes = spark.sparkContext.pickleFile(path)

            # If not using spark in engine, convert into RDDLikeList
            if not self.setup.use_spark:
                block_nodes = RDDLikeList(block_nodes.collect())

            # print(f"Block nodes", das_utils.rddPartitionDistributionMoments(block_nodes))

            # num_partitions = self.num_reader_partitions
            if self.getboolean(CC.PARTITION_BY_BLOCK_GROUP, default=False):
                # block_nodes = das_utils.partitionByParentGeocode(block_nodes, self.num_reader_partitions)
                block_nodes = das_utils.partitionBySiblingNumber(block_nodes, self.num_reader_partitions)

            # elif abs(block_nodes.getNumPartitions() - num_partitions)/num_partitions > 0.2:
            #     # This is for comparison with just changing the number of partitions
            #     print("Repartitioning with default hash partitioner...")
            #     block_nodes = block_nodes.repartition(num_partitions)
            #     # block_nodes = das_utils.partitionBySiblingNumber(block_nodes, num_partitions)

            # print(f"Block nodes (repartitioned)", das_utils.rddPartitionDistributionMoments(block_nodes))
            # print(f"Block nodes max part: {das_utils.maxPartPerKey(block_nodes, lambda n: n.parentGeocode)}")

            if not self.getboolean(CC.SKIP_PERSIST_IN_READER, default=False):
                block_nodes.persist()
                join_data.unpersist()

            # Check that the input data satisfies constraints
            if self.setup.validate_input_data_constraints:
                self.validateConstraintsNodeRDD(block_nodes, self.setup.levels[0])

        except RuntimeError as e:
            # Testpoint 013S for CEF Ingest Failed
            self.das.delegate.log_testpoint('014F')
            raise e

            # Testpoint 014S Successful Completion of CEF Ingest
        self.das.delegate.log_testpoint('014S')

        return block_nodes

    @staticmethod
    def validateNodeRDD(node_rdd, failed_node_function=lambda d: False, failed_msg: str = "failed",
                        sample_description: str = "Geocodes", sample_function=lambda node: node.geocode,
                        error: bool = False, sample_size: int = 5):
        """
        Generic function to validate each GeounitNode in an RDD.
        1) Checks each element of :node_rdd: with :failed_node_function:
        2) Checks whether there are elements that failed validation
        3) If yes, issues warning
        4) Prints out a sample of failed nodes, with each node printed by :sample_function:

        This is only staticmethod of the reader module, because reader module is where such validation would typically take place.
        Since it's static, it can be easily taken out in the future

        :param node_rdd: RDD of GeounitNodes to validate
        :param failed_node_function: validating function, returns True if node fails and False if node passes validation
        :param failed_msg: Diagnostic message describing what "failed validation" is (e.g. "failed constraints", "lack invariants" etc.)
        :param sample_description: Description for string returned for each of the sampled failed nodes
        :param sample_function: Function converting a failed node to a string describing it
        :param error: if True throw Exception, otherwise throw warning
        :param sample_size: How many failed nodes to prints
        """
        failed_nodes = node_rdd.filter(failed_node_function)
        fn_count = failed_nodes.count()
        if fn_count > 0:
            failed_node_sample = failed_nodes.take(sample_size)

            msg = f"{fn_count} nodes {failed_msg}"

            sample_msg = f"Sample of {sample_size} failed nodes ({failed_msg}):\n" \
                f"{sample_description}:"

            err = NodeRDDValidationError(msg, sample_msg, tuple(map(sample_function, failed_node_sample)))

            if error:
                raise err
            else:
                logging.warning(err.msg)
                warnings.warn(err.msg)
                print(f"{err.sample_msg} {err.sample}")

        das_utils.freeMemRDD(failed_nodes)

    def validateConstraintsNodeRDD(self, node_rdd: RDDLikeList, level):
        """
        Make sure that every node in RDD satisfies its own constraints
        :param node_rdd:
        :param level: which geolevel (for diagnostic messaging)
        """
        self.log_and_print(f"Checking if all of the {level} constraints are satisfied on input data")
        self.annotate(f"validateNodeRDD {level} starting")
        self.validateNodeRDD(node_rdd,
                             failed_node_function=lambda node: not node.checkConstraints(),
                             failed_msg=f"in input data ({level}) don't satisfy constraints",
                             sample_description="Failed constraints (!!!POSSIBLY TITLED INFO!!!) as (geocode, [(constraint_name, RHS-in-actual-data, RHS-as-set-by-constraint, sign)...])",
                             sample_function=lambda node: (node.geocode, node.checkConstraints(return_list=True)),
                             error=False)
        self.annotate(f"validateNodeRDD {level} finished")

    def makeBlockNode(self, person_unit_arrays):
        """
            This function makes block nodes from person unit arrays for a given geocode.

            Inputs:
                config: a configuration object
                person_unit_arrays: a RDD of (geocode, arrays), where arrays are the tables defined in the config

            Output:
                block_node: a nodes.GeounitNode object for the given geocode
        """

        geocode, arrays = person_unit_arrays

        # Assign arrays to table names in a dictionary {name:array} and fill in with zeros if array is non-existent
        assert len(arrays) == len(self.data_names)
        data_dict = {n: sparse.multiSparse(a, shape=self.shape_dict[n]) for n, a in zip(self.data_names, arrays)}

        # geocode is a tuple where the [1] entry is empty. We only want the [0] entry.
        geocode = geocode[0]
        logging.info(f"creating geocode: {geocode}")

        raw = data_dict[self.main_table_name]
        raw_housing = data_dict[self.unit_table_name]

        # Make Invariants
        invariants_dict = self.setup.makeInvariants(raw=raw, raw_housing=raw_housing, invariant_names=self.invar_names)

        # Make invariants to constrain to other tables (e.g. PL94, run previously)
        for tname in self.constraint_tables:
            invariants_dict.update({tname: data_dict[tname].toDense().flatten()})

        # Make Constraints
        constraints_dict = self.setup.makeConstraints(hist_shape=(self.setup.hist_shape, self.setup.unit_hist_shape), invariants=invariants_dict, constraint_names=self.cons_names)

        block_node = nodes.GeounitNode(geocode=geocode, geocode_dict=self.modified_geocode_dict,
                                       raw=raw, raw_housing=raw_housing,
                                       cons=constraints_dict, invar=invariants_dict)
        return block_node
