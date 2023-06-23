This is an overview of the implementation of TDA. More details can be found [here](DAS-Implementation-Details).

TDA is a Python Spark (PySpark) application that runs on Amazon Web Services (AWS) Elastic Map Reduce (EMR). The TDA cluster employs a single Master node (on which the DAS PySpark Driver application is typically deployed) and multiple Core and Task nodes (in which the noisy measurements are taken for each geographic unit, and the mathematical optimization to generate microdata in each parent-children set of geographic units occurs) for both data storage and computation. Processing proceeds in parallel across the core and task nodes managed by the master node and the EMR infrastructure. Inputs are provided and outputs are saved in AWS S3 at locations specified as part of the configuration of a given run of the TDA.

For the DHC (Demographic and Housing Characterisics) release, the inputs to the TDA consist of:
1. 2020 Census enumeration data contained in 52 Census Edited Files (CEF)--one for each state, DC and the Commonwealth of Puerto Rico, as described here.
2. Geographic data (GRF-C files) indicating geographic IDs in a hierarchy: nation, state, county, census tract, block group, and block.
3. The PPMF (privacy-protected microdata file) generated for the 2020
   P.L. Redistricting Data release; this is used to ensure that the published DHC data is consistent with the previously published redistricting data.
4. A **query strategy** that specifies the queries for which noisy measurements (and invariants) are taken (though not required as input for TDA to run, there is also a related **query workload** that specifies the publication table structure as a separate set of queries used to evaluate system performance relative to the 2010 CEF; terminology following the strategy-workload distinction made in [the Matrix Mechanism papers](https://link.springer.com/article/10.1007/s00778-015-0398-x)).
5. Key supporting parameters:
    * a privacy-loss budget associated with each query at each geographic level (computed based on related config parameters)
    * **query orderings** specifying the order in which strategy queries are added to each of the optimization problems used to generate well-formed (non-negative integral) histogram counts
    * a set of **invariants** and **constraints** which the output histograms (used to generate protected microdata) must satisfy

Before the TDA runs, the input CEFs and GRF-C are validated, checking that CEF fields have valid input values given the CEF schema, and that geographic locations referenced in the CEF are defined in the GRF-C data.

For the DHC release, the TDA produced four privacy-protected microdata detail files (MDFs):
1. Individual person microdata for the US
2. Individual person microdata for Puerto Rico
3. Household/housing unit microdata for the US
4. Household/housing unit microdata for Puerto Rico

Each run also saved the query histograms for each geographic region
(down to the block level) as *noisy measurements* for future use. The
TDA algorithm was run four times to generate the privacy-protected
MDFs listed above.

Each run shares the following steps:
1. The input files are combined to create an internal representation of the relevant subset of the approximately 330 million people and 140 million households in the United States and its territories (e.g. all US states and state-equivalents, but not Puerto Rico, are combined for run \# 1 above)
2. For each geographic level and each region (node) within each level, a measurement for each strategy query is generated using the Discrete Gaussian Mechanism; these are the **noisy measurements**, which can be negative (and often will be, if the enumerated count in the CEF was 0 or near 0) and provide many inconsistent estimators (e.g., summing the CENRACE query and the VOTINGAGE query both yield estimates of the TOTAL population, but these will generally be different estimates)
3. To avoid negative counts and impose consistency, the noisy measurements are _optimized_ to produce well-formed arithmetically-consistent histograms that satisfy the designated invariants and constraints. This is done by translating the constraints into a system of linear equations and inequalities, which are appended to optimization problems (with measures of distance to query count estimates in the objective function) and solved by the Gurobi optimizer (accessed via its Python API). In order to reduce certain kinds of bias and undesirable correlation in disclosure error, queries are added to the linear system in a particular order (which may vary across geographic levels) and solved in multiple passes, often targeting queries expected to be large in true count in earlier passes (though this expectation is formed without examining the to-be-protected data, to avoid leaking unprotected information through the choice of query order itself)
4. The histograms generated via optimization are then used to generate a set of records for the MDF, which is then used downstream to produce the published tabulations
5. These records are then written to S3 in the MDF format. The noisy measurements are also saved for later release
6. For tuning experiments using the 2010 CEF, the output microdata may be compared against the original inputs to quantify their accuracy

After the four MDF files were generated, they are copied to AWS S3
together with the noisy measurements and other artifacts from the DAS
run. These include configuration files, execution logs, and saved
application sources and libraries. The contents of this source release
was extracted from `git` commits corresponding to the code which
generated the officially published DHC release.
