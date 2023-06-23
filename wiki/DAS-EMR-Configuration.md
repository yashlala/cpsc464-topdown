The Disclosure Avoidance System (DAS) makes use of high-performance computing to process information about the estimated 330 million individuals and 140 million households collected by the 2020 Census. The DAS is implemented in Python using the PySpark parallel computing library powered by [Amazon's Elastic Map Reduce](https://aws.amazon.com/emr/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) (EMR) service. The EMR service launches a compute cluster consisting of [a single master node (though recent EMR updates allow for multiple master nodes) and a variable number (2-100) of core and task nodes](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-master-core-task-nodes.html).

Typically, DAS uses EMR nodes provisioned with [96 vCPUs, 768GB of RAM](https://aws.amazon.com/ec2/instance-types/r5/), and 4TiB of disk space (Amazon instance types `r5.24xlarge` or `r4d.24xlarge`), though this could reasonably be made smaller or larger (for the very few larger classes of EMR nodes available), depending on the target histogram/schema and product.

The EMR configuration is divided into three components: node configuration, Spark configuration, and Java configuration. These rely on files and scripts fetched from Amazon S3.

Node configuration consists of a sequence of six DAS bootstrap scripts applied to the nodes launched from the standard AWS EMR instance image which do the following:
  * install and update certificates
  * download DAS software from S3 and install the necessary DAS components, Python libraries, and software tools on each node
  * install and configure security software and perform hardening on the operating system
  * configure necessary cluster permissions and keys and start required servers and daemons
  * set up the license configuration for the Gurobi optimizer
  * apply other DAS-specific node settings

The bootstrap configuration file can be found in `/etc/profile.d/census_das.sh` where it can be included by shell or Python scripts.

In order to run reliably, we've found it necessary to modify some of the default Java configurations used by EMR.

Spark and Java configurations can be defined in the run cluster script.
  * Spark configuration section in run cluster
    We have to set variables in spark resources section if they are not set default.
  * Find the right python driver for spark and spark submit location.
    PYSPARK_DRIVER_PYTHON, SPARK_SUBMIT
  * Assign default values to below variables
    EXECUTOR_CORES
    EXECUTORS_PER_NODE
    DRIVER_CORES
    DRIVER_MEMORY
    EXECUTOR_MEMORY
    EXECUTOR_MEMORY_OVERHEAD
    DEFAULT_PARALLELISM
  * EMR comes with some default java configurations, but we typically specify extra options. See sample extra java options below
    -XX:+UseG1GC
    -XX:+UnlockDiagnosticVMOptions
    -XX:+G1SummarizeConcMark
    -XX:InitiatingHeapOccupancyPercent=35
    -verbose:gc
    -XX:+PrintGCDetails
    -XX:+PrintGCDateStamps
    -XX:OnOutOfMemoryError
