#!/bin/bash
# Run a DAS job with:
# [config=configfile] [output=outputfile] bash run_cluster.sh  [--all] [--fg|--bg] [--help]
#
# --fg   - run DAS in the foreground
# --bg   - run DAS in background withoutput to $output
# --fgo  - run DAS in foreground with no capture of output
# --help - print help
# --all  - use all resources
# --dry-run - specify the --dry-run option
# --pdf_config - only generate a PDF certificate of the config file
# --dry-read - specifies the --dry-read file
# --dry-write  - specifies the --dry-write file
#
# NOTE: This is the master script for running a cluster.
#       Your run scripts should just set variables and then call this one.
#
#ENDOFHELP

VERSION=1.7

show_help() {
    SCRIPT=$(readlink -f "$0")  # script fill path
    HELPLINES=$(grep -nhT ENDOFHELP "$SCRIPT" | head -1 | cut -f 1) # line number help appears on
    head --lines "${HELPLINES}" "$SCRIPT"
    exit 0
}

PYTHON=$PYSPARK_DRIVER_PYTHON
SPARK_SUBMIT=/usr/bin/spark-submit
LOGLEVEL="--loglevel DEBUG"
#LOGLEVEL="--loglevel WARNING"
tagger_Success="PASS"

# A righteous umask
umask 022

# Set Tagger to True by default
if [[ -z "$TAGGER_ENABLE" ]]; then
  echo "S3 Tagger value was unset, defaulting it to TRUE"
  export TAGGER_ENABLE="TRUE"
fi

#
# Process the command-line options that are passed in as variables
#

# Option processing
# https://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash
# Process arguments
RUNMODE=fg
ALL=N
DEFAULT_OUTPUT=das_output.out
DRY=''
PDF_ONLY=''
SPARK=YES

while [[ "$#" -gt 0 ]];
do
    case $1 in
   --bg)      RUNMODE=bg;;
   --fg)      RUNMODE=fg;;
   --fgo)     RUNMODE=fgo;;
   --dry-run)   DRY="$DRY--dry-run";;
   --dry-read)  DRY="$DRY --dry-read $2"  ; shift ;;
   --dry-write) DRY="$DRY --dry-write $2" ; shift ;;
   --pdf_only)  PDF_ONLY='--pdf_only certificate.pdf' ;;
   --pdf_only) PDF_ONLY='--pdf_only certificate.pdf' ;;
   --sparklens) SPARKLENS_ENABLE=True ;;
   --help)      show_help; exit 0;;
   *) echo "Unknown parameter: $1"; show_help; exit 1;;
    esac;
    shift;
done

#
# check to see if the 'output' and 'config' environment variables were not set.
# If they were not set, generate an appropriate error.
# Note that these environment variables are lower case for historical reasons.
#
if [ x$output = x ]; then
  output="$DEFAULT_OUTPUT"
  echo using default output $output
fi

if [ x$SPARKLENS_VERSION = x ]; then
  SPARKLENS_VERSION="2.12-0.4.0"
  echo using default output $output
fi

if [ x$SPARKLENS_ENABLE = x ]; then
  echo Sparklens is disabled.
  echo To Enable SparkLens execute the following:
  echo export SPARKLENS_ENABLE=True
else
  S3_BASE_OUTPUT="$($PYTHON ${DAS_HOME}/das_decennial/das2020_driver.py --get writer:output_path $config)/sparklens/logs"
  SPARKLENS_OPTIONS="--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
                     --conf spark.sparklens.reporting.disabled=false
                     --conf spark.sparklens.data.dir=$S3_BASE_OUTPUT
                     --jars $DAS_HOME/dist/jar/sparklens-${SPARKLENS_VERSION}-spark3.jar"
fi

################################################################
#
# Run enviornment
CLUSTER_INFO=../bin/cluster_info.py
if [ ! -r $CLUSTER_INFO ]; then
    CLUSTER_INFO=/mnt/gits/das-vm-config/bin/cluster_info.py
fi

get_ci() {
    $PYTHON $CLUSTER_INFO --get $1
}

if [ -z "${DAS_TIER}" ]; then
    echo Please set the DAS_TIER variable. Exiting...
    exit 1
fi

if [ "${DAS_TIER}" != ITE ]; then
    # Note in ITE
    echo operating in $DAS_ENVIRONMENT
    export JBID=bond007
    export DAS_HOME=/mnt/gits/das-vm-config
else
    if [ -z "${JBID}" ]; then
        echo Please set the JBID environment variable. Exiting...
        exit 1
    fi

    if [ -z "${DAS_HOME}" ]; then
        echo DAS_HOME not set. Assuming /mnt/gits/das-vm-config
        export DAS_HOME=/mnt/gits/das-vm-config
    fi
fi

export DAS_OBJECT_CACHE=$(get_ci DAS_S3MGMT)/$(get_ci DAS_OBJECT_CACHE_BASE)
export DVS_OBJECT_CACHE=$(get_ci DAS_S3MGMT)/$(get_ci DVS_OBJECT_CACHE_BASE)
export DAS_S3MGMT_ACL=$(get_ci DAS_S3MGMT_ACL)
export DVS_AWS_S3_ACL=$(get_ci DAS_S3MGMT_ACL)
MISSION_REPORT="${DAS_DASHBOARD_URL}/app/mission"


# Command to send things to the dashboard:
send_dashboard() {
    $PYTHON programs/dashboard.py $* || exit 1
}


################################################################
# Validation

## First make sure that there are no syntax or import errors
if ! $PYTHON das2020_driver.py --help >/dev/null ; then
    echo das2020_driver.py contains syntax errors. STOP. >/dev/stderr
    exit 1
fi

if [ x$config = x ]; then
  echo Please specify a config file.
  exit 1
fi

################################################################
#
# Config file validation.
# Make sure we have a config file and that it is valid.
#
CONFIG_DIR=configs

#
# If a slash was not provided in the config file name, prepend the CONFIG_DIR
#
if [[ $config == */* ]] ; then
  echo Config specified with full path: $config
else
  config=$CONFIG_DIR/$config
fi

## Check to make sure the config file exists
if [ ! -r "$config" ]; then
  echo Cannot read DAS config file $config
  exit 1
else
    echo Using config file: $config
fi

if [ -z $NO_CHECK_CLEAN ]; then
    unclean=$(git status --porcelain| grep -v das_framework | grep -v '^\?')
    if [ ! -z "$unclean" ]; then
        echo
        python3 programs/colors.py CBLUEBG CWHITE --message="git is not clean. Will not run DAS."
        echo
        git status | grep -v das_framework
        echo
        python3 programs/colors.py CBLUEBG CWHITE --message="Please commit modified files."
        echo
        echo NOTE: You can suppress this check by setting the environment variable NO_CHECK_CLEAN
        exit 1
    fi
fi

# Make sure that there are no syntax or import errors in the python code
if ! $PYTHON das2020_driver.py --help >/dev/null ; then
    echo das2020_driver.py contains syntax or import errors. STOP. >/dev/stderr
    exit 1
fi

#
# Spark configuration
#
# Set these variables if they are not set

YARNLIST=/tmp/yarnlist$$
yarn application -list > $YARNLIST 2>&1
if grep '^Total number.*:0' $YARNLIST ; then
    /bin/rm $YARNLIST
    echo No other Yarn jobs are running.
else
    tput rev          # reverse video
    echo OTHER YARN JOBS ARE RUNNING
    tput sgr0         # regular video
    cat $YARNLIST
    echo

    # See if we are running in LightsOut
    LightsOut=$(HTTPS_PROXY=$BCC_HTTPS_PROXY aws emr describe-cluster --cluster-id $CLUSTERID \
        --query 'Cluster.[Tags[?Key==`LightsOut`].Value[][]]' --output text)
    if [ "${LightsOut:0:1}" = "T" ]; then
        echo RUNNING LIGHTS OUT. CONTINUING
    else
        tput setaf 5  # purple
        echo Type return to continue execution, or control-C to abort.
        tput sgr0     # regular video
        read
    fi
fi
/bin/rm -f $YARNLIST

if [ -z "${EXECUTOR_CORES}" ];           then EXECUTOR_CORES=4 ; fi
if [ -z "${EXECUTORS_PER_NODE}" ];       then EXECUTORS_PER_NODE=4 ; fi
if [ -z "${DRIVER_CORES}" ];             then DRIVER_CORES=10 ; fi
if [ -z "${DRIVER_MEMORY}" ];            then DRIVER_MEMORY=5g ; fi
if [ -z "${EXECUTOR_MEMORY}" ];          then EXECUTOR_MEMORY=16g ; fi
if [ -z "${EXECUTOR_MEMORY_OVERHEAD}" ]; then EXECUTOR_MEMORY_OVERHEAD=20g ; fi
if [ -z "${SHUFFLE_PARTITIONS}" ];       then SHUFFLE_PARTITIONS=1000 ; fi
if [ -z "${DYNAMIC_ALLOCATION}" ];       then DYNAMIC_ALLOCATION=false ; fi

# Note: We need to add the desired number of partitions as the second input argument of each spark join
# operation throughout our code before setting DEFAULT_PARALLELISM:
# if [ -z ${DEFAULT_PARALLELISM+x} ];      then DEFAULT_PARALLELISM=1000 ; fi

## Decide how many executors to run based on how many nodes are avaialble
NODES=`yarn node -list 2>/dev/null | grep RUNNING | wc -l`
echo AVAILABLE NODES: $NODES

if [ -z "${NUM_EXECUTORS}" ]; then
   NUM_EXECUTORS=$(( $EXECUTORS_PER_NODE * $NODES ))
fi

# for CORE=3 m4.16xlarge and MASTER = m4.2xlarge
# maxResultsSize is the max that will be sent to the driver. It must fit in memory

# If SPARK_RESOURCES is set, use that.
# Otherwise create the string based on our values set above

echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo " ++ SPARK CONFIGURATION  "
if [ x$SPARK_RESOURCES = x ]; then
    SPARK_RESOURCES="--driver-memory $DRIVER_MEMORY
 --num-executors $NUM_EXECUTORS
 --executor-memory $EXECUTOR_MEMORY --executor-cores $EXECUTOR_CORES
 --driver-cores $DRIVER_CORES --conf spark.driver.maxResultSize=0g
 --conf spark.executor.memoryOverhead=$EXECUTOR_MEMORY_OVERHEAD
 --conf spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS
 --conf spark.dynamicAllocation.enabled=$DYNAMIC_ALLOCATION"

    echo DRIVER_MEMORY: $DRIVER_MEMORY
    echo NUM_EXECUTORS: $NUM_EXECUTORS
    echo EXECUTOR_MEMORY: $EXECUTOR_MEMORY
    echo EXECUTOR_MEMORY_OVERHEAD: $EXECUTOR_MEMORY_OVERHEAD
    echo EXECUTOR_CORES: $EXECUTOR_CORES
    echo DRIVER_CORES: $DRIVER_CORES
    echo DYNAMIC_ALLOCATION: $DYNAMIC_ALLOCATION
    echo SHUFFLE_PARTITIONS: $SHUFFLE_PARTITIONS
else
    echo SPARK_RESOURCES: $SPARK_RESOURCES
fi
echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"


#
# Now that we have a config file, try to find the python
# Notice that we assume that we are using the same
#
# Make sure that das2020_driver.py supports --get
#
if ! $PYTHON das2020_driver.py --get python:executable:$PYSPARK_PYTHON $config >/dev/null ; then
    echo das2020_driver.py does not support --get or an included config file not found >/dev/stderr
    exit 1
fi

# get_config():
# get a value out of the config file
# usage:
# get_config <section>:<option>:<default>
get_config() {
    $PYTHON das2020_driver.py --get $1 $config
}

export PYSPARK_PYTHON=$(get_config python:executable:$PYSPARK_PYTHON)
export PYSPARK_DRIVER_PYTHON=$(get_config python:executable:$PYSPARK_DRIVER_PYTHON)

echo PYSPARK_PYTHON: $PYSPARK_PYTHON


if [ ! -r $PYSPARK_PYTHON ]; then
    echo "Specified PYSPARK_PYTHON version is not installed: $PYSPARK_PYTHON "
    exit 1
fi

if [ ! -r $PYSPARK_DRIVER_PYTHON ]; then
    echo "Specified PYSPARK_DRIVER_PYTHON version is not installed: $PYSPARK_PYTHON "
    exit 1
fi

################################################################
## Resize the cluster if requested
##
resize_cmd=''
emr_task_nodes=$(get_config setup:emr_task_nodes:"")
echo emr_task_nodes: $emr_task_nodes
if [[ ! -z $emr_task_nodes ]]; then
    resize_cmd+=" --task $emr_task_nodes "
fi

emr_core_nodes=$(get_config setup:emr_core_nodes:"")
if [[ ! -z $emr_core_nodes ]]; then
    resize_cmd+=" --core $emr_core_nodes "
fi

if [[ ! -z $emr_core_nodes ]]; then
    echo resizing cluster $resize_cmd
    $PYTHON programs/emr_control.py $resize_cmd | tee /tmp/resize$$
    grep RESIZE /tmp/resize$$ && (echo Initiating cluster resizing. Waiting 60 seconds; sleep 60)
    /bin/rm -f /tmp/resize$$
fi


################################################################
## Run the DAS
##

## Start the mission
$($PYTHON programs/dashboard.py --mission_start)

echo
echo === Running DAS ===
echo config=$config
echo START=`date -I`
echo CWD=$PWD
echo ===================
export PATH=$PATH:$PWD

send_dashboard --log MISSION $MISSION_NAME $config

# If logs does not exist, make it a symlink to /mnt/logs
if [ ! -e logs ] ; then
    mkdir -p /mnt/logs || exit 1
    ln -s /mnt/logs logs || exit 1
fi

TESTFILE=logs/test-$$
# Verify we can write to logs
echo Verifying we can write to logs
touch $TESTFILE
rm $TESTFILE
## Create the DAS command
# https://stackoverflow.com/questions/45269660/apache-spark-garbage-collection-logs-for-driver
#Add for verbose Java gargage collection (the output will be in stdout on worker nodes)
#-XX:+PrintGCDetails,-XX:+PrintGCTimeStamps
#-XX:+UseG1GC - this is a different collector, can help in some situations where garbage collection is the bottleneck.
#Note that with large executor heap sizes, it may be important to increase the G1 region size with -XX:G1HeapRegionSize
#-Xmn=4/3*E where E is estimated Eden size sets the memory dedicated to the Young generation (extra 1/3 is for Survivor1 and Survivor2)

#--conf spark.memory.fraction=0.2
#--conf spark.executor.pyspark.memory=100K

#--conf spark.default.parallelism=1000
#--conf spark.sql.execution.arrow.enabled=true
#--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
#--conf spark.default.parallelism=$DEFAULT_PARALLELISM
#--conf spark.sql.shuffle.partitions=$DEFAULT_PARALLELISM

#extra_java_options="-XX:+PrintGCTimeStamps -XX:+PrintGCDetails -verbose:gc "
javaconf="--conf"

# extra_java_options="-XX:+UseG1GC -XX:-UseParallelGC -XX:-UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'"
# Use the line above instead of the following line to print more detail regarding GC to the yarn logs:
extra_java_options="-XX:+UseG1GC -XX:-UseParallelGC -XX:InitiatingHeapOccupancyPercent=35 -XX:-PrintGCDetails -XX:-PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'"

echo Starting spark
spark_submit="$SPARK_SUBMIT $SPARK_RESOURCES $SPARKLENS_OPTIONS
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.configuration.xml
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties
    --conf spark.eventLog.dir=hdfs:///var/log/spark/apps/
    --conf spark.eventLog.enabled=true
    --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.configuration.xml
    --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties
    --conf spark.hadoop.fs.s3.maxRetries=50
    --conf spark.local.dir=/mnt/tmp/
    --conf spark.network.timeout=3000s
    --conf spark.storage.blockManagerSlaveTimeoutMs=1800000
    --conf spark.executor.heartbeatInterval=30s
    --conf spark.scheduler.listenerbus.eventqueue.capacity=50000
    --conf spark.sql.execution.arrow.pyspark.enabled=true
    --conf spark.submit.deployMode=client
    --conf spark.task.maxFailures=8
    --files ./log4j.configuration.xml
    --files ./log4j.properties
    --master yarn
    --deploy-mode client
    --name $JBID:$MISSION_NAME "

python_cmd="das2020_driver.py $config $LOGLEVEL --logfilename $LOGFILE_NAME $DRY $PDF_ONLY ${CUSTOM_SET_01} ${CUSTOM_SET_02} ${CUSTOM_SET_03} ${CUSTOM_SET_04} ${CUSTOM_SET_05}"

# Print the command line and send it to the dashboard:

COMMAND_LINE=$(echo $spark_submit --conf "spark.executor.extraJavaOptions=$extra_java_options" $python_cmd)
echo
echo $COMMAND_LINE
echo $COMMAND_LINE | send_dashboard --spark_submit
echo

## save yarn logs
save_yarn_logs() {
    YARN_LOGFILE=`echo $LOGFILE_NAME | sed 's/.log$/_yarnlog.txt/'`
    APPLICATION_ID=$(cat ${LOGFILE_NAME%.log}.appid)
    echo Saving yarn logs to local $YARN_LOGFILE
    seconds=30; date1=$((`date +%s` + $seconds));
    echo Sleeping $seconds seconds...
    while [ "$date1" -ge `date +%s` ]; do
        echo -ne "$(date -u --date @$(($date1 - `date +%s` )) +%H:%M:%S)\r";
        sleep 1
    done
    /usr/bin/yarn logs -applicationId $APPLICATION_ID --size_limit_mb=-1 > $YARN_LOGFILE
    ls -l $YARN_LOGFILE
    echo Compressing yarn logs...
    head -10000 $YARN_LOGFILE > $YARN_LOGFILE.head.txt
    tail -10000 $YARN_LOGFILE > $YARN_LOGFILE.tail.txt
    gzip -1 $YARN_LOGFILE
    ls -l $YARN_LOGFILE*
}

upload_logs() {
    echo upload_logs
    # nicely format the DFXML file for viewing
    DFXML_FILE=logs/$(basename $LOGFILE_NAME .zip).dfxml
    DFXML_PP_FILE=logs/$(basename $LOGFILE_NAME .zip).pp.dfxml
    xmllint -format $DFXML_FILE > $DFXML_PP_FILE

    # Add the log and other info files to the zip file
    go2zip_template=$(basename $LOGFILE_NAME .log)  # basename of the logfile name without the .log extension
    ZIP_FILENAME=$go2zip_template.zip
    (cd logs; zip -uv $ZIP_FILENAME $go2zip_template*)
    zip -uv logs/$ZIP_FILENAME $output

    # Show the contents of the ZIP file
    echo
    unzip -l logs/$ZIP_FILENAME

    # These individual POINTs may seem odd, but when I took them out the mission report didn't
    # show up. I think that it's some buffer not getting flushed before the monitoring program
    # exists. So just leave them in. There is so much other junk.

    echo POINT1

    # Upload to s3
    aws s3 cp --no-progress logs/$ZIP_FILENAME $DAS_S3ROOT/rpc/upload/logs/

    echo POINT2

    DAS_S3=$(echo $DAS_S3ROOT | sed 's;s3://;;')

    echo POINT3

    END_TIME=$(date -Iseconds)

    echo POINT4

    # Get S3 errors from AWS CloudWatch, print them, and optionally send them to the log
    python3 programs/aws_errors.py $START_TIME $END_TIME --upload
    echo ===
    echo ===
    echo mission report:   $MISSION_REPORT/$MISSION_NAME/report
    echo ===
    echo ===
}

##
## mission failure command
## Todo: refactor this so that we always save_yarn_logs and upload_logs and *then* check the ailure code.
##
mission_failure() {
    echo =========================
    echo ==== MISSION FAILURE ====
    echo =========================
    date
    echo $MISSION_NAME failed with exit code $1
    echo processing stops.
    echo
    save_yarn_logs
    echo Incomplete spark logs might be found at /mnt/tmp/logs/{applicationId}{inProgressFlag}
    echo
    upload_logs
    echo
    send_dashboard --exit_code=$1
    echo mission report:   $MISSION_REPORT/$MISSION_NAME/report
    tagger_Success="FAIL"
    exit 1
}

## The command to run the cluster, make the zipfile, and upload the zipfile to S3
runcluster() {
    mkdir -p /mnt/logs
    START_TIME=$(date -Iseconds)
    mkdir -p $(dirname $LOGFILE_NAME)
    $spark_submit --conf "spark.executor.extraJavaOptions=$extra_java_options" $python_cmd || mission_failure $?
    save_yarn_logs

    echo $0: PID $$ done at `date`. DAS is finished. Uploading Yarn logs...
    upload_logs
    echo runcluster finished
}

case $RUNMODE in
    bg)
        echo running in background
        (runcluster > >(send_dashboard --tee --code CODE_STDOUT) \
            2> >(send_dashboard --tee --red --code CODE_STDERR)) \
            >$output 2>&1 </dev/null &
        echo PID $$ done at `date`. DAS continues executing...
        echo watch output with:
        echo tail -f $output
        ;;
    fg)
        echo "Running in foreground. Logfile: $LOGFILE_NAME"
        runcluster \
             > >(send_dashboard --tee       --code CODE_STDOUT --stdout --write logs/$(basename $LOGFILE_NAME .log).stdout) \
            2> >(send_dashboard --tee --red --code CODE_STDERR --stderr --write logs/$(basename $LOGFILE_NAME .log).stderr)
        ;;
    fgo)
        echo "Running in foreground with no stdout or stderr capture."
        runcluster
        ;;
    *)
        echo "Unknown run mode: $RUNMODE"
        exit 1
esac

# Finally wait for all subprocesses to finish, so that the prompt is visible
wait

if [[ "$TAGGER_ENABLE" = "TRUE" ]]; then
  # Run the S3 Tagger
  # Capture the S3 output location
  export S3_BASE_OUTPUT=$($PYTHON ${DAS_HOME}/das_decennial/das2020_driver.py --get writer:output_path $config)
  if [[ -z "${S3_BASE_OUTPUT}" ]]; then
    echo "S3_BASE_OUTPUT location is not set, Tagger will be skipped, as we the base location is unknown."
  else
    ## Config files have the S3Bucket, and the prefix s3:// prefixed, this removes that bit of code from the config file name to get just the prefix.
    # 3_BASE_OUTPUT=s3://uscb-decennial-ite-das/DHC_TestProduct/TestMUD/     ==>   DHC_TestProduct/TestMUD
    s3prefix=$($PYTHON -c "var='$S3_BASE_OUTPUT'; var = '/'.join(var[5:].split('/')[1:]) if var.startswith('s3://') else var; var = var[:-1] if var.endswith('/') else var;  print(var)")
    s3bucket=$($PYTHON -c "var='$S3_BASE_OUTPUT'; var = var[5:].split('/')[0] if var.startswith('s3://') else var; var = var[:-1] if var.endswith('/') else var;  print(var)")

    if [[ -z "${TAG_CONFIG_FILE}" ]]; then
      if [[ -z "${RUN_CONFIGURATION}" ]]; then
        export TAG_CONFIG_FILE=${DAS_HOME}/das_decennial/s3tagger/configs/default.ini
      else
        if [[ ${RUN_CONFIGURATION} == "production/pl94" ]]; then
          export TAG_CONFIG_FILE=${DAS_HOME}/das_decennial/s3tagger/configs/pl94.ini
        elif [[ ${RUN_CONFIGURATION} == "production/dhc" ]]; then
          export TAG_CONFIG_FILE=${DAS_HOME}/das_decennial/s3tagger/configs/dhc.ini
        else
          export TAG_CONFIG_FILE=${DAS_HOME}/das_decennial/s3tagger/configs/default.ini
        fi
      fi
    fi
    start_time=$(date +%s)

    # Multiple variables exist and are used in the tagging process.  These variables may or may not be defined, but are not defined in this file.
    # During EMR / Enterprise runs they will be defined in the environment and if they are not provided here, they are extracted from the
    # environment during run time.  Please see the following list of options, corresponding Environment Variable, and the default value.
    #
    #  CMD Line Option  |    Environment Variable           |      Default                                           |   Required    | Notes
    #  --bucket         |            N/A                    |        N/A                                             |     True      | Extracted above and formatted for consumption.  Format is:  uscb-decennial-ite-das  [Note: no s3:// or trailing /
    #  --prefix         |            N/A                    |        N/A                                             |     True      | Extracted above and formatted for consumption.  This is the base location for tagging operations, and is Required.
    #  --config         |            N/A                    | ${DAS_HOME}/das_decennial/s3tagger/configs/default.ini |     True      | Provides the tagging rules for execution.
    #  --RunConfiguration |    $RUN_CONFIGURATION           |        Unknown                                         |     False     | Known Values: PL94, PL94-PER_US, PL94-PER_PR, PL94-UNIT_US, PL94-UNIT_PR, DHC, DHC-PER_US, DHC-PER_PR, DHC-UNIT_US, DHC-UNIT_PR
    #  --RunType          |    $RunType                     |        Unknown                                         |     False     | Development, Test, Experiment, Production
    #  --DasName          |    $DAS_NAME                    |        Unknown                                         |     False     | Provided by the CFT, or extracted from the Environment variables
    #  --DasRelease       |    $DAS_RELEASE                 |        Unknown                                         |     False     | Extracted from the Execution Environment
    #  --DasEmrVersion    |    N/A                          |        Unknown                                         |     False     | Extracted from the Execution Environment through AWS CLI
    #  --DataSource       |    $DATA_SOURCE                 |        Unknown                                         |     False     | Provided by the CFT, or extracted from the Environment variables
    #  --Protect          |    $TAGGER_PROTECT              |        None                                            |     False     | Provided by the CFT, or extracted from the Environment variables.  This will apply the protect tag to all of the files to disable deletion.
    #  --POC              |    $(whoami)                    |        $(whoami)                                       |     False     | If the user is 'hadoop' (normal CFT execution), the POC tag from the CFT is used, otherwise, the value of whoami will be used.
    #  --Success          |    N/A                          |        Pass                                            |     True      | Extracted from the run_cluster.sh script during execution to signal a successful execution of the spark call, or a mission failure.

    echo "${DAS_HOME}/das_decennial/s3tagger/S3_Tag_Utility.sh --tag-objects --bucket ${s3bucket} --prefix ${s3prefix} --config ${TAG_CONFIG_FILE} --Success ${tagger_Success} --RunConfiguration ${RUN_CONFIGURATION}"
    if ! (${DAS_HOME}/das_decennial/s3tagger/S3_Tag_Utility.sh --tag-objects --bucket ${s3bucket} --prefix ${s3prefix} --config ${TAG_CONFIG_FILE} --Success ${tagger_Success} --RunConfiguration ${RUN_CONFIGURATION}); then
      echo "Failed to Tag Data Properly"
      exit 1
    fi

    end_time=$(date +%s)
    elapsed=$((end_time - start_time))
    echo ""
    echo "=================================================="
    echo "Tagging Complete - RunTime: ${elapsed}"
    echo "=================================================="
  fi
else
  echo "S3 Tagging is disabled"
fi

echo "Run Cluster is Complete"
