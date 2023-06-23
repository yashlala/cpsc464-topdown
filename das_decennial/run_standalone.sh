# For use with runs executed on the GAM cluster (and maybe on research2)
# Run a DAS job with:
# [config=configfile] [output=outputfile] bash run_cluster_small.sh

echo Running DAS on `date` $config

CONFIG_DIR=configs
DEFAULT_CONFIG=test_GAM_config_standalone.ini
DEFAULT_OUTPUT=das_output.out

#output=$2
if [ x$output = x ]; then
  output="$DEFAULT_OUTPUT"
fi

#config=$1
if [ x$config = x ]; then
  config="$DEFAULT_CONFIG"
fi

# If a slash was not provided, prepend the CONFIG_DIR
if [[ "$config" == *\/* ]] || [[ "$config" == *\\* ]]
then
  echo full path provided for config file $config
else
  config=$CONFIG_DIR/$config
fi

echo Running DAS with config $config
## Check to make sure the config file exists
if [ ! -r "$config" ]; then
  echo Cannot read DAS config file $config
  exit 1
fi

export TERM=xterm

## This program runs the DAS framework driver with the config file specified.
## We use qsub because of the memory and CPU requirements. Otherwise, the job
## can be killed.  --py-files das_framework/driver.py
echo starting at `date`
echo $PWD

export PATH=$PATH:$PWD
export PYSPARK_PYTHON=/usr/local/anaconda3/bin/python3
echo starting at `date`

nohup 2>&1 spark-submit --driver-memory 5g --executor-memory 3g --total-executor-cores 3 --executor-cores 3 --driver-cores 1 --num-executors 1 --conf spark.driver.maxResultSize=0g --conf spark.local.dir="/tmp/" --master local[9]  das_framework/driver.py $config --loglevel DEBUG &> $output &

echo done at `date`
