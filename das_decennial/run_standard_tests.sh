#!/bin/bash

# Outputing stdout and stderr to specific output files to ensure the output
# doesn't propagate back up to Jenkins (which displays stdout)
#bash run_user007 fast 1> fast_output.out 2>&1
REPOSITORY_NAME=${PWD##*/} # grabs the name of the current dir
export REPOSITORY_NAME

config=configs/PL94/jenkins_topdown_RI.ini RUN_CONFIGURATION=jenkins \
	bash run_cluster.sh --fg --dry-run
echo 'Dry run complete, beginning normal run'
config=configs/PL94/jenkins_topdown_RI.ini RUN_CONFIGURATION=jenkins \
	bash run_cluster.sh --fg
