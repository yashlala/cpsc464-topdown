#! /bin/bash

INPUT_UNIT_US=FAST_MOM_DHCH_US.ini
INPUT_UNIT_PR=IRON_FISTED_DEVELOPMENT_DHCH_PR.ini
INPUT_PERSON_PR=MYSTIFIED_SENTENCE_DHCP_PR.ini
INPUT_PERSON_US=PEDESTRIAN_SPECIAL_DHCP_US.ini

CANDIDATE_BASE=ddp2_candidate1


UNIT_US=${CANDIDATE_BASE}/unit_US.ini
UNIT_PR=${CANDIDATE_BASE}/unit_PR.ini
PERSON_US=${CANDIDATE_BASE}/person_US.ini
PERSON_PR=${CANDIDATE_BASE}/person_PR.ini

echo "The following files were provided ..."
echo "UNIT_US ......... $INPUT_UNIT_US"
echo "UNIT_PR ......... $INPUT_UNIT_PR"
echo "PERSON_US ....... $INPUT_PERSON_US"
echo "PERSON_PR ....... $INPUT_PERSON_PR"

rm -rf ${CANDIDATE_BASE}
mkdir ${CANDIDATE_BASE}

# Copy inputs to output location for modification
cp ${INPUT_UNIT_US} ${UNIT_US}
cp ${INPUT_UNIT_PR} ${UNIT_PR}
cp ${INPUT_PERSON_US} ${PERSON_US}
cp ${INPUT_PERSON_PR} ${PERSON_PR}

# Scrub the variables for commmon env variable paths
./config_compare.py --scrub ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
mv ${UNIT_US}.scrubbed ${UNIT_US}
mv ${UNIT_PR}.scrubbed ${UNIT_PR}
mv ${PERSON_US}.scrubbed ${PERSON_US}
mv ${PERSON_PR}.scrubbed ${PERSON_PR}

# Remove the git commit messages
./config_compare.py --delete "reader:git_commit" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:das_run_uuid" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:das_environment" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:bcc_http_proxy" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:bcc_https_proxy" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:clusterid" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:bcc_no_proxy" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:jbid" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:friendly_name" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:no_proxy" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:mission_name" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:master_ip" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}
./config_compare.py --delete "environment:spark_local_ip" ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}

# rewrite the output paths for each file type
./config_compare.py --set 'writer:output_path:$MDF_UNIT_SINGLE_STATE_PATH' ${UNIT_PR}
./config_compare.py --set 'writer:output_path:$MDF_UNIT_US_PATH' ${UNIT_US}
./config_compare.py --set 'writer:output_path:$MDF_PER_SINGLE_STATE_PATH' ${PERSON_PR}
./config_compare.py --set 'writer:output_path:$MDF_PER_US_PATH' ${PERSON_US}

# rewrite output_datafile_name
./config_compare.py --set 'writer:output_datafile_name:$MDF_UNIT_SINGLE_STATE_NAME' ${UNIT_PR}
./config_compare.py --set 'writer:output_datafile_name:$MDF_UNIT_US_NAME' ${UNIT_US}
./config_compare.py --set 'writer:output_datafile_name:$MDF_PER_SINGLE_STATE_NAME' ${PERSON_PR}
./config_compare.py --set 'writer:output_datafile_name:$MDF_PER_US_NAME' ${PERSON_US}

# rewrite spark.name
./config_compare.py --set 'setup:spark.name:${DHCH_NAME}-${SINGLE_STATE_NAME}' ${UNIT_PR}
./config_compare.py --set 'setup:spark.name:${DHCH_NAME}-US' ${UNIT_US}
./config_compare.py --set 'setup:spark.name:${DHCP_NAME}-${SINGLE_STATE_NAME}' ${PERSON_PR}
./config_compare.py --set 'setup:spark.name:${DHCP_NAME}-US' ${PERSON_US}

# rewrite Person.path
./config_compare.py --set 'reader:Person.path:$CEF_PER_SINGLE_STATE_PATH' ${PERSON_PR} ${UNIT_PR}
./config_compare.py --set 'reader:Person.path:$CEF_PER_US_PATH' ${PERSON_US} ${UNIT_US}
./config_compare.py --set 'reader:person.path:$CEF_PER_SINGLE_STATE_PATH' ${PERSON_PR} ${UNIT_PR}
./config_compare.py --set 'reader:person.path:$CEF_PER_US_PATH' ${PERSON_US} ${UNIT_US}

# rewrite Unit.path
./config_compare.py --set 'reader:Unit.path:$CEF_UNIT_SINGLE_STATE_PATH' ${PERSON_PR} ${UNIT_PR}
./config_compare.py --set 'reader:Unit.path:$CEF_UNIT_US_PATH' ${PERSON_US} ${UNIT_US}
./config_compare.py --set 'reader:unit.path:$CEF_UNIT_SINGLE_STATE_PATH' ${PERSON_PR} ${UNIT_PR}
./config_compare.py --set 'reader:unit.path:$CEF_UNIT_US_PATH' ${PERSON_US} ${UNIT_US}

# rewrite GRFC
./config_compare.py --set 'reader:grfc_path:$GRFC_SINGLE_STATE_PATH' ${UNIT_PR}
./config_compare.py --set 'reader:grfc_path:$GRFC_US_PATH' ${UNIT_US}
./config_compare.py --set 'reader:grfc_path:$GRFC_SINGLE_STATE_PATH' ${PERSON_PR}
./config_compare.py --set 'reader:grfc_path:$GRFC_US_PATH' ${PERSON_US}

# Sort the output of the config files for ease of use / reading
./config_compare.py --sort ${UNIT_US} ${UNIT_PR} ${PERSON_US} ${PERSON_PR}


echo "The following files were generated ..."
echo "UNIT_US ......... $UNIT_US"
echo "UNIT_PR ......... $UNIT_PR"
echo "PERSON_US ....... $PERSON_US"
echo "PERSON_PR ....... $PERSON_PR"