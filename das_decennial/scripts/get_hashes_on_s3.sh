#!/usr/bin/env bash

# if no file provided, get all files from s3
if [[ $# -eq 1 ]]
then
    echo "usage: bash validate_cef_version.sh path/to/s3/loc/after/DAS_S3ROOT"
fi

loc=$DAS_S3ROOT/$1/
files=`aws s3 ls $loc | awk '{print $NF}'`
if [[ -z "$files" ]]
then
    echo "no files in $loc on s3"
    exit 1
fi

for f in ${files}
do
    aws s3 cp $loc$f ./ > /dev/null

    checksum=`shasum $f | awk '{print $1}'`
    echo "$f: $checksum"

    rm $f
done