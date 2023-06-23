#!/usr/bin/env bash

# if no file provided, get all files from s3

loc=$DAS_S3INPUTS/mft/cdl-to-das/

if [[ $# -eq 0 ]]
then
    files=`aws s3 ls $loc | awk '{print $NF}'`

# if two argument provided, parse and check
elif [[ $# -eq 1 ]]
then

    # check filename has .zip in it
    if [[ "${1: -4}" != ".zip" ]]
    then
        echo "must be a zip file"
        exit 1
    fi

    file=`aws s3 ls $loc$1`
    if [[ -z "$file" ]]
    then
        echo "no such file on s3"
        exit 1
    fi
    files=$1
# otherwise, illegal number of arguments
else
    echo "usage: bash validate_cef_version.sh [-a OR -f filename.zip OR -l path/to/s3/loc/after/DAS_S3ROOT/]"
    exit 1
fi

for f in ${files}
do
    echo ${f}

    # download cef, unzip
    dir=${f%%.*}
    mkdir $dir
    aws s3 cp $loc$f ./$dir/ > /dev/null
    cd $dir
    unzip ${f} > /dev/null

    # set start and end of all name ids
    start="CEF20_"
    end="_${dir: -2}"

    # get version in count
    mid="CNT"
    file=`ls | grep $start$mid$end`
    cnt_ver=`head $file | awk '{print $2}'`
    echo "    count file version   : $cnt_ver"
    cnt_chksm=`shasum $file | awk '{print $1}'`
    echo "    count file checksum  : $cnt_chksm"

    # persons
    mid="PER"
    file=`ls | grep $start$mid$end`
    if [[ `wc -l $file | awk '{print $1}'` -ne `grep '^.................   [0-9 ][0-9]' $file | wc -l` ]]
    then
        echo "    version differs throughout $file"
        exit 1
    fi
    per_ver=`head -1 $file | grep -o '^.................   [0-9 ][0-9]' | awk '{print $NF}'`
    echo "    persons file version : $per_ver"
    per_chksm=`shasum $file | awk '{print $1}'`
    echo "    persons file checksum: $per_chksm"

    # units
    mid="UNIT"
    file=`ls | grep $start$mid$end`
    #'^............    0'
    if [[ `wc -l $file | awk '{print $1}'` -ne `grep '^............   [0-9 ][0-9]' $file | wc -l` ]]
    then
        echo "    version differs throughout $file"
        exit 1
    fi
    unt_ver=`head -1 $file | grep -o '^............   [0-9 ][0-9]' | awk '{print $NF}'`
    echo "    units file version   : $unt_ver"
    unt_chksm=`shasum $file | awk '{print $1}'`
    echo "    units file checksum  : $unt_chksm"

    cd ..
    rm -rf $dir

done
