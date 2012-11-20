#!/bin/bash

if [ -z "$1" ]
then
        echo "Date required." >&2
        exit 1
fi

input="$@"
date=$(date -d "$input" "+%Y-%m-%d %R")

if [ $? -ne 0 ]
then
        echo "Invalid date." >&2
        exit 1
fi

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/config.cfg

year=$(date -d "$date" +%Y)
month=$(date -d "$date" +%m)
day=$(date -d "$date" +%d)

storage_src=$METERING_STORAGE_DIR_D/$year/$month
request_src=$METERING_REQUEST_DIR_D/$year/$month
compute_src=$METERING_COMPUTE_DIR_D/$year/$month

storage_dest=$METERING_STORAGE_DIR_M/$year/$month
request_dest=$METERING_REQUEST_DIR_M/$year/$month
compute_dest=$METERING_COMPUTE_DIR_M/$year/$month

mmkdir -p $storage_dest
mmkdir -p $request_dest
mmkdir -p $compute_dest


reduce=(
        'bzcat '
        '| awk '\''{ '
                'owners[$1] = $1;'
                'for (i = 2; i <= NF; i++) { '
                        'sums[$1,i] += $i; '
                '} '
        '} END { '
                'for (o in owners) { '
                        'printf("%s ",owners[o]); '
                        'for (i = 2; i < NF; i++) { '
                                'printf("%s ", sums[owners[o],i]); '
                        '} '
                        'printf("%s", sums[owners[o],NF]);'
                        'printf("\n"); '
                '} '
        '}'\'' '
        '| bzip2'
)

reducestr=$(printf "%s" "${reduce[@]}") # array join

name="metering-storage-monthly-$year-$month"
jobid=$(mmkjob -r "$reducestr | mpipe $storage_dest/$name.bz2" -n "$name")
mfind $storage_src | maddkeys $jobid
mjob -e $jobid

name="metering-request-monthly-$year-$month"
jobid=$(mmkjob -r "$reducestr | mpipe $request_dest/$name.bz2" -n "$name")
mfind $request_src | maddkeys $jobid
mjob -e $jobid
