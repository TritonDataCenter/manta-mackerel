#!/bin/bash
# for future reference: msplit hour%numReducers to pick a reducer

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
storage_path=$METERING_STORAGE_DIR/$year/$month/$day
request_path=$METERING_REQUEST_DIR/$year/$month/$day
compute_path=$METERING_COMPUTE_DIR/$year/$month/$day

reduce=(
        'awk '\''{ '
                'owners[$1] = $1;'
                'for (i = 2; i <= NF; i++) { '
                        'sums[$1,i] += $i; '
                '} '
        '} END { '
                'for (o in owners) { '
                        'printf("%s ",owners[o]); '
                        'for (i = 2; i <= NF; i++) { '
                                'printf("%s ", sums[ownerse[o],i]); '
                        '} '
                        'printf("\n"); '
                '} '
        '}'\'' | '
        'bzip2'
)

reducestr=$(printf "%s" "${reduce[@]}") # array join

name="metering-storage-$year-$month-$day"
jobid=$(mmkjob -r "$reducestr | mpipe $storage_path/metering-storage-$year-$month-$day.bz2" -n "$name")
$(mls $storage_path/$(mls $storage_path | json -ga name) | json -ga name) | awk "{print $storage_path/\$1}" | maddkeys $jobid

name="metering-request-$year-$month-$day"
jobid=$(mmkjob -r "$reducestr | mpipe $request_path/metering-request-$year-$month-$day.bz2" -n "$name")
$(mls $request_path/$(mls $request_path | json -ga name) | json -ga name) | awk "{print $request_path/\$1}" | maddkeys $jobid
