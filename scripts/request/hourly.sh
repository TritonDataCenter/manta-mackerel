#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $dir/../common/utils.sh

getDate "$@"

dest_dir=$MANTA_REQUEST_DIR_HOURLY/$year/$month/$day/$hour

mmkdir -p $dest_dir
fatal "$?" "Error creating directory $dest_dir"

name="metering-request-hourly-$year-$month-$day-$hour"
jobid=$(mmkjob -n "$name" \
               -s "$REQUEST_MAP_CMD_HOURLY" \
               -s "$REQUEST_REDUCE_CMD_HOURLY" \
               -m "/assets/$REQUEST_MAP_CMD_HOURLY" \
               -r "dest=$dest_dir name=$name \
                        /assets/$REQUEST_REDUCE_CMD_HOURLY" \
               -c "$REQUEST_NUM_REDUCERS_HOURLY"
)

fatal "$?" "Error creating job $name"

$REQUEST_KEYGEN_HOURLY $date | maddkeys $jobid

mjob -e $jobid
fatal "$?" "Error ending job $jobid"
