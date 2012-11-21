#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $dir/../common/utils.sh

getDate "$@"

dest_dir=$MANTA_REQUEST_DEST_DAILY/$year/$month/$day

mmkdir -p $dest_dir
fatal "$?" "Error creating directory $dest_dir"

name="metering-request-daily-$year-$month-$day"
jobid=$(mmkjob -n "$name" \
               -s "$REQUEST_REDUCE_CMD_DAILY" \
               -r "dest=$dest_dir name=$name \
                        /assets/$REQUEST_REDUCE_CMD_DAILY" \
               -c "$REQUEST_NUM_REDUCERS_DAILY"
)

fatal "$?" "Error creating job $name"

$REQUEST_KEYGEN_DAILY $date | maddkeys $jobid

mjob -e $jobid
fatal "$?" "Error ending job $jobid"
