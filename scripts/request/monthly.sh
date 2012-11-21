#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $dir/../common/utils.sh

getDate "$@"

dest_dir=$MANTA_REQUEST_DEST_MONTHLY/$year/$month

mmkdir -p $dest_dir
fatal "$?" "Error creating directory $dest_dir"

name="metering-request-monthly-$year-$month"
jobid=$(mmkjob -n "$name" \
               -s "$REQUEST_REDUCE_CMD_MONTHLY" \
               -r "dest=$request_dest name=$name \
                        /assets/$REQUEST_REDUCE_CMD_MONTHLY" \
               -c "$REQUEST_NUM_REDUCERS_MONTHLY"
)

fatal "$?" "Error creating job $name"

$REQUEST_KEYGEN_MONTHLY $date | maddkeys $jobid

mjob -e $jobid
fatal "$?" "Error ending job $jobid"
