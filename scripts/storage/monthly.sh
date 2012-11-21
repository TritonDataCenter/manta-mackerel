#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $dir/../common/utils.sh

getDate "$@"

dest_dir=$MANTA_STORAGE_DEST_MONTHLY/$year/$month
mmkdir -p $dest_dir
fatal "$?" "Error creating directory $dest_dir"

name="metering-request-monthly-$year-$month"
jobid=$(mmkjob -n "$name" \
               -s "$STORAGE_REDUCE1_CMD_MONTHLY" \
               -r "dest=$dest_dir name=$name \
                        /assets/$STORAGE_REDUCE_CMD_MONTHLY" \
               -c "$STORAGE_NUM_REDUCERS_MONTHLY"
)

fatal "$?" "Error creating job $name"

$STORAGE_KEYGEN_MONTHLY $date | maddkeys $jobid

mjob -e $jobid
fatal "$?" "Error ending job $jobid"
