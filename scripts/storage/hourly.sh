#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $dir/../common/utils.sh

getDate "$@"

dest_dir=$MANTA_STORAGE_DIR_HOURLY/$year/$month/$day/$hour
mmkdir -p $dest_dir
fatal "$?" "Error creating directory $dest_dir"

name="metering-storage-hourly-$year-$month-$day-$hour"
jobid=$(mmkjob -n "$name" \
               -s "$STORAGE_MAP_CMD_HOURLY" \
               -s "$STORAGE_REDUCE1_CMD_HOURLY" \
               -s "$STORAGE_REDUCE2_CMD_HOURLY" \
               -m "/assets/$STORAGE_MAP_CMD_HOURLY" \
               -r "/assets/$STORAGE_REDUCE1_CMD_HOURLY" \
               -c "$STORAGE_NUM_REDUCERS1_HOURLY" \
               -r "dest=$dest_dir name=$name \
                        /assets/$STORAGE_REDUCE2_CMD_HOURLY" \
               -c "$STORAGE_NUM_REDUCERS2_HOURLY"
)

fatal "$?" "Error creating job $name"

$STORAGE_KEYGEN_HOURLY $date | maddkeys $jobid

mjob -e $jobid
fatal "$?" "Error ending job $jobid"
