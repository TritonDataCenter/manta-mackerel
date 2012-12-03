#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $COMMON

getDate "$@"

eval $MANTA_REQUEST_DEST_HOURLY # sets up $dest_dir with date
mmkdir -p $dest_dir
fatal "$?" "creating directory $dest_dir"

eval $MANTA_JOB_NAME_STORAGE_HOURLY # sets up $job_name
eval $MANTA_NAME_HOURLY # sets up $name with name format

jobid=$(mmkjob -n "$job_name" \
               -s "$STORAGE_MAP_CMD_HOURLY" \
               -s "$STORAGE_REDUCE1_CMD_HOURLY" \
               -s "$STORAGE_REDUCE2_CMD_HOURLY" \
               -s "$COLLATE_CMD" \
               -s "$CONFIG" \
               -m "/assets/$STORAGE_MAP_CMD_HOURLY" \
               -r "/assets/$STORAGE_REDUCE1_CMD_HOURLY" \
                        -c "$STORAGE_NUM_REDUCERS1_HOURLY" \
               -r "/assets/$STORAGE_REDUCE2_CMD_HOURLY" \
                        -c "$STORAGE_NUM_REDUCERS2_HOURLY" \
               -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
)

fatal "$?" "creating job $job_name"

$STORAGE_KEYGEN_HOURLY $date | maddkeys $jobid
fatal "$?" "adding keys to $jobid"

mjob -e $jobid
fatal "$?" "ending job $jobid"

monitor $jobid $SLEEP_RETRY
