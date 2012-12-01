#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $COMMON

getDate "$@"

eval $MANTA_STORAGE_DEST_DAILY # sets up $dest_dir with date
mmkdir -p $dest_dir
fatal "$?" "Error creating directory $dest_dir"

eval $MANTA_JOB_NAME_REQUEST_DAILY # sets up $job_name
eval $MANTA_NAME_DAILY # sets up $name with name format

jobid=$(mmkjob -n "$job_name" \
               -s "$REQUEST_REDUCE_CMD_DAILY" \
               -s "$COLLATE_CMD" \
               -s "$CONFIG" \
               -r "/assets/$REQUEST_REDUCE_CMD_DAILY" \
                        -c "$REQUEST_NUM_REDUCERS_DAILY" \
               -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
)

fatal "$?" "Error creating job $job_name"

$REQUEST_KEYGEN_DAILY $date | maddkeys $jobid

mjob -e $jobid
fatal "$?" "Error ending job $jobid"

monitor $jobid $MONITOR_SLEEP
