#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $COMMON

getDate "$@"

eval $MANTA_REQUEST_DEST_DAILY # sets up $dest_dir with date
mmkdir -p $dest_dir
fatal "$?" "creating directory $dest_dir"

eval $MANTA_JOB_NAME_STORAGE_DAILY # sets up $job_name
eval $MANTA_NAME_DAILY # sets up $name with name format

jobid=$(mmkjob -n "$job_name" \
               -s "$STORAGE_REDUCE1_CMD_DAILY" \
               -s "$COLLATE_CMD" \
               -r "/assets/$STORAGE_REDUCE_CMD_DAILY" \
                        -c "$STORAGE_NUM_REDUCERS_DAILY"
               -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
)

fatal "$?" "creating job $job_name"

$STORAGE_KEYGEN_DAILY $date | maddkeys $jobid
fatal "$?" "adding keys to $jobid"

mjob -e $jobid
fatal "$?" "ending job $jobid"

monitor $jobid $SLEEP_RETRY
