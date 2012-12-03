#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $COMMON

getDate "$@"

eval $MANTA_REQUEST_DEST_MONTHLY # sets up $dest_dir with date
mmkdir -p $dest_dir
fatal "$?" "creating directory $dest_dir"

eval $MANTA_JOB_NAME_STORAGE_MONTHLY # sets up $job_name
eval $MANTA_NAME_MONTHLY # sets up $name with name format

jobid=$(mmkjob -n "$job_name" \
               -s "$STORAGE_REDUCE1_CMD_MONTHLY" \
               -s "$COLLATE_CMD" \
               -r "/assets/$STORAGE_REDUCE_CMD_MONTHLY" \
                        -c "$STORAGE_NUM_REDUCERS_MONTHLY"
               -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
)

fatal "$?" "creating job $job_name"

$STORAGE_KEYGEN_MONTHLY $date | maddkeys $jobid
fatal "$?" "adding keys to $jobid"

mjob -e $jobid
fatal "$?" "ending job $jobid"

monitor $jobid $SLEEP_RETRY
