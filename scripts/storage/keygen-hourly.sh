#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $COMMON

getDate "$@"
date_fmt=$(date -d "$date" "+%Y-%m-%d-%H")

for shard in $(mls $MANTA_STORAGE_SOURCE_HOURLY | json -ga name)
do
        mfind -n "manta-" \
                $MANTA_STORAGE_SOURCE_HOURLY/$shard/$year/$month/$day/$hour
done
