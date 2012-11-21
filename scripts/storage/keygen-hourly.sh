#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../config.cfg
source $dir/../common/utils.sh

getDate "$@"
date_fmt=$(date -d "$date" "+%Y-%m-%d-%H")

for shard in $(mls $MANTA_STORAGE_SOURCE_HOURLY | json -ga name)
do
        mls $MANTA_STORAGE_SOURCE_HOURLY/$shard \
        | grep $date_fmt \
        | json -ga name \
        | awk '{print "'$MANTA_STORAGE_SOURCE_HOURLY/$shard/'"$1"/manta.gz"}'
done
