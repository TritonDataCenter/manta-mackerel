#!/bin/bash -x
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
MANTA_USER=poseidon
MANTA_KEY_ID=$(ssh-keygen -l -f /root/.ssh/id_rsa.pub | awk '{print $2}')
MANTA_URL=$(mdata-get manta_url)

mmkdir -p $ASSETS_DIR/bin
mmkdir -p $ASSETS_DIR/cfg
mmkdir -p $ASSETS_DIR/lib
mmkdir -p $ASSETS_DIR/tmp
mput -f $dir/../../cfg/config.sh $CONFIG
mput -f $dir/../../bin/storage-map $STORAGE_MAP_CMD_HOURLY
mput -f $dir/../../bin/storage-reduce1 $STORAGE_REDUCE1_CMD_HOURLY
mput -f $dir/../../bin/storage-reduce2 $STORAGE_REDUCE2_CMD_HOURLY
mput -f $dir/../../bin/request-map $REQUEST_MAP_CMD_HOURLY
mput -f $dir/../../bin/request-reduce $REQUEST_REDUCE_CMD_HOURLY
mput -f $dir/../../lib/sum-columns.js $LIB_SUM_COLUMNS
mput -f $dir/../../bin/sum-columns $SUM_COLUMNS
mput -f $dir/../../bin/collate $COLLATE_CMD
mput -f $dir/../../bin/split-usage $SPLIT_USAGE_MAP_CMD
