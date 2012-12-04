#!/bin/bash -x
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh

mmkdir -p $ASSETS_DIR/bin
mmkdir -p $ASSETS_DIR/cfg
mmkdir -p $ASSETS_DIR/lib
mput -f $dir/../../cfg/config.sh $CONFIG
mput -f $dir/../../bin/storage-map $STORAGE_MAP_CMD_HOURLY
mput -f $dir/../../bin/storage-reduce1 $STORAGE_REDUCE1_CMD_HOURLY
mput -f $dir/../../bin/storage-reduce2 $STORAGE_REDUCE2_CMD_HOURLY
mput -f $dir/../../bin/request-map $REQUEST_MAP_CMD_HOURLY
mput -f $dir/../../bin/request-reduce $REQUEST_REDUCE_CMD_HOURLY
mput -f $dir/../../lib/sum-columns.js $ASSETS_DIR/lib/sum-columns.js
mput -f $dir/../../bin/sum-columns $ASSETS_DIR/bin/sum-columns
mput -f $dir/../../bin/collate $COLLATE_CMD
