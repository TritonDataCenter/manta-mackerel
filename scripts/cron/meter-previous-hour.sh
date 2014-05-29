#!/bin/bash
# Copyright (c) 2013, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/common.sh

date=$(date --utc -d '1 hour ago')
storagedate=$(date --utc -d '4 hours ago')

NODE=$dir/../../build/node/bin/node

$NODE $dir/../../bin/meter -j 'storage' -d "$storagedate" -r
$NODE $dir/../../bin/meter -j 'request' -d "$date" -r
$NODE $dir/../../bin/meter -j 'compute' -d "$date" -r
$NODE $dir/../../bin/meter -j 'accessLogs' -d "$date" -r
