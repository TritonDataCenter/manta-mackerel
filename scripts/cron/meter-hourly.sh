#!/bin/bash -x
# Copyright (c) 2013, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/common.sh

getDate "$@"

NODE=$dir/../../build/node/bin/node

$NODE $dir/../../bin/meter -j 'storage' -d "$date" -r
$NODE $dir/../../bin/meter -j 'request' -d "$date" -r
$NODE $dir/../../bin/meter -j 'compute' -d "$date" -r
$NODE $dir/../../bin/meter -j 'accessLogs' -d "$date" -r
