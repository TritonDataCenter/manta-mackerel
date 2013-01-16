#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/common.sh

getDate "$@"

$dir/../../bin/meter -p hourly -s storage -d "$date"
$dir/../../bin/meter -p hourly -s request -d "$date"
