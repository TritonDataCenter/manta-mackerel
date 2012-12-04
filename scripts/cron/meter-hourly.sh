#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $COMMON

getDate "$@"

$dir/../meter.sh -p hourly -s storage -d "$date"
$dir/../meter.sh -p hourly -s request -d "$date"
