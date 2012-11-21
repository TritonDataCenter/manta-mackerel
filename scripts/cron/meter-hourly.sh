#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.cfg
source $dir/../common/utils.sh

getDate "$@"

$dir/../storage/hourly.sh $date
$dir/../request/hourly.sh $date
#$dir/../compute/hourly.sh $date
