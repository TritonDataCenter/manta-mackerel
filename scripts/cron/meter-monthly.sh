#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.cfg
source $dir/../common/utils.sh

getDate "$@"

$dir/../storage/monthly.sh $date
$dir/../request/monthly.sh $date
#$dir/../compute/monthly.sh $date
