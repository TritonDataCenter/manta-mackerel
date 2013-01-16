#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/common.sh

getDate "$@"

$dir/../../bin/meter -p daily -s storage -d "$date"
$dir/../../binmeter -p daily -s request -d "$date"
