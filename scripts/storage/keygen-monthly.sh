#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../cfg/config.sh
source $COMMON

getDate "$@"

mfind -n "d[0-9][0-9]" $MANTA_STORAGE_SOURCE_MONTHLY/$year/$month
