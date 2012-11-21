#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../../config.cfg
source $dir/../common/utils.sh

getDate "$@"

mfind $MANTA_REQUEST_SOURCE_HOURLY/$year/$month/$day/$hour
