#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/common.sh

date=$(date --utc -d '1 hour ago')
storagedate=$(date --utc -d '4 hours ago')

NODE=$dir/../../build/node/bin/node

$NODE $dir/../../bin/meter -j 'storage' -d "$storagedate" -r
$NODE $dir/../../bin/meter -j 'request' -d "$date" -r
$NODE $dir/../../bin/meter -j 'compute' -d "$date" -r
$NODE $dir/../../bin/meter -j 'accessLogs' -d "$date" -r
