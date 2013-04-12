#!/bin/bash -x
# Copyright (c) 2013, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/common.sh

getDate "$@"

NODE=$dir/../../build/node/bin/node

$NODE $dir/../../bin/meter -p monthly -c storage -d "$date"
$NODE $dir/../../bin/meter -p monthly -c request -d "$date"
$NODE $dir/../../bin/meter -p monthly -c compute -d "$date"
