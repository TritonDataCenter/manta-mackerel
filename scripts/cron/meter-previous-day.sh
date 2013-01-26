#!/bin/bash
# Copyright (c) 2013, Joyent, Inc. All rights reserved.

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$dir/meter-daily.sh `date -d "1 day ago"`
