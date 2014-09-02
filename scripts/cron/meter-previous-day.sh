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
$dir/meter-daily.sh `date -d "1 day ago"`
