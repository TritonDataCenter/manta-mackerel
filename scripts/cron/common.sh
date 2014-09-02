#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

function getDate() {
        if [ -z "$1" ]
        then
                echo "Date required." >&2
                exit 1
        fi

        local input="$@"
        date=$(date --utc -d "$input")

        if [ $? -ne 0 ]
        then
                echo "Invalid date: $@" >&2
                exit 1
        fi

        year=$(date -d "$date" +%Y)
        month=$(date -d "$date" +%m)
        day=$(date -d "$date" +%d)
        hour=$(date -d "$date" +%H)

        return 0
}
