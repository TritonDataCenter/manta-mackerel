#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

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
