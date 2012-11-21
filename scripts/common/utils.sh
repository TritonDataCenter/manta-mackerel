#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

function fatal() {
        if [ $1 -ne 0 ]
        then
                echo $2
                exit 1
        fi
        return 0
}

function getDate() {
        if [ -z "$1" ]
        then
                echo "Date required." >&2
                exit 1
        fi

        input="$@"
        date=$(date -d "$input" "+%Y-%m-%d %H")

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
