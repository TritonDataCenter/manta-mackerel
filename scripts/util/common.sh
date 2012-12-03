#!/bin/bash -x
# Copyright (c) 2012, Joyent, Inc. All rights reserved.

function fatal() {
        if [ $1 -ne 0 ]
        then
                echo "Error $2"
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

        local input="$@"
        date=$(date --utc -d "$input" "+%Y-%m-%d %H")

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

function monitor() {
        local job=$1
        local retry_limit=$2

        local t=2
        local count=0

        while [ $(mjob $job | json state) != "done" ];
        do
                if [ $count -gt $retry_limit ]
                then
                        echo "Error: exceeded retry limit for job output for" \
                                " job $job"
                        exit 1
                fi

                sleep $t;
                t=$(echo "$t ^ 2" | bc)
                count=$(expr $count + 1)
        done

        mjob -o $job

        failures=$(mjob -x $job)
        if [ ! -z $failures ]
        then
                echo "Error: failures detected for job $job on inputs:"
                echo $failures
        fi

}
