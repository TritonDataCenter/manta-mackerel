#!/bin/bash
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

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../config.cfg

year=$(date -d "$date" +%Y)
month=$(date -d "$date" +%m)
day=$(date -d "$date" +%d)
hour=$(date -d "$date" +%H)

mfind $REQUEST_SOURCE/$year/$month/$day/$hour
