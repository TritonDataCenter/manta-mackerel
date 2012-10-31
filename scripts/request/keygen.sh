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

year=$(date -d "$date" +%Y)
month=$(date -d "$date" +%m)
day=$(date -d "$date" +%d)
hour=$(date -d "$date" +%H)

mls /poseidon/stor/logs/muskie/$year/$month/$day/$hour | json -ga name | \
awk "{print \"/poseidon/stor/logs/muskie/$year/$month/$day/$hour/\"\$1}"
