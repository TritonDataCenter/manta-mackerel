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

if [ -n "${METERING_STORAGE_DIR:-x}" ]
then
        echo "METERING_STORAGE_DIR not set" >&2
        echo "defaulting to /poseidon/stor/metering/storage" >&2
        METERING_STORAGE_DIR=/poseidon/stor/metering/storage
fi

scripts="/opt/smartdc/mackerel/scripts"
keygen=$scripts/storage/keygen.sh

year=$(date -d "$date" +%Y)
month=$(date -d "$date" +%m)
day=$(date -d "$date" +%d)
hour=$(date -d "$date" +%H)
storage_path=$METERING_STORAGE_DIR/$year/$month/$day/$hour

mmkdir $METERING_STORAGE_DIR/$year
mmkdir $METERING_STORAGE_DIR/$year/$month
mmkdir $METERING_STORAGE_DIR/$year/$month/$day
mmkdir $METERING_STORAGE_DIR/$year/$month/$day/$hour

map=(
        'bzcat | '
        'json -g -a entry | '
        'json -g -a | '
        'grep { | '
        'json -c '\''type!=="directory"'\'' -g -a '
                'owner objectId contentLength sharks.length'
)

reduce=(
        'sort | '
        'uniq -c | '
        'awk '\''{ '
                'objects[$2] += 1; '
                'keys[$2] += $1; '
                'size[$2] += $4/4096 == int($4/4096) ? '
                        '$4/4096 * $5 * 4 : int($4/4096) + 1 * $5 * 4; '
        '} END { '
                'for(i in keys) { '
                        'print i, objects[i], keys[i], size[i]; '
                '} '
        '}'\'' | '
        'bzip2 | '
        'mpipe '$storage_path/metering-storage-$year-$month-$day-$hour.bz2
)

mapstr=$(printf "%s" "${map[@]}") # array join
reducestr=$(printf "%s" "${reduce[@]}") # array join

name="metering-storage-$year-$month-$day-$hour"

jobid=$(mmkjob -m "$mapstr" -r "$reducestr" -n "$name")

$keygen $date | maddkeys $jobid

mjob -e $jobid
