#!/bin/bash

if [ -z "$1" ]
then
        echo "Date required." >&2
        exit 1
fi

input="$@"
date=$(date -d "$input" "+%Y-%m-%d %R")

if [ $? -ne 0 ]
then
        echo "Invalid date." >&2
        exit 1
fi

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/config.cfg

mmkdir -p $METERING_STORAGE_DIR
mmkdir -p $METERING_REQUEST_DIR
mmkdir -p $METERING_COMPUTE_DIR

cd $dir
find . -maxdepth 1 -mindepth 1 -type d -execdir '{}'/runjob-hourly.sh $date \;
