#!/bin/bash

if [ -z "$1" ]
then
        echo "Date required."
        exit 1
fi

input="$@"
date=$(date -d "$input" "+%Y-%m-%d %R")

if [ $? -ne 0 ]
then
        echo "Invalid date."
        exit 1
fi

scripts="/opt/smartdc/mackerel/scripts"

export PATH=/opt/smartdc/mola/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/mola/build/node/bin:/opt/smartdc/mola/node_modules/.bin:/opt/smartdc/mola/node_modules/manta/bin
export MANTA_USER=poseidon
export MANTA_KEY_ID=$(ssh-keygen -l -f /root/.ssh/id_rsa.pub | awk '{print $2}')
export MANTA_URL=$(mdata-get manta_url)

export METERING_BASE_DIR=/$MANTA_USER/stor/metering
export METERING_STORAGE_DIR=$METERING_BASE_DIR/storage
export METERING_REQUEST_DIR=$METERING_BASE_DIR/request
export METERING_COMPUTE_DIR=$METERING_BASE_DIR/compute

mmkdir -p $METERING_STORAGE_DIR
mmkdir -p $METERING_REQUEST_DIR
mmkdir -p $METERING_COMPUTE_DIR

cd $scripts
find . -maxdepth 1 -mindepth 1 -type d -execdir '{}'/runjob.sh $date \;
