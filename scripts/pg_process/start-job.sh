#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

set -o xtrace
set -o errexit
set -o pipefail

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'

source ~/.bashrc

disk=

calc_disk_size ()
{
    if [[ $1 -le 2 ]]
    then
        disk=2
    elif [[ $1 -le 4 ]]
    then
        disk=4
    elif [[ $1 -le 8 ]]
    then
        disk=8
    elif [[ $1 -le 16 ]]
    then
        disk=16
    elif [[ $1 -le 32 ]]
    then
        disk=32
    elif [[ $1 -le 64 ]]
    then
        disk=64
    elif [[ $1 -le 128 ]]
    then
        disk=128
    elif [[ $1 -le 256 ]]
    then
        disk=256
    elif [[ $1 -le 512 ]]
    then
        disk=512
    elif [[ $1 -le 1024 ]]
    then
        disk=1024
    else
        echo "dump size $1 exceeds marlin supported disk size"
        exit 1
    fi
}

# mainline

date=$(date -u +%Y/%m/%d/%H -d '1 hour ago')
shards=$(mls /poseidon/stor/manatee_backups)
for s in $shards
do
    dump=$(mfind -t o /poseidon/stor/manatee_backups/$s/$date | \
        grep moray- | tail -1)
    kbytes=$(minfo $dump | grep m-pg-size | cut -d ' ' -f2)
    # Allocate at least 1GB or 4x what the DB dir size was.
    gbytes=$(echo "scale=0;  $kbytes / 2^20 * 4 + 1" | bc)
    # DB size is appx 10x what the compressed dump is.
    calc_disk_size $gbytes

    echo $dump | mjob create --memory=8192 --disk=$disk  -s \
        /poseidon/stor/pgdump/assets/postgresql.conf -s \
        /poseidon/stor/pgdump/assets/process_dump.sh -m \
        '/assets/poseidon/stor/pgdump/assets/process_dump.sh' -w &
done
