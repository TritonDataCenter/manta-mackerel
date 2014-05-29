#!/bin/bash
set -o xtrace
set -o errexit
set -o pipefail

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'

backoff=/opt/smartdc/mackerel/scripts/pg_process/backoff.sh
backoff=./backoff.sh
disk=

calc_disk_size ()
{
    local size=$1
    if [[ $size -le 2 ]]
    then
        disk=2
    elif [[ $size -le 4 ]]
    then
        disk=4
    elif [[ $size -le 8 ]]
    then
        disk=8
    elif [[ $size -le 16 ]]
    then
        disk=16
    elif [[ $size -le 32 ]]
    then
        disk=32
    elif [[ $size -le 64 ]]
    then
        disk=64
    elif [[ $size -le 128 ]]
    then
        disk=128
    elif [[ $size -le 256 ]]
    then
        disk=256
    elif [[ $size -le 512 ]]
    then
        disk=512
    elif [[ $size -le 1024 ]]
    then
        disk=1024
    else
        echo "dump size $size exceeds marlin supported disk size"
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
    bytes=$(minfo $dump | grep m-pg-size | cut -d ' ' -f2)
    # Allocate at least 1GB or 4x what the DB dir size was.
    gbytes=$(echo "scale=0;  $bytes / 2^30 * 4 + 1" | bc)
    # DB size is appx 10x what the compressed dump is.
    calc_disk_size $gbyte

    echo $dump | mjob create --memory=8192 --disk=$disk  -s \
        /poseidon/stor/pgdump/assets/postgresql.conf -s \
        /poseidon/stor/pgdump/assets/process_dump.sh -m \
        '/assets/poseidon/stor/pgdump/assets/process_dump.sh' -w &
done
