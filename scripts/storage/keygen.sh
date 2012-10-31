#!/bin/bash
if [ -z "$1" ]
then
        echo "Date required." >&2
        exit 1
fi

input="$@"
date=$(date -d "$input" "+%Y-%m-%d-%H")

if [ $? -ne 0 ]
then
        echo "Invalid date: $@" >&2
        exit 1
fi

for shard in $(mls /poseidon/stor/manatee_backups | json -ga name)
do
        mls /poseidon/stor/manatee_backups/$shard | grep $date | json name | \
        awk '{print "/poseidon/stor/manatee_backups/'$shard'/"$1"/manta.bzip"}'
done
