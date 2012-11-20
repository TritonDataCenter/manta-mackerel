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

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../config.cfg

for shard in $(mls $STORAGE_SOURCE | json -ga name)
do
        mls $STORAGE_SOURCE/$shard | grep $date | json -ga name | \
        awk '{print "'$STORAGE_SOURCE/$shard/'"$1"/manta.bzip"}'
done
