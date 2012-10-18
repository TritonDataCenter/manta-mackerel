#!/bin/bash
if [ -z "$1" ]
then
        echo "date required: format YYYY-MM-DD-HH"
        exit 1
fi
for shard in $(mls manatee_backups | json -ga name)
do
        mls /poseidon/stor/manatee_backups/$shard | grep $1 | json name | awk "{print \"/poseidon/stor/manatee_backups/$shard/\" \$1}"
done
