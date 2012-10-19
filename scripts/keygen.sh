#!/bin/bash
if [ -z "$1" ]
then
        echo "date required: format YYYY-MM-DD-HH"
        exit 1
fi

manta_bin="/opt/smartdc/mackerel/node_modules/manta/bin"
for shard in $($manta_bin/mls manatee_backups | json -ga name)
do
        $manta_bin/mls /poseidon/stor/manatee_backups/$shard | grep $1 | json name | awk "{print \"/poseidon/stor/manatee_backups/$shard/\" \$1 \"/manta.bzip\"}"
done
