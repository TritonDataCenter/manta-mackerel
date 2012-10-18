#!/bin/bash

JOBID=$(mmkjob \
-m \
        'bzcat $mr_input_key | json -g -a entry | json -g -a | grep { | json -c '\''type!=="directory"'\'' -g -a owner objectId contentLength sharks.length' \
-r \
        'sort | uniq -c | awk '\''{owner=$2;objects[$2]+=1;keys[$2]+=$1;size[$2]+=( (($4/4096) == int($4/4096) ? $4/4096 : int($4/4096)+1) *$5*4)} END {for(i in keys) print i, objects[i], keys[i], size[i]}'\'' | mpipe /poseidon/stor/metering/storage/'`date +%F-%H -d "1 hour ago"` \
-n \
        'metering-storage-'`date +%F-%H -d "1 hour ago"` \
)

/opt/smartdc/mackerel/scripts/keygen.sh `date +%F-%H -d "1 hour ago"` | maddkeys $JOBID

mjob -e $JOBID
