#!/bin/bash
if [ -z "$1" ]
then
        echo "date required: format YYYY-MM-DD-HH"
        exit 1
fi
export PATH=/opt/smartdc/mola/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/mola/build/node/bin:/opt/smartdc/mola/node_modules/.bin:/opt/smartdc/mola/node_modules/manta/bin
export MANTA_USER=poseidon
export MANTA_KEY_ID=`ssh-keygen -l -f /root/.ssh/id_rsa.pub | awk '{print $2}'`
export MANTA_URL=`mdata-get manta_url`

mmkdir /poseidon/stor/metering
mmkdir /poseidon/stor/metering/storage

keygen="/opt/smartdc/mackerel/scripts/keygen.sh"
JOBID=$(mmkjob \
-m \
        'bzcat | json -g -a entry | json -g -a | grep { | json -c '\''type!=="directory"'\'' -g -a owner objectId contentLength sharks.length' \
-r \
        'sort | uniq -c | awk '\''{owner=$2;objects[$2]+=1;keys[$2]+=$1;size[$2]+=( (($4/4096) == int($4/4096) ? $4/4096 : int($4/4096)+1) *$5*4)} END {for(i in keys) print i, objects[i], keys[i], size[i]}'\'' | mpipe /poseidon/stor/metering/storage/'$1 \
-n \
        'metering-storage-'$1 \
)

$keygen $1 | maddkeys $JOBID

mjob -e $JOBID
