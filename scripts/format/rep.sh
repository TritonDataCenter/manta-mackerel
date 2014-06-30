#!/bin/bash

source /root/.bashrc

#LDAP_CREDS="-D cn=root -w secret"
#LDAP_URL=ldaps://ufds.us-east-2.joyent.us
MAHI_URL=http://authcache.us-east.joyent.us
PATH=/usr/openldap/bin:$PATH
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Generated $(date)"

STORAGE=/poseidon/stor/usage/storage/latest
REQUEST=/poseidon/stor/usage/request/latest
COMPUTE=/poseidon/stor/usage/compute/latest
STORAGETMP=/var/tmp/mackerel-storage.txt
REQUESTTMP=/var/tmp/mackerel-request.txt
COMPUTETMP=/var/tmp/mackerel-compute.txt
STORAGEDEST=/poseidon/stor/usage/hourly-storage-usage.txt
REQUESTDEST=/poseidon/stor/usage/hourly-request-usage.txt
COMPUTEDEST=/poseidon/stor/usage/hourly-compute-usage.txt

function human(){
echo $1 | awk 'function human(x) {
        s="BKMGTEPYZ";
        while (x>=1024 && length(s)>1) {
            x/=1024;
            s=substr(s,2)
        }
        if (x < 10) {
            return int((x*10)+0.5)/10 substr(s,1,1)
        }
        return int(x+0.5) substr(s,1,1)
    }
    {gsub(/^[0-9]+/, human($1)); print}'
}

function humanTime(){
        local seconds=$1
        local minutes=$((seconds / 60))
        local seconds=$((seconds % 60))
  echo $minutes'm'$seconds's'
}
USERS=$(mget -q $STORAGE | json -ga -c '+storage.stor.bytes > 0' -e 'total= +storage.stor.bytes + +storage.public.bytes + +storage.jobs.bytes' owner storage.stor.bytes storage.public.bytes storage.jobs.bytes total | sort -nrk5)

echo "Generated $(date)" > $STORAGETMP
echo "Storage data from the hour of $(mls -j $(dirname $STORAGE) | json -ga -c "this.name === '$(basename $STORAGE)'" mtime)" >> $STORAGETMP
printf "%-25s  %-7s  %-7s  %-7s  %-7s\n" "LOGIN" "TOTAL" "STORAGE" "PUBLIC" "JOBS" >> $STORAGETMP
while read -r line; do
    UUID=$(echo $line | awk '{print $1}')
    STOR=$(echo $line | awk '{print $2}')
    PUB=$(echo $line | awk '{print $3}')
    JOBS=$(echo $line | awk '{print $4}')
    TOTAL=$(echo $line | awk '{print $5}')

    LOGIN=$(curl -s $MAHI_URL/getName -H 'content-type:application/json' -X POST --data-binary "{\"uuids\":[\"$UUID\"]}" | json $UUID )

    printf "%-25s  %-7s  %-7s  %-7s  %-7s\n" $LOGIN $(human $TOTAL) $(human $STOR) $(human $PUB) $(human $JOBS) >> $STORAGETMP
done <<< "$USERS"

mput -f $STORAGETMP $STORAGEDEST


USERS=$(mget -q $REQUEST | json -ga -c 'requests.bandwidth.in > 0 || requests.bandwidth.out > 0' -e 'tbw=+requests.bandwidth.in + +requests.bandwidth.out' owner tbw requests.bandwidth.in requests.bandwidth.out | sort -nrk2)

echo "Generated $(date)" > $REQUESTTMP
echo "Request data from the hour of $(mls -j $(dirname $REQUEST) | json -ga -c "this.name === '$(basename $REQUEST)'" mtime)" >> $REQUESTTMP
printf "%-25s  %-7s  %-7s  %-7s\n" "LOGIN" "TOTAL" "BWIN" "BWOUT" >> $REQUESTTMP
while read -r line; do
    UUID=$(echo $line | awk '{print $1}')
    TOTAL=$(echo $line | awk '{print $2}')
    BWIN=$(echo $line | awk '{print $3}')
    BWOUT=$(echo $line | awk '{print $4}')

    LOGIN=$(curl -s $MAHI_URL/getName -H 'content-type:application/json' -X POST --data-binary "{\"uuids\":[\"$UUID\"]}" | json $UUID )

    printf "%-25s  %-7s  %-7s  %-7s\n" $LOGIN $(human $TOTAL) $(human $BWIN) $(human $BWOUT) >> $REQUESTTMP
done <<< "$USERS"

mput -f $REQUESTTMP $REQUESTDEST


USERS=$(mget -q $COMPUTE| $DIR/hourlycompute.js |  sort -nrk2)
echo "Generated $(date)" > $COMPUTETMP
echo "Compute data from the hour of $(mls -j $(dirname $COMPUTE) | json -ga -c "this.name === '$(basename $COMPUTE)'" mtime)" >> $COMPUTETMP
printf "%-25s  %-8s  %-8s  %-5s  %-6s  %-5s  %-5s  %-5s\n" "LOGIN" "BILLTIME" "RAWTIME" "JOBS" "PHASES" "TASKS" "BWIN" "BWOUT"  >> $COMPUTETMP
while read -r line; do
    UUID=$(echo $line | awk '{print $1}')
    GBSECS=$(echo $line | awk '{print $2}')
    RAWSECS=$(echo $line | awk '{print $3}')
    JOBS=$(echo $line | awk '{print $4}')
    PHASES=$(echo $line | awk '{print $5}')
    TASKS=$(echo $line | awk '{print $6}')
    BWIN=$(echo $line | awk '{print $7}')
    BWOUT=$(echo $line | awk '{print $8}')

    LOGIN=$(curl -s $MAHI_URL/getName -H 'content-type:application/json' -X POST --data-binary "{\"uuids\":[\"$UUID\"]}" | json $UUID )

                printf "%-25s  %-8s  %-8s  %-5s  %-6s  %-5s  %-5s  %-5s\n" $LOGIN $(humanTime $GBSECS) $(humanTime $RAWSECS) $JOBS $PHASES $TASKS $(human $BWIN) $(human $BWOUT)  >> $COMPUTETMP
done <<< "$USERS"

mput -f $COMPUTETMP $COMPUTEDEST
