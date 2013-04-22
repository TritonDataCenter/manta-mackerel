#!/bin/bash

LDAP_CREDS="-D cn=root -w secret"
LDAP_URL=ldaps://10.3.80.8
PATH=/usr/openldap/bin:$PATH

echo "Generated $(date)"



STORAGE=/poseidon/stor/usage/storage/latest-hourly
REQUESTS=/poseidon/stor/usage/request/latest-daily

echo "Storage data from the hour of $(mls $(dirname $STORAGE) | json -ga -c "this.name === '$(basename $STORAGE)'" mtime)"
echo "Request data from the calendar day preceding $(mls $(dirname $REQUESTS) | json -ga -c "this.name === '$(basename $REQUESTS)'" mtime)"

USERS=$(mget -q $STORAGE | json -ga -c 'stor.bytes > 0' -e 'total=stor.bytes+public.bytes+jobs.bytes' owner stor.bytes public.bytes jobs.bytes total | sort -nk5)

printf "%-25s  %-15s  %-15s  %-15s  %-15s\n" "LOGIN" "TOTAL" "STORAGE" "PUBLIC" "JOBS"
while read -r line; do
    UUID=$(echo $line | awk '{print $1}')
    STOR=$(echo $line | awk '{print $2}')
    PUB=$(echo $line | awk '{print $3}')
    JOBS=$(echo $line | awk '{print $4}')
    TOTAL=$(echo $line | awk '{print $5}')

    LOGIN=$(LDAPTLS_REQCERT=allow ldapsearch -LLL -x -H $LDAP_URL $LDAP_CREDS -b ou=users,o=smartdc uuid=$UUID login | grep -v dn | nawk  -F ': ' '{print $2}')

    printf "%-25s  %-15s  %-15s  %-15s  %-15s\n" $LOGIN $TOTAL $STOR $PUB $JOBS
done <<< "$USERS"


#mget -q /poseidon/stor/usage/request/latest-daily | json -ga -c 'bwin > 0 || bwout > 0' owner total bwin bwout
USERS=$(mget -q $STORAGE | mget -q /poseidon/stor/usage/request/latest-daily | json -ga -c 'bwin > 0 || bwout > 0' -e 'tbw=bwin+bwout' owner total bwin bwout tbw | sort -nk5)

printf "\n\n%-25s  %-15s  %-25s  %-25s\n" "LOGIN" "REQUESTS" "BWIN" "BWOUT"
while read -r line; do
    UUID=$(echo $line | awk '{print $1}')
    TOTAL=$(echo $line | awk '{print $2}')
    BWIN=$(echo $line | awk '{print $3}')
    BWOUT=$(echo $line | awk '{print $4}')

    LOGIN=$(LDAPTLS_REQCERT=allow ldapsearch -LLL -x -H $LDAP_URL $LDAP_CREDS -b ou=users,o=smartdc uuid=$UUID login | grep -v dn | nawk  -F ': ' '{print $2}')

    printf "%-25s  %-15s  %-25s  %-25s\n" $LOGIN $TOTAL $BWIN $BWOUT
done <<< "$USERS"
