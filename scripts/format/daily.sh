#!/bin/bash -x

source /root/.bashrc
LDAP_CREDS="-D cn=root -w secret"
LDAP_URL=ldaps://ufds.us-east-2.joyent.us
PATH=/usr/openldap/bin:$PATH

SUMMARY=/poseidon/stor/usage/summary/latest
SUMMARYTMP=/var/tmp/daily-summary.txt
SUMMARYDEST=/poseidon/stor/usage/daily-summary.txt

USERS=$(mget -q $SUMMARY | json -ga owner storageGBHours computeGBSeconds bandwidthGB.in bandwidthGB.out | sort -nrk2)
DATE=$(mget -q $SUMMARY | head -1 | json date)
echo "Generated $(date)" > $SUMMARYTMP
echo "Summary for $DATE" > $SUMMARYTMP
printf "%-20s  %-10s  %-10s  %-10s  %-10s\n" "LOGIN" "GBHOURS" "GBSECS" "BWINGB" "BWOUTGB" >> $SUMMARYTMP
while read -r line; do
    UUID=$(echo $line | awk '{print $1}')
    GBHOURS=$(echo $line | awk '{print $2}')
    GBSECS=$(echo $line | awk '{print $3}')
    BWINGB=$(echo $line | awk '{print $4}')
    BWOUTGB=$(echo $line | awk '{print $5}')

    LOGIN=$(LDAPTLS_REQCERT=allow ldapsearch -LLL -x -H $LDAP_URL $LDAP_CREDS -b ou=users,o=smartdc uuid=$UUID login | grep -v dn | nawk  -F ': ' '{print $2}')

    printf "%-20s  %-10s  %-10s  %-10s  %-10s\n" $LOGIN $GBHOURS $GBSECS $BWINGB $BWOUTGB >> $SUMMARYTMP
done <<< "$USERS"

mput -f $SUMMARYTMP $SUMMARYDEST
