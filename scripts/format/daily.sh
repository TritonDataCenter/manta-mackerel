#!/bin/bash -x

source /root/.bashrc
LDAP_CREDS="-D cn=root -w secret"
LDAP_URL=ldaps://ufds.us-east-2.joyent.us
PATH=/usr/openldap/bin:$PATH

SUMMARY=/poseidon/stor/usage/summary/latest
SUMMARYTMP=/var/tmp/daily-summary.txt
SUMMARYDEST=/poseidon/stor/usage/daily-summary.txt

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

USERS=$(mget -q $SUMMARY | json -ga owner storageGBHours computeGBSeconds bandwidthBytes.in bandwidthBytes.out | sort -nrk2)
DATE=$(mget -q $SUMMARY | head -1 | json date)
echo "Generated $(date)" > $SUMMARYTMP
echo "Summary for $DATE" > $SUMMARYTMP
printf "%-25s  %-10s  %-10s  %-5s  %-5s\n" "LOGIN" "GBHOURS" "GBSECS" "BWIN" "BWOUT" >> $SUMMARYTMP
while read -r line; do
    UUID=$(echo $line | awk '{print $1}')
    GBHOURS=$(echo $line | awk '{print $2}')
    GBSECS=$(echo $line | awk '{print $3}')
    BWINBytes=$(echo $line | awk '{print $4}')
    BWOUTBytes=$(echo $line | awk '{print $5}')

    LOGIN=$(LDAPTLS_REQCERT=allow ldapsearch -LLL -x -H $LDAP_URL $LDAP_CREDS -b ou=users,o=smartdc uuid=$UUID login | grep -v dn | nawk  -F ': ' '{print $2}')

    printf "%-25s  %-10s  %-10s  %-5s  %-5s\n" $LOGIN $GBHOURS $GBSECS $(human $BWINBytes) $(human $BWOUTBytes) >> $SUMMARYTMP
done <<< "$USERS"

mput -f $SUMMARYTMP $SUMMARYDEST
