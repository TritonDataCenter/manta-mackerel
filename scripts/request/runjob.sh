#!/bin/bash

if [ -z "$1" ]
then
        echo "Date required." >&2
        exit 1
fi

input="$@"
date=`date -d "$input" "+%Y-%m-%d %R"`

if [ $? -ne 0 ]
then
        echo "Invalid date." >&2
        exit 1
fi

if [ -z $METERING_REQUEST_DIR ]
then
        echo "METERING_REQUEST_DIR not set." >&2
        echo "Defaulting to /poseidon/stor/metering/request." >&2
        METERING_REQUEST_DIR=/poseidon/stor/metering/request
fi

scripts="/opt/smartdc/mackerel/scripts"
keygen=$scripts/request/keygen.sh

year=`date -d "$date" +%Y`
month=`date -d "$date" +%m`
day=`date -d "$date" +%d`
hour=`date -d "$date" +%H`
request_path=$METERING_REQUEST_DIR/$year/$month/$day/$hour

mmkdir -p $METERING_REQUEST_DIR/$year/$month/$day/$hour

map=(
        'bzcat | '
        'bunyan -j --strict '
                '-c "this.audit == true && this.req.url != '\''/ping'\''" | '
        'json -ga req.owner req.method '
                'req.headers.content-length res.headers.content-length | '
        'awk '\''{ '
                'if($2 == "GET") { '
                        'gets[$1] += 1; '
                        'if ($3) { '
                                        'bwout[$1] += $3; '
                                '} '
                        '} '
                'if($2 == "PUT") { '
                        'puts[$1] += 1; '
                        'if($3) { '
                                'bwin[$1] += $3; '
                        '} '
                '} '
                'if($2 == "DELETE") { '
                        'deletes[$1] += 1; '
                '} '
                'if($2 == "HEAD") { '
                        'heads[$1] += 1; '
                '} '
                'if($2 == "POST") { '
                        'posts[$1] += 1; '
                '} '
                'owners[$1] = $1; '
        '} END { '
                'for (o in owners) { '
                        'total = gets[o] + puts[o] + deletes[o] + '
                                'heads[o] + posts[o]; '
                        'print o, total, gets[o], puts[o], deletes[o], '
                                'heads[o], posts[o], bwin[o], bwout[o]; '
                '} '
        '}'\'
)

reduce=(
        'awk '\''{ '
                'owners[$1] = $1; '
                'total[$1] += $2; '
                'gets[$1] += $3; '
                'puts[$1] += $4; '
                'deletes[$1] += $5; '
                'heads[$1] += $6; '
                'posts[$1] += $7; '
                'bwin[$1] += $8; '
                'bwout[$1] += $9; '
        '} END { '
                'for (o in owners) { '
                        'print o, total[o], gets[o], puts[o], deletes[o], '
                                'heads[o], posts[o], bwin[o], bwout[o]; '
                '} '
        '} '\'' | '
        'bzip2 | '
        'mpipe '$request_path/metering-request-$year-$month-$day-$hour.bz2
)

mapstr=$(printf "%s" "${map[@]}") # array join
reducestr=$(printf "%s" "${reduce[@]}") # array join

name="metering-request-$year-$month-$day-$hour"

jobid=$(mmkjob -m "$mapstr" -r "$reducestr" -n "$name")

$keygen $date | maddkeys $jobid

mjob -e $jobid
