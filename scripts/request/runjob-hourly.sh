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

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../config.cfg

year=`date -d "$date" +%Y`
month=`date -d "$date" +%m`
day=`date -d "$date" +%d`
hour=`date -d "$date" +%H`

request_dest=$METERING_REQUEST_DIR_H/$year/$month/$day/$hour
mmkdir -p $request_dest
name="metering-request-hourly-$year-$month-$day-$hour"

map=(
        'bzcat '
        '| bunyan -j --strict '
                '-c "this.audit == true && this.req.url != '\''/ping'\''" '
        '| json -ga req.owner req.method '
                'req.headers.content-length res.headers.content-length '
        '| awk '\''{ '
                'owners[$1] = $1; '
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
        '} END { '
                'for (o in owners) { '
                        'gets[o] = gets[o] == "" ? 0 : gets[o];'
                        'puts[o] = puts[o] == "" ? 0 : puts[o];'
                        'deletes[o] = deletes[o] == "" ? 0 : deletes[o];'
                        'heads[o] = heads[o] == "" ? 0 : heads[o];'
                        'posts[o] = posts[o] == "" ? 0 : posts[o];'
                        'bwin[o] = bwin[o] == "" ? 0 : bwin[o];'
                        'bwout[o] = bwout[o] == "" ? 0 : bwout[o];'
                        'total = gets[o] + puts[o] + deletes[o] + '
                                'heads[o] + posts[o]; '
                        'print o, total, gets[o], puts[o], deletes[o], '
                                'heads[o], posts[o], bwin[o], bwout[o]; '
                '} '
        '}'\'' '
        '| msplit -n '$REQUEST_REDUCERS_H' -d " " -f 1'
)

reduce=(
        'awk '\''{ '
                'owners[$1] = $1;'
                'for (i = 2; i <= NF; i++) { '
                        'sums[$1,i] += $i; '
                '} '
        '} END { '
                'for (o in owners) { '
                        'printf("%s ",owners[o]); '
                        'for (i = 2; i < NF; i++) { '
                                'printf("%s ", sums[owners[o],i]); '
                        '} '
                        'printf("%s", sums[owners[o],NF]);'
                        'printf("\n"); '
                '} '
        '}'\'' '
        '| bzip2 '
        '| mpipe '$request_dest/$name-
                '$(base64 /dev/urandom | tr -d "+/\r\n" | head -c 8).bz2'
)

mapstr=$(printf "%s" "${map[@]}") # array join
reducestr=$(printf "%s" "${reduce[@]}") # array join

jobid=$(mmkjob -m "$mapstr" -r "$reducestr" -n "$name" -c "$REQUEST_REDUCERS_H")

$REQUEST_KEYGEN_H $date | maddkeys $jobid

mjob -e $jobid
