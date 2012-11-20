#!/bin/bash

if [ -z "$1" ]
then
        echo "Date required." >&2
        exit 1
fi

input="$@"
date=$(date -d "$input" "+%Y-%m-%d %H")

if [ $? -ne 0 ]
then
        echo "Invalid date: $@" >&2
        exit 1
fi

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../config.cfg

year=$(date -d "$date" +%Y)
month=$(date -d "$date" +%m)
day=$(date -d "$date" +%d)
hour=$(date -d "$date" +%H)

storage_dest=$METERING_STORAGE_DIR_H/$year/$month/$day/$hour
mmkdir -p $storage_dest
name="metering-storage-hourly-$year-$month-$day-$hour"

map=(
        'bzcat '
        '| ('
                # find the index of the _value field from the schema
                'read -r line; '
                'index=$(echo $line '
                        '| json -e '\''this.i=keys.indexOf("_value")'\'' i); '
                'json -ga "entry[$index]" '
                '| json -c '\''type!=="directory"'\'' -ga owner objectId '
                        'contentLength sharks.length '
                #remove blank lines from when type === directory
                '| sed "/^[ \t]*$/d"'
        ') '
        '| msplit -n '$STORAGE_REDUCERS_1_H' -d " " -f 1,2'
)

reduce1=(
        'sort '
        '| uniq -c '
        '| awk '\'
        'BEGIN { '
                'blockSize='$BLOCK_SIZE';'
        '} {'
                'objects[$2] += 1; '
                'keys[$2] += $1; '
                # numBlocks = Math.ceil(contentLength/blockSize);
                'numBlocks = $4/blockSize == int($4/blockSize) ? '
                        '$4/blockSize : int($4/blockSize + 1); '
                'numCopies = $5; '
                'kbytesPerBlock = blockSize/1024; '
                'size[$2] += numBlocks * numCopies * kbytesPerBlock; '
        '} END { '
                'for(i in keys) { '
                        'print i, objects[i], keys[i], size[i]; '
                '} '
        '}'\'' '
        '| msplit -n '$STORAGE_REDUCERS_2_H' -d " " -f 1'
)

reduce2=(
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
        '| mpipe '$storage_dest/$name-
                '$(base64 /dev/urandom | tr -d "+/\r\n" | head -c 8).bz2'
)

mapstr=$(printf "%s" "${map[@]}") # array join
reduce1str=$(printf "%s" "${reduce1[@]}") # array join
reduce2str=$(printf "%s" "${reduce2[@]}") # array join

jobid=$(mmkjob -m "$mapstr" \
               -r "$reduce1str" -c "$STORAGE_REDUCERS_1_H" \
               -r "$reduce2str" -c "$STORAGE_REDUCERS_2_H" \
               -n "$name")

$STORAGE_KEYGEN_H $date | maddkeys $jobid

mjob -e $jobid
