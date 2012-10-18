#!/bin/bash
sort | uniq -c \
| awk '
{
        owner = $2;
        objects[$2] += 1;
        keys[$2] += $1;
        size[$2] += ($4/4096 == int($4/4096) ? $4/4096 : int($4/4096)+1)*$5*4;
} END {
        for(i in keys) {
                print i, objects[i], keys[i], size[i]
        }
}'
