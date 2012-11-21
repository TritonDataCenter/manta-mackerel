#!/bin/bash
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$dir/meter-monthly.sh `date -d "1 month ago"`
