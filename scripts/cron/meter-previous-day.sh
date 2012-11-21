#!/bin/bash
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$dir/meter-daily.sh `date -d "1 day ago"`
