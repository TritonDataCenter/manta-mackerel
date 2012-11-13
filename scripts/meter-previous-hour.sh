#!/bin/bash
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$dir/meter-hourly.sh `date -d "1 hour ago"`
