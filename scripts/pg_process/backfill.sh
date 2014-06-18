#!/bin/bash
# ghetto backfill script -- replace echo ' ' with whatever hour you are missing
echo $1 |  mjob create --memory=8192 --disk=512 -s /poseidon/stor/pgdump/assets/postgresql.conf -s /poseidon/stor/pgdump/assets/process_dump.sh -m /assets/poseidon/stor/pgdump/assets/process_dump.sh -w
