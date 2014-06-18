#!/bin/bash
set -o xtrace
set -o errexit
set -o pipefail

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
PG_CONF=/assets/poseidon/stor/pgdump/assets/postgresql.conf

DUMP_DIR=/manatee_dumps
PG_START_TIMEOUT=5
PG_START_TRIES=
PG_START_MAX_TRIES=100
shard=$(echo $MANTA_INPUT_OBJECT | cut -d '/' -f5)
year=$(echo $MANTA_INPUT_OBJECT | cut -d '/' -f6)
month=$(echo $MANTA_INPUT_OBJECT | cut -d '/' -f7)
day=$(echo $MANTA_INPUT_OBJECT | cut -d '/' -f8)
hour=$(echo $MANTA_INPUT_OBJECT | cut -d '/' -f9)
date=$year-$month-$day-$hour

manta_dir=$(dirname $MANTA_INPUT_OBJECT)

function finish {
    pkill postgres
}
trap finish EXIT

function fatal
{
    FATAL=1
    echo "$(basename $0): fatal error: $*"
    pgkill postgres
    exit 1
}

# load the pg dump into a running postgres instance
function prep_db
{
    mkdir -p /manatee
    chown postgres /manatee
    sudo -u postgres initdb /manatee
    cp $PG_CONF /manatee/.
    sudo -u postgres pg_ctl start -D /manatee
}

function load_db
{
    psql -U postgres -c 'create role moray with superuser'
    createdb -U postgres moray
    gunzip -c $MANTA_INPUT_FILE > /moray.sql
    psql -U postgres --set ON_ERROR_STOP=on moray < moray.sql
}

function wait_for_pg_start
{
    echo "waiting $PG_START_TIMEOUT seconds for PG to start"
    PG_START_TRIES=$(($PG_START_TRIES + 1))
    if [[ $PG_START_TRIES -gt $PG_START_MAX_TRIES ]]; then
        fatal "PG start tries exceeded, did not start in time"
    fi
    sleep $PG_START_TIMEOUT
    # check and see if pg is up.
    set +o errexit
    psql -U postgres -c 'select current_time'
    if [[ $? -eq 0 ]]; then
        set -o errexit
        echo "PG has started"
    else
        set -o errexit
        echo "PG not started yet, waiting again"
        wait_for_pg_start
    fi
}

function backup
{
    mkdir -p $DUMP_DIR

    echo "getting db tables"
    local schema=$DUMP_DR/$date'_schema'
    # trim the first 3 lines of the schema dump
    sudo -u postgres psql moray -c '\dt' | sed -e '1,3d' > $schema
    [[ $? -eq 0 ]] || (rm $schema; fatal "unable to read db schema")
    for i in `sed 'N;$!P;$!D;$d' $schema | tr -d ' '| cut -d '|' -f2`
    do
        local time=$(date -u +%F-%H-%M-%S)
        local dump_file=$DUMP_DIR/$i-$time.gz
        sudo -u postgres pg_dump moray -a -t $i | gsed 's/\\\\/\\/g' | sqlToJson.js | gzip -1 > $dump_file
        [[ $? -eq 0 ]] || fatal "Unable to dump table $i"
    done
    rm $schema
    [[ $? -eq 0 ]] || fatal "unable to remove schema"
}

function upload_pg_dumps
{
    local upload_error=0;
    for f in $(ls $DUMP_DIR); do
        echo "uploading dump $f to manta"
        mput -f $DUMP_DIR/$f $manta_dir/$f
        if [[ $? -ne 0 ]]; then
            echo "unable to upload dump $DUMP_DIR/$f"
            upload_error=1
        else
            echo "removing dump $DUMP_DIR/$f"
            rm $DUMP_DIR/$f
        fi
    done

    return $upload_error
}

npm install -g sqltojson
prep_db
wait_for_pg_start
load_db
backup
for tries in {1..5}; do
    echo "upload attempt $tries"
    upload_pg_dumps
    if [[ $? -eq 0 ]]; then
        echo "successfully finished uploading attempt $tries"
        exit 0
    else
        echo "attempt $tries failed"
    fi
done

fatal "unable to upload all pg dumps"
