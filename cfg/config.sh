DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

PATH=$PATH:/usr/node/bin:/opt/smartdc/mola/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/mola/build/node/bin:/opt/smartdc/mola/node_modules/.bin:/opt/smartdc/mola/node_modules/manta/bin
MANTA_USER=poseidon
MANTA_KEY_ID=$(ssh-keygen -l -f /root/.ssh/id_rsa.pub | awk '{print $2}')
MANTA_URL=$(mdata-get manta_url)

MANTA_BASE=/$MANTA_USER/stor/usage # base directory for metering

## Compression tools
ZIP=gzip        # command to compress from stdin
ZCAT=gzcat      # command to decompress to stdout
ZEXT=gz         # file extension for compressed files

## Local files

# Common utils
COMMON=$DIR/../scripts/util/common.sh

# Hourly key generators
STORAGE_KEYGEN_HOURLY=$DIR/../scripts/storage/keygen-hourly.sh
REQUEST_KEYGEN_HOURLY=$DIR/../scripts/request/keygen-hourly.sh

# Daily key generators
STORAGE_KEYGEN_DAILY=$DIR/../scripts/storage/keygen-daily.sh
REQUEST_KEYGEN_DAILY=$DIR/../scripts/request/keygen-daily.sh

# Monthly key generators
STORAGE_KEYGEN_MONTHLY=$DIR/../scripts/storage/keygen-monthly.sh
REQUEST_KEYGEN_MONTHLY=$DIR/../scripts/request/keygen-monthly.sh

SLEEP_RETRY=10; # Number of times to poll job status for output before alarming


## Assets locations

ASSETS_DIR=$MANTA_BASE/assets

BLOCK_SIZE=4096 # Manta block size in bytes
STORAGE_MAP_CMD_HOURLY=$ASSETS_DIR/bin/storage-map
STORAGE_REDUCE1_CMD_HOURLY=$ASSETS_DIR/bin/storage-reduce1
STORAGE_REDUCE2_CMD_HOURLY=$ASSETS_DIR/bin/storage-reduce2
STORAGE_REDUCE_CMD_DAILY=$ASSETS_DIR/bin/sum-columns
STORAGE_REDUCE_CMD_MONTHLY=$ASSETS_DIR/bin/sum-columns

REQUEST_MAP_CMD_HOURLY=$ASSETS_DIR/bin/request-map
REQUEST_REDUCE_CMD_HOURLY=$ASSETS_DIR/bin/request-reduce
REQUEST_REDUCE_CMD_DAILY=$ASSETS_DIR/bin/sum-columns
REQUEST_REDUCE_CMD_MONTHLY=$ASSETS_DIR/bin/sum-columns

COLLATE_CMD=$ASSETS_DIR/bin/collate
LIB_SUM_COLUMNS=$ASSETS_DIR/lib/sum-columns.js

CONFIG=$ASSETS_DIR/cfg/config.sh

## Manta directories for metering source files and result directories

# Destinations for results
MANTA_STORAGE_DEST=$MANTA_BASE/storage
MANTA_REQUEST_DEST=$MANTA_BASE/request
MANTA_COMPUTE_DEST=$MANTA_BASE/compute

# These settings are set using eval in the job creation scripts.
# eval is needed here because $year, $month etc are set at job creation. These
# settings represent the format of the destination paths and names.
MANTA_STORAGE_DEST_HOURLY='$MANTA_STORAGE_DEST/$year/$month/$day/$hour'
MANTA_REQUEST_DEST_HOURLY='$MANTA_REQUEST_DEST/$year/$month/$day/$hour'
MANTA_COMPUTE_DEST_HOURLY='$MANTA_COMPUTE_DEST/$year/$month/$day/$hour'

MANTA_STORAGE_DEST_DAILY='$MANTA_STORAGE_DEST/$year/$month/$day'
MANTA_REQUEST_DEST_DAILY='$MANTA_REQUEST_DEST/$year/$month/$day'
MANTA_COMPUTE_DEST_DAILY='$MANTA_COMPUTE_DEST/$year/$month/$day'

MANTA_STORAGE_DEST_MONTHLY='$MANTA_STORAGE_DEST/$year/$month'
MANTA_REQUEST_DEST_MONTHLY='$MANTA_REQUEST_DEST/$year/$month'
MANTA_COMPUTE_DEST_MONTHLY='$MANTA_COMPUTE_DEST/$year/$month'

MANTA_JOB_NAME_STORAGE_HOURLY='metering-storage-hourly-$year-$month-$day-$hour'
MANTA_JOB_NAME_REQUEST_HOURLY='metering-request-hourly-$year-$month-$day-$hour'

MANTA_JOB_NAME_STORAGE_DAILY='metering-storage-daily-$year-$month-$day'
MANTA_JOB_NAME_REQUEST_DAILY='metering-request-daily-$year-$month-$day'

MANTA_JOB_NAME_STORAGE_MONTHLY='metering-storage-monthly-$year-$month'
MANTA_JOB_NAME_REQUEST_MONTHLY='metering-request-monthly-$year-$month'

MANTA_NAME_HOURLY='h$hour.txt.$ZEXT'
MANTA_NAME_DAILY='d$day.txt.$ZEXT'
MANTA_NAME_MONTHLY='m$month.txt.$ZEXT'

# Source directories to pull logs from
MANTA_STORAGE_SOURCE_HOURLY=/$MANTA_USER/stor/manatee_backups
MANTA_REQUEST_SOURCE_HOURLY=/$MANTA_USER/stor/logs/muskie

MANTA_STORAGE_SOURCE_DAILY=$MANTA_STORAGE_DEST
MANTA_REQUEST_SOURCE_DAILY=$MANTA_REQUEST_DEST

MANTA_STORAGE_SOURCE_MONTHLY=$MANTA_STORAGE_DEST
MANTA_REQUEST_SOURCE_MONTHLY=$MANTA_REQUEST_DEST


## Number of reducers

STORAGE_NUM_REDUCERS1_HOURLY=2
STORAGE_NUM_REDUCERS2_HOURLY=2
REQUEST_NUM_REDUCERS_HOURLY=2

STORAGE_NUM_REDUCERS_DAILY=2
REQUEST_NUM_REDUCERS_DAILY=2

STORAGE_NUM_REDUCERS_MONTHLY=2
REQUEST_NUM_REDUCERS_MONTHLY=2
