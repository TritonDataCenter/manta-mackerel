DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

PATH=/opt/local/gnu/bin:/opt/marlin/node_modules/manta/bin:/opt/smartdc/mola/node_modules/.bin:/opt/smartdc/mola/build/node/bin:$PATH
export MANTA_USER=poseidon
export MANTA_KEY_ID=$(ssh-keygen -l -f /root/.ssh/id_rsa.pub | awk '{print $2}')
export MANTA_URL=$(mdata-get manta_url)

MANTA_BASE=/$MANTA_USER/stor/usage # base directory for metering

## Compression tools
ZIP=gzip        # command to compress from stdin
ZCAT=gzcat      # command to decompress to stdout
ZEXT=gz         # file extension for compressed files

HEADER_CONTENT_TYPE="application/x-gzip"

## Local files

# Common functions
COMMON=$DIR/../scripts/util/common.sh

# Hourly key generators
KEYGEN_STORAGE_HOURLY=$DIR/../scripts/storage/keygen-hourly.sh
KEYGEN_REQUEST_HOURLY=$DIR/../scripts/request/keygen-hourly.sh

# Daily key generators
KEYGEN_STORAGE_DAILY=$DIR/../scripts/storage/keygen-daily.sh
KEYGEN_REQUEST_DAILY=$DIR/../scripts/request/keygen-daily.sh

# Monthly key generators
KEYGEN_STORAGE_MONTHLY=$DIR/../scripts/storage/keygen-monthly.sh
KEYGEN_REQUEST_MONTHLY=$DIR/../scripts/request/keygen-monthly.sh

SLEEP_RETRY=10; # Number of times to poll job status for output before alarming


## Assets locations

ASSETS_DIR=$MANTA_BASE/assets

COLLATE_CMD=$ASSETS_DIR/bin/collate
SUM_COLUMNS=$ASSETS_DIR/bin/sum-columns

BLOCK_SIZE=4096 # Manta block size in bytes
STORAGE_MAP_CMD_HOURLY=$ASSETS_DIR/bin/storage-map
STORAGE_REDUCE1_CMD_HOURLY=$ASSETS_DIR/bin/storage-reduce1
STORAGE_REDUCE2_CMD_HOURLY=$ASSETS_DIR/bin/storage-reduce2
STORAGE_REDUCE_CMD_DAILY=$SUM_COLUMNS
STORAGE_REDUCE_CMD_MONTHLY=$SUM_COLUMNS

REQUEST_MAP_CMD_HOURLY=$ASSETS_DIR/bin/request-map
REQUEST_REDUCE_CMD_HOURLY=$ASSETS_DIR/bin/request-reduce
REQUEST_REDUCE_CMD_DAILY=$SUM_COLUMNS
REQUEST_REDUCE_CMD_MONTHLY=$SUM_COLUMNS

SPLIT_USAGE_MAP_CMD=$ASSETS_DIR/bin/split-usage


# library files
LIB_SUM_COLUMNS=$ASSETS_DIR/lib/sum-columns.js

CONFIG=$ASSETS_DIR/cfg/config.sh # the configuration file

LOOKUP=$ASSETS_DIR/tmp/lookup # where to put the redis lookup list


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

MANTA_NAME_HOURLY='h$hour.json.$ZEXT'
MANTA_NAME_DAILY='d$day.json.$ZEXT'
MANTA_NAME_MONTHLY='m$month.json.$ZEXT'

# Source directories to pull logs from
MANTA_STORAGE_SOURCE_HOURLY=/$MANTA_USER/stor/manatee_backups
MANTA_REQUEST_SOURCE_HOURLY=/$MANTA_USER/stor/logs/muskie

MANTA_STORAGE_SOURCE_DAILY=$MANTA_STORAGE_DEST
MANTA_REQUEST_SOURCE_DAILY=$MANTA_REQUEST_DEST

MANTA_STORAGE_SOURCE_MONTHLY=$MANTA_STORAGE_DEST
MANTA_REQUEST_SOURCE_MONTHLY=$MANTA_REQUEST_DEST


# Customer-accessible usage reports
MANTA_USAGE_STORAGE_HOURLY='reports/usage/storage/$year/$month/$day/$hour'
MANTA_USAGE_REQUEST_HOURLY='reports/usage/request/$year/$month/$day/$hour'
MANTA_USAGE_COMPUTE_HOURLY='reports/usage/compute/$year/$month/$day/$hour'

MANTA_USAGE_STORAGE_DAILY='reports/usage/storage/$year/$month/$day'
MANTA_USAGE_REQUEST_DAILY='reports/usage/request/$year/$month/$day'
MANTA_USAGE_COMPUTE_DAILY='reports/usage/compute/$year/$month/$day'

MANTA_USAGE_STORAGE_MONTHLY='reports/usage/storage/$year/$month'
MANTA_USAGE_REQUEST_MONTHLY='reports/usage/request/$year/$month'
MANTA_USAGE_COMPUTE_MONTHLY='reports/usage/compute/$year/$month'

MANTA_USAGE_NAME_STORAGE_HOURLY='usage-split-storage-hourly-$year-$month-$day-$hour'
MANTA_USAGE_NAME_REQUEST_HOURLY='usage-split-request-hourly-$year-$month-$day-$hour'

MANTA_USAGE_NAME_STORAGE_DAILY='usage-split-storage-daily-$year-$month-$day'
MANTA_USAGE_NAME_REQUEST_DAILY='usage-split-request-daily-$year-$month-$day'

MANTA_USAGE_NAME_STORAGE_MONTHLY='usage-split-storage-monthly-$year-$month'
MANTA_USAGE_NAME_REQUEST_MONTHLY='usage-split-request-monthly-$year-$month'

MANTA_USAGE_NAME_HOURLY='h$hour.json.$ZEXT'
MANTA_USAGE_NAME_DAILY='d$day.json.$ZEXT'
MANTA_USAGE_NAME_MONTHLY='m$month.json.$ZEXT'

## Number of reducers

STORAGE_NUM_REDUCERS1_HOURLY=2
STORAGE_NUM_REDUCERS2_HOURLY=2
REQUEST_NUM_REDUCERS_HOURLY=2

STORAGE_NUM_REDUCERS_DAILY=2
REQUEST_NUM_REDUCERS_DAILY=2

STORAGE_NUM_REDUCERS_MONTHLY=2
REQUEST_NUM_REDUCERS_MONTHLY=2
