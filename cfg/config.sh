DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

PATH=$PATH:/opt/smartdc/mola/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/mola/build/node/bin:/opt/smartdc/mola/node_modules/.bin:/opt/smartdc/mola/node_modules/manta/bin
MANTA_USER=poseidon
MANTA_KEY_ID=$(ssh-keygen -l -f /root/.ssh/id_rsa.pub | awk '{print $2}')
MANTA_URL=$(mdata-get manta_url)


## Compression tools
ZIP=gzip        # command to compress from stdin
ZCAT=gzcat      # command to decompress to stdout
ZEXT=gz         # file extension for compressed files

## Local files

# Hourly key generators
STORAGE_KEYGEN_HOURLY=$DIR/../scripts/storage/keygen-hourly.sh
REQUEST_KEYGEN_HOURLY=$DIR/../scripts/request/keygen-hourly.sh

# Daily key generators
STORAGE_KEYGEN_DAILY=$DIR/../scripts/storage/keygen-daily.sh
REQUEST_KEYGEN_DAILY=$DIR/../scripts/request/keygen-daily.sh

# Monthly key generators
STORAGE_KEYGEN_MONTHLY=$DIR/../scripts/storage/keygen-monthly.sh
REQUEST_KEYGEN_MONTHLY=$DIR/../scripts/request/keygen-monthly.sh


## Assets locations

ASSETS_DIR=/$MANTA_USER/stor/metering/assets

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


## Manta directories for metering source files and result directories

# Destination directories for results
MANTA_BASE_DEST=/$MANTA_USER/stor/metering

MANTA_STORAGE_DEST=$MANTA_BASE_DEST/storage
MANTA_REQUEST_DEST=$MANTA_BASE_DEST/request
MANTA_COMPUTE_DEST=$MANTA_BASE_DEST/compute

MANTA_STORAGE_DEST_HOURLY=$MANTA_STORAGE_DEST/hourly
MANTA_REQUEST_DEST_HOURLY=$MANTA_REQUEST_DEST/hourly
MANTA_COMPUTE_DEST_HOURLY=$MANTA_COMPUTE_DEST/hourly

MANTA_STORAGE_DEST_DAILY=$MANTA_STORAGE_DEST/daily
MANTA_REQUEST_DEST_DAILY=$MANTA_REQUEST_DEST/daily
MANTA_COMPUTE_DEST_DAILY=$MANTA_COMPUTE_DEST/daily

MANTA_STORAGE_DEST_MONTHLY=$MANTA_STORAGE_DEST/monthly
MANTA_REQUEST_DEST_MONTHLY=$MANTA_REQUEST_DEST/monthly
MANTA_COMPUTE_DEST_MONTHLY=$MANTA_COMPUTE_DEST/monthly

# Source directories to pull logs from
MANTA_STORAGE_SOURCE_HOURLY=/$MANTA_USER/stor/manatee_backups
MANTA_REQUEST_SOURCE_HOURLY=/$MANTA_USER/stor/logs/muskie

MANTA_STORAGE_SOURCE_DAILY=$MANTA_STORAGE_DEST_HOURLY
MANTA_REQUEST_SOURCE_DAILY=$MANTA_REQUEST_DEST_HOURLY

MANTA_STORAGE_SOURCE_MONTHLY=$MANTA_STORAGE_DEST_DAILY
MANTA_REQUEST_SOURCE_MONTHLY=$MANTA_REQUEST_DEST_DAILY


## Number of reducers

STORAGE_NUM_REDUCERS1_HOURLY=2
STORAGE_NUM_REDUCERS2_HOURLY=2
REQUEST_NUM_REDUCERS_HOURLY=2

STORAGE_NUM_REDUCERS_DAILY=2
REQUEST_NUM_REDUCERS_DAILY=2

STORAGE_NUM_REDUCERS_MONTHLY=2
REQUEST_NUM_REDUCERS_MONTHLY=2
