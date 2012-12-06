#!/bin/bash -x

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $dir/../cfg/config.sh

function fatal() {
        if [ $1 -ne 0 ]
        then
                echo "Error $2"
                exit 1
        fi
        return 0
}

function warn() {
        if [ $1 -ne 0 ]
        then
                echo "Warning: error $2"
                return 1
        fi
        return 0
}

function monitor() {
        local jobid=$1
        local count=0
        local t=1

        while [ $(mjob $jobid | json state) != "done" ];
        do
                if [ $count -gt $SLEEP_RETRY ]
                then
                        echo "Error: exceeded retry limit for job output for" \
                                " job $jobid"
                        exit 1
                fi

                sleep $t
                t=$(expr $t \* 2)
                count=$(expr $count + 1)
        done

        mjob -o $jobid

        local failures=$(mjob -x $jobid)
        if [ ! -z "$failures" ]
        then
                echo "Error: failures detected for job $jobid."
                echo $failures
        fi
}

function split-usage() {
        local output=$(mjob -o $jobid)

        if [ -z $output ]; then
                echo "Warning: no output for job $jobid. Exiting.."
                exit 1
        fi

        # create lookup file
        mget $output \
        | $ZCAT \
        | json -ga owner \
        | xargs -I xx redis-cli -h $(mdata-get auth_cache_name) get /uuid/xx \
        | tr -d \" > /tmp/_mackerel_lookup

        mput -f /tmp/_mackerel_lookup $LOOKUP

        # get names
        printf -v dest_dir_fmt "MANTA_USAGE_%s_%s" $service $period
        dest_dir_fmt=${!dest_dir_fmt}
        eval local dest_dir=$dest_dir_fmt

        printf -v job_name_fmt "MANTA_USAGE_NAME_%s_%s" $service $period
        job_name_fmt=${!job_name_fmt}
        eval local job_name=$job_name_fmt

        printf -v name_fmt "MANTA_USAGE_NAME_%s" $period
        name_fmt=${!name_fmt}
        eval name=$name_fmt

        cat /tmp/_mackerel_lookup | xargs -I xx mmkdir -p /xx/$dest_dir

        # create the job
        local job=$(mmkjob \
                -n "$job_name" \
                -s "$LOOKUP" \
                -s "$CONFIG" \
                -s "$SPLIT_USAGE_MAP_CMD" \
                -m "$ZCAT \
                    | dest=$dest_dir name=$name \
                        /assets/$SPLIT_USAGE_MAP_CMD /assets/$LOOKUP"
        )

        echo "$output" | maddkeys $job
        warn "$?" "adding keys to $job"

        # end the job
        mjob -e $job
        fatal "$?" "ending job $job"

        monitor $job
}

# -- Mainline -- #
# get opts
while getopts ":d:p:s:" opt
do
        case $opt in
        d)
                date=$(date --utc -d "$OPTARG" "+%Y-%m-%d %H")

                if [ $? -ne 0 ]
                then
                        echo "Invalid date: $@" >&2
                        exit 1
                fi

                year=$(date -d "$date" +%Y)
                month=$(date -d "$date" +%m)
                day=$(date -d "$date" +%d)
                hour=$(date -d "$date" +%H)
                ;;
        p)
                period=$(echo $OPTARG | tr '[:lower:]' '[:upper:]')

                if [ $period != "HOURLY" -a \
                        $period != "DAILY" -a \
                        $period != "MONTHLY" ]
                then
                        echo "One of 'hourly' 'daily', or 'monthly'" \
                                " required for option -$opt" >&2
                        exit 1
                fi

                ;;
        s)
                service=$(echo $OPTARG | tr '[:lower:]' '[:upper:]')

                if [ $service != "REQUEST" -a \
                        $service != "STORAGE" -a \
                        $service != "COMPUTE" ]
                then
                        echo "One of 'request' 'storage', or 'compute'" \
                                " required for option -$opt" >&2
                        exit 1
                fi

                ;;
        \?)
                echo "Invalid option: -$OPTARG" >&2
                exit 1
                ;;
        :)
                echo "Argument required for option -$OPTARG" >&2
                exit 1
                ;;
        esac
done

if [ -z $date ]; then
        echo "Date required (-d)." >&2
        exit 1
fi

if [ -z $service ]; then
        echo "Service required (-s). One of 'request', 'storage', or 'compute'" >&2
        exit 1
fi

if [ -z $period ]; then
        echo "Period required (-p). One of 'hourly', 'daily', or 'monthly'" >&2
        exit 1
fi

# set up names
printf -v dest_dir_fmt "MANTA_%s_DEST_%s" $service $period
dest_dir_fmt=${!dest_dir_fmt}
eval dest_dir=$dest_dir_fmt

mmkdir -p $dest_dir
fatal "$?" "creating directory $dest_dir"

printf -v job_name_fmt "MANTA_JOB_NAME_%s_%s" $service $period
job_name_fmt=${!job_name_fmt}
eval job_name=$job_name_fmt

printf -v name_fmt "MANTA_NAME_%s" $period
name_fmt=${!name_fmt}
eval name=$name_fmt

# Create the job
if [ $service == "REQUEST" ]; then
        if [ $period == "HOURLY" ]; then
                jobid=$(mmkjob \
                        -n "$job_name" \
                        -s "$REQUEST_MAP_CMD_HOURLY" \
                        -s "$REQUEST_REDUCE_CMD_HOURLY" \
                        -s "$COLLATE_CMD" \
                        -s "$CONFIG" \
                        -m "/assets/$REQUEST_MAP_CMD_HOURLY" \
                        -r "/assets/$REQUEST_REDUCE_CMD_HOURLY" \
                                -c "$REQUEST_NUM_REDUCERS_HOURLY" \
                        -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
                )
        fi

        if [ $period == "DAILY" ]; then
                jobid=$(mmkjob \
                        -n "$job_name" \
                        -s "$REQUEST_REDUCE_CMD_DAILY" \
                        -s "$COLLATE_CMD" \
                        -s "$CONFIG" \
                        -s "$LIB_SUM_COLUMNS" \
                        -r "/assets/$REQUEST_REDUCE_CMD_DAILY" \
                                -c "$REQUEST_NUM_REDUCERS_DAILY" \
                        -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
                )
        fi

        if [ $period == "MONTHLY" ]; then
                jobid=$(mmkjob \
                        -n "$job_name" \
                        -s "$REQUEST_REDUCE_CMD_MONTHLY" \
                        -s "$COLLATE_CMD" \
                        -s "$CONFIG" \
                        -s "$LIB_SUM_COLUMNS" \
                        -r "/assets/$REQUEST_REDUCE_CMD_MONTHLY" \
                                -c "$REQUEST_NUM_REDUCERS_MONTHLY" \
                        -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
                )
        fi
fi

if [ $service == "STORAGE" ]; then
        if [ $period == "HOURLY" ]; then
                jobid=$(mmkjob \
                        -n "$job_name" \
                        -s "$STORAGE_MAP_CMD_HOURLY" \
                        -s "$STORAGE_REDUCE1_CMD_HOURLY" \
                        -s "$STORAGE_REDUCE2_CMD_HOURLY" \
                        -s "$COLLATE_CMD" \
                        -s "$CONFIG" \
                        -m "/assets/$STORAGE_MAP_CMD_HOURLY" \
                        -r "/assets/$STORAGE_REDUCE1_CMD_HOURLY" \
                                -c "$STORAGE_NUM_REDUCERS1_HOURLY" \
                        -r "/assets/$STORAGE_REDUCE2_CMD_HOURLY" \
                                -c "$STORAGE_NUM_REDUCERS2_HOURLY" \
                        -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
                )
        fi
        if [ $period == "DAILY" ]; then
                jobid=$(mmkjob \
                        -n "$job_name" \
                        -s "$STORAGE_REDUCE_CMD_DAILY" \
                        -s "$COLLATE_CMD" \
                        -s "$CONFIG" \
                        -s "$LIB_SUM_COLUMNS" \
                        -r "/assets/$STORAGE_REDUCE_CMD_DAILY" \
                                -c "$STORAGE_NUM_REDUCERS_DAILY"
                        -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
                )
        fi
        if [ $period == "MONTHLY" ]; then
                jobid=$(mmkjob \
                        -n "$job_name" \
                        -s "$STORAGE_REDUCE_CMD_MONTHLY" \
                        -s "$COLLATE_CMD" \
                        -s "$CONFIG" \
                        -s "$LIB_SUM_COLUMNS" \
                        -r "/assets/$STORAGE_REDUCE_CMD_MONTHLY" \
                                -c "$STORAGE_NUM_REDUCERS_MONTHLY"
                        -r "dest=$dest_dir name=$name /assets/$COLLATE_CMD"
                )
        fi
fi

fatal "$?" "creating job $job_name"

# add keys
printf -v keygen "KEYGEN_%s_%s" $service $period
keygen=${!keygen}
$keygen $date | maddkeys $jobid
warn "$?" "adding keys to $jobid"

# end the job
mjob -e $jobid
fatal "$?" "ending job $jobid"

# monitor for output
monitor $jobid
# split usage into customer directories
split-usage
