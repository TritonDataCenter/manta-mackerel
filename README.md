# Mackerel

Repository: <git@git.joyent.com:mackerel.git>
Browsing: <https://mo.joyent.com/mackerel>
Who: Fred Kuo
Docs: <https://mo.joyent.com/docs/mackerel>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA-195>


# Overview

Metering and usage reports for Manta.

Metering code is in two main categories - scripts that set up and kick off
Marlin jobs (job creation scripts), and code that is uploaded as assets and
executed as part of a Marlin job (job code). Configuration settings are read
from /cfg/config.sh for both job creation scripts and job code.

Job creation scripts are in /scripts.
Job code is in /bin and /lib.
Configuration file is in /cfg.

Storage data comes from pg dumps from each moray shard.
Request data comes from audit logs from muskie.

# Running metering jobs

Run metering jobs using

    scripts/meter.sh -d <date> -p <period> -s <service>

where date is in some format readable by date(1), period is one of "hourly",
"daily", or "monthly" and service is one of "storage", "request", or "compute".

Examples:

    scripts/meter.sh -p hourly -s storage -d "`date`"
    scripts/meter.sh -p monthly -s request -d "`date -d '1 hour ago'`"

# Testing

    make test
