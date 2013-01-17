# Mackerel

Repository: <git@git.joyent.com:mackerel.git>
Browsing: <https://mo.joyent.com/mackerel>
Who: Fred Kuo
Docs: <https://mo.joyent.com/docs/mackerel>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA-195>


# Overview

Metering and usage reports for Manta.

Metering code is in two main categories - scripts that set up and kick off
Marlin jobs (job creation), and code that is uploaded as assets and
executed as part of a Marlin job (assets).

Job creation code is in /lib
Assets are in /assets
Configuration file is in /cfg

Storage data comes from pg dumps from each Moray shard.
Request data comes from audit logs from Muskie.

# Running metering jobs

Run metering jobs using

    bin/meter -d date -p period -s service [-c configPath]

where date is in some format readable by date(1), period is one of "hourly",
"daily", or "monthly" and service is one of "storage", "request", or "compute".

Examples:

    bin/meter -p hourly -s storage -d "$(date)"
    bin/meter -p monthly -s request -d "$(date -d '1 month ago')"
    bin/meter -p daily -s compute -d "$(date -d '2 days ago')" -c path/to/config

# Testing

    make test
