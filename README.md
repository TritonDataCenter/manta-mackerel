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

# Testing

    make test
