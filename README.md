<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019 Joyent, Inc.
-->

# manta-mackerel

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

Mackerel generates Metering and usage reports for Manta.


## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/joyent/manta/blob/master/docs/mantav2.md) for
details on major Manta versions.

- [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta.
- [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.


## Overview

Metering code is in two main categories - scripts that set up and kick off
Marlin jobs (job creation), and code that is uploaded as assets and
executed as part of a Marlin job (assets).

Job creation code is in /lib
Programs that generate job input keys are in /lib/keygen
Assets are in /assets
Configuration files are in /etc

Storage data comes from Postgres dumps from each Moray shard.
Request data comes from Muskie audit logs.
Compute data comes from Marlin agent audit logs.

# Running metering jobs

Run metering jobs using

    bin/meter [-f configPath] [-c] [-r] -d date -j jobName

where date is in some format readable by `Date.parse()` (e.g. ISO-8601 or
output from date(1)) and jobName is one of `storage`, `compute`, `request`,
`accessLogs`, or `summarizeDaily`. Setting the -c flag will print the job
manifest without creating a job. Setting the -r flag will automatically retry
the job in the event of failures.

Examples:

    bin/meter -j "storage" -d "$(date)"
    bin/meter -j "accessLogs" -d "$(date -d '1 month ago')"
    bin/meter -j "summarizeDaily" "$(date -d '2 days ago')" -f path/to/config

# Configuration file

The configuration file config.json should be in a format compatible with require
(i.e.  JSON or a module that exports a JS object).

Configuration settings include:

    {
        // local path to manta client config
        'mantaConfigFile': '/path/to/file',

        // mapping from manta object to local file path
        'assets': {
            '/manta/asset/path': '/local/file/path',
            ...
        }

        // where the uuid->login mapping is written to locally
        'mantaLookupPath': '/path/to/file'

        // retry configuration settings for result monitoring
        'monitorBackoff': {
            'initialDelay': 1000, // milliseconds
            'maxDelay': 120000, // milliseconds
            'failAfter': 20 // count
        }

        // mahi config
        c.mahi = {
            host: 'localhost',
            port: 6379
        }
    }

Job details look like:

    {
        'jobs': {
            jobName: { // e.g. storage, request, compute

                'keygen': 'name of key generator', // under /lib/keygen

                // any arguments to pass to the keygen
                'keygenArgs': {
                    'source': '/manta/source/path'
                },

                // the job manifest passed to marlin
                'job': {
                    'name': name,
                    'phases': [ {
                            'type': type,
                            'assets': [...],
                            'init': initStr,
                            'exec': execStr
                    }, {
                    ...
                    } ]
                },

                // where to create a link to the result when the job finishes
                'linkPath': '/path/to/latest'

                // any environment variables you need set at job runtime
                env: {
                    'DEST': '/manta/path.json',
                    ...
                }
            },
            ...
        },
        ...

    }

Some job configuration is passed to a job via environment variables.

* ACCESS_DEST: the Manta path to append to "/:user" for access logs
* COUNT_UNAPPROVED_USERS: if true, users with approved_for_provisioning set to
false will be included in reports DATE: the date the job will meter for
* DELIVER_UNAPPROVED_REPORTS: if true, users with approved_for_provisioning set
to false will receive usage reports in their /reports directory DEST: the
Manta path where usage reports should be saved to
* DROP_POSEIDON_REQUESTS: if true, drop requests where poseidon is the caller
from usage reports or access logs HEADER_CONTENT_TYPE: content type for usage
reports
* MALFORMED_LIMIT: in request and compute, how many lines of malformed json to
allow before throwing an error. scalar ("500") or percent ("1%") MIN_SIZE:
minimum size of objects in storage metering
* NAMESPACES: a list of all namespaces in Manta
* USER_DEST: the Manta path to append to "/:user" for usage reports
* USER_LINK: the path to create a link to the latest usage report generated for
a user



## Testing

Mackerel needs a Manta deployment. Point to a Manta deployment by filling in
the fields in `etc/test-config.js`, which overrides config entries in the normal
config file at `etc/config.js`. Entries to override include:

    * manta url/user/sign: manta credentials
    * mantaBaseDirectory: modify this to make the integration tests use your
        directory instead of the default /poseidon/stor/usage
    * lookupFile: path to the test lookup file
    * mahi.host: mahi host (only used for generating lookups)

Run tests using:
    make test

To run a single integration test:
    nodeunit -t storage test/integration.test.js

## Resharding

See MANTA-1744 and MANTA-1665.

There are two things that need to happen for consumers of dumps when resharding
happens:
1. Shard discovery via Manta, and
2. Dedupe rows

Ideally, these two things would happen automatically. However, the infrequeent
nature of resharding makes automating these tasks low priority.

If a reshard needed to happen and the automatic systems were not in place yet,
shard discovery would need to updated manually via configuration. Duplicate rows
would be removed via marlin jobs that would run over the (very few) dumps that
have duplicates.

A somewhat related but separate issue is the transformation of dumps from SQL
to JSON. Currently that transformation is occurring on the postgres boxes
themselves. However, as the datasets get larger, it would be preferable to use
marlin to transform the dumps.
