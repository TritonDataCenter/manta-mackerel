# Mackerel

Repository: <git@git.joyent.com:mackerel.git>
Browsing: <https://mo.joyent.com/mackerel>
Who: Fred Kuo
Docs: <https://mo.joyent.com/docs/mackerel>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA-195>
//TODO
Mention bandwidth overhead in marlin compute jobs


# Overview

Metering and usage reports for Manta.

Metering code is in two main categories - scripts that set up and kick off
Marlin jobs (job creation), and code that is uploaded as assets and
executed as part of a Marlin job (assets).

Job creation code is in /lib
Programs that generate job input keys are in /lib/keygen
Assets are in /assets
Configuration files are in /etc

Storage data comes from Postgres dumps from each Moray shard.
Request data comes from Muskie audit logs.
Compute data comes from marlin agent audit logs.

# Running metering jobs

Run metering jobs using

    bin/meter [-f configPath] [-w] -d date -j jobName

where date is in some format readable by `Date.parse()` and jobNAme is one of
`storage`, `compute`, `request`, `accessLogs`, or `summarizeDaily`. Set the -w
to use workflow to manage the metering job.

Examples:

    bin/meter -j "storage" -d "$(date)"
    bin/meter -j "accessLogs" -d "$(date -d '1 month ago')" -w
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

        // job configuration settings
        'jobs': {
            category: { // e.g. storage, request, compute
                period: { // e.g. hourly, daily, monthly
                    'keygen': '/local/path/to/keygen.js',

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
                                'exec': execStr,
                                ...
                        }, {
                        ...
                        } ]
                    },

                    // which workflow to use
                    'workflow': workflowname

                    // where to create a link to the result
                    'linkPath': '/path/to/latest'

                    // any environment variables you need
                    // set at job runtime
                    env: {
                        'DEST': '/manta/path.json',
                        ...
                    }
                },
                ...
            },
            ...
        },

    }

Request metering categorizes requests and bandwidth by the network it came
from.  Specify the ranges and networks in networks.json as an array of {name,
ranges} tuples, where ranges is an array of IP ranges (v6 supported) in CIDR
notation.

A request is categorized for a network by looking through the array
and checking for matches for any of the ranges in each network, and the first
matching range is selected, so the order of the tuples is important.

    [
        {
            'name': 'internal',
            'ranges': [ '10.0.0.0/8', '192.168.0.0/16' ]
        },
        {
            'name': 'external',
            'ranges': [ '0.0.0.0/0' ]
        }
    ]


# Testing

Mackerel needs a Manta deployment. Point to a Manta deployment by filling in
the fields in `etc/test-config.js`, which overrides config entries in the normal
config file at `etc/config.js`. Set `TEST_USER` in your environment to match the
user in the manta config file. Entries to override include:

    * mantaConfigFile: path to a manta config file.
    * mahi.host: mahi host for generating lookups
    * workflow.url: url of a manta workflow

Sample Manta config file:

    {
        "manta": {
            "connectTimeout": 1000,
            "retry": {
                "attempts": 5,
                "minTimeout": 1000
            },
            "sign": {
                "key": "/home/dev/.ssh/id_rsa",
                "keyId": "e3:4d:9b:26:bd:ef:a1:db:43:ae:4b:f7:bc:69:a7:24"
            },
            "url": "https://manta-beta.joyentcloud.com",
            "user": "fredkuo"
        }
    }


Examples:

    TEST_USER=fredkuo make test
