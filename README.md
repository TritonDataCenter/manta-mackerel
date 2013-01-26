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
Programs that generate job input keys are in /lib/keygen
Assets are in /assets
Configuration file is in /etc

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

# Configuration file

The configuration file should be in a format compatible with require (i.e.
JSON or a module that exports a JS object).

Configuration settings include:

        {
                // local path to manta client config
                'mantaConfigFile': '/path/to/file',

                // mapping from manta object to local file path
                'assets': {
                        '/manta/asset/path': '/local/file/path',
                        ...
                }

                // retry configuration settings for result monitoring
                'backoff': {
                        'initialDelay': 1000, // milliseconds
                        'maxDelay': 120000, // milliseconds
                        'failAfter': 20 // count
                }

                // job configuration settings
                'jobs': {
                        service: { // e.g. storage, request, compute
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

                // redis config
                c.redis = {
                        port: 6379,
                        hostname: 'localhost',
                        clientOpts: { ... } // any additional client options
                }
        }



# Testing

    make test
