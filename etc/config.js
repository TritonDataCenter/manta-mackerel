#!/usr/bin/env node
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

/*
 * Configuration file for mackerel job runner
 * If run directly, prints out config as JSON for easy parsing/printing (e.g.
 * pipe to 'json').
 */

var c = {};

// -- manta config
// manta client config file
c.mantaConfigFile = '/opt/smartdc/common/etc/config.json';
var user;
try {
        user = require(c.mantaConfigFile).manta.user;
} catch (e) {
        user = 'poseidon';
}

// -- assets
var mbase = '/' + user + '/stor/usage'; // manta base directory
var md = mbase + '/assets'; // manta assets directory
var lbase = '/opt/smartdc/mackerel'; // local base directory
var ld = lbase + '/assets'; // local assets directory
var userbase = '/reports/usage'; // user-accessible base directory

// redis config
c.redis = {
        port: 6379,
        host: 'auth.beta.joyent.us', // TODO retrieve this programmatically - via environment?

        // optional client options
        maxParallel: undefined, // maximum parallel requests sent to redis
        options: {}, // additional options passed to client

        // for retry
        connectTimeout: undefined,
        retries: undefined,
        minTimeout: undefined,
        maxTimeout: undefined
}

// assets is a mapping from the manta object path to the local file path
c.assets = {}
c.assets[md + '/bin/avg-columns'] = ld + '/bin/avg-columns';
c.assets[md + '/bin/deliver-usage'] = ld + '/bin/deliver-usage';
c.assets[md + '/bin/request-map'] = ld + '/bin/request-map';
c.assets[md + '/bin/request-reduce'] = ld + '/bin/request-reduce';
c.assets[md + '/bin/split-usage'] = ld + '/bin/split-usage';
c.assets[md + '/bin/storage-map'] = ld + '/bin/storage-map';
c.assets[md + '/bin/storage-reduce1'] = ld + '/bin/storage-reduce1';
c.assets[md + '/bin/storage-reduce2'] = ld + '/bin/storage-reduce2';
c.assets[md + '/bin/storage-reduce3'] = ld + '/bin/storage-reduce3';
c.assets[md + '/bin/sum-columns'] = ld + '/bin/sum-columns';
c.assets[md + '/bin/avg-columns'] = ld + '/bin/avg-columns';
c.assets[md + '/lib/sum-columns.js'] = ld + '/lib/sum-columns.js';
c.assets[md + '/lib/avg-columns.js'] = ld + '/lib/avg-columns.js';
c.assets[md + '/lib/carrier.js'] = lbase + '/node_modules/carrier/lib/carrier.js';
c.assets[md + '/lib/memorystream.js'] = lbase + '/node_modules/memorystream/index.js';
c.assets[md + '/lib/storage-map.js'] = ld + '/lib/storage-map.js';
c.assets[md + '/lib/storage-reduce1.js'] = ld + '/lib/storage-reduce1.js';
c.assets[md + '/lib/storage-reduce3.js'] = ld + '/lib/storage-reduce3.js';
c.assets[md + '/lib/deliver-usage.js'] = ld + '/lib/deliver-usage.js';
c.assets[md + '/cfg/lookup.json'] = ld + '/cfg/auto-generated-lookup.json';


// retry configuration settings for job result monitoring
c.monitorBackoff = {};
c.monitorBackoff.initialDelay = 1000; // 1 second
c.monitorBackoff.maxDelay = 120000; // 2 minutes
c.monitorBackoff.failAfter = 20; // ~ 30 minutes total

// retry configuration settings for job failures
c.backoff = {};
c.backoff.initialDelay = 60000; // 1 minute
c.backoff.maxDelay = 600000; // 10 minutes
c.backoff.failAfter = 5;

// job configuration
// each job object must have a 'keygen' field pointing to the path of the
// keygen file, and a 'job' field containing the job manifest.
c.jobs = {};
c.jobs.storage = {
        hourly: {
                // keygen path (required)
                keygen: lbase + '/lib/keygen/StorageHourlyKeyGenerator.js',

                // additional keygen arguments
                keygenArgs: {
                        // where to find source keys
                        source: '/poseidon/stor/manatee_backups'
                },

                // manta destination path for link to latest report
                linkPath: mbase + '/storage/latest-hourly',

                // job manifest (required)
                job: {
                        name: 'metering-storage-hourly-$year-$month-$day-$hour',
                        phases: [ {
                                type : 'storage-map',
                                assets : [
                                        md + '/bin/storage-map',
                                        md + '/lib/carrier.js',
                                        md + '/lib/storage-map.js'
                                ],
                                exec : '/assets' + md + '/bin/storage-map'
                        }, {
                                type: 'reduce',
                                assets : [
                                        md + '/bin/storage-reduce1',
                                        md + '/lib/carrier.js',
                                        md + '/lib/storage-reduce1.js'
                                ],
                                exec: '/assets' + md + '/bin/storage-reduce1',
                                count: 2
                        }, {
                                type: 'reduce',
                                assets : [
                                        md + '/bin/storage-reduce2',
                                        md + '/lib/carrier.js',
                                        md + '/lib/sum-columns.js'
                                ],
                                exec: '/assets' + md + '/bin/storage-reduce2',
                                count: 2
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/storage-reduce3',
                                        md + '/lib/carrier.js',
                                        md + '/lib/storage-reduce3.js'
                                ],
                                exec: '/assets' + md + '/bin/storage-reduce3',
                                count: 2
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },

                // specific job environment settings that will be prepended to each exec string
                env: {
                        NAMESPACES: 'stor public jobs reports',
                        ZCAT: 'gzcat',

                        HEADER_CONTENT_TYPE: 'application/x-json-stream',

                        // manta destination path (for final mpipe)
                        DEST: mbase + '/storage/$year/$month/$day/$hour/h$hour.json',

                        // manta destination path for user reports
                        // user login will be prepended to this path during the job
                        USER_DEST: userbase + '/storage/$year/$month/$day/$hour/h$hour.json'
                }
        },
        daily: {
                keygen: lbase + '/lib/keygen/FindKeyGenerator.js',
                keygenArgs: {
                        source: '/poseidon/stor/usage/storage/$year/$month/$day',
                        regex: 'h[0-9][0-9]'
                },
                linkPath: mbase + '/storage/latest-daily',
                job: {
                        name: 'metering-storage-daily-$year-$month-$day',
                        phases: [ {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/sum-columns',
                                        md + '/lib/sum-columns.js',
                                        md + '/lib/carrier.js'
                                ],
                                exec: '/assets' + md + '/bin/sum-columns',
                                count: 1
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                env: {
                        HEADER_CONTENT_TYPE: 'application/x-json-stream',
                        DEST: mbase + '/storage/$year/$month/$day/d$day.json',
                        USER_DEST: userbase + '/storage/$year/$month/$day/d$day.json'
                }
        },
        monthly: {
                keygen: lbase + '/lib/keygen/FindKeyGenerator.js',
                keygenArgs: {
                        source: '/poseidon/stor/usage/storage/$year/$month',
                        regex: 'd[0-9][0-9]'
                },
                linkPath: mbase + '/storage/latest-monthly',
                job: {
                        name: 'metering-storage-daily-$year-$month',
                        phases: [ {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/sum-columns',
                                        md + '/lib/sum-columns.js',
                                        md + '/lib/carrier.js'
                                ],
                                exec: '/assets' + md + '/bin/sum-columns',
                                count: 1
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                env: {
                        HEADER_CONTENT_TYPE: 'application/x-json-stream',
                        DEST: mbase + '/storage/$year/$month/m$month.json',
                        USER_DEST: userbase + '/storage/$year/$month/m$month.json'
                }
        }
};
c.jobs.request = {
        hourly: {
                keygen: lbase + '/lib/keygen/FindKeyGenerator.js',
                keygenArgs: {
                        source: '/poseidon/stor/logs/muskie/$year/$month/$day/$hour'
                },
                linkPath: mbase + '/request/latest-hourly',
                job: {
                        name: 'metering-request-hourly-$year-$month-$day-$hour',
                        phases: [ {
                                type: 'storage-map',
                                assets: [
                                        md + '/bin/request-map'
                                ],
                                exec: '/assets' + md + '/bin/request-map'
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/request-reduce'
                                ],
                                exec: '/assets' + md + '/bin/request-reduce',
                                count: 2
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                env: {
                        HEADER_CONTENT_TYPE: 'application/x-json-stream',
                        DEST: mbase + '/request/$year/$month/$day/$hour/h$hour.json',
                        USER_DEST: userbase + '/request/$year/$month/$day/$hour/h$hour.json'
                }
        },
        daily: {
                keygen: lbase + '/lib/keygen/FindKeyGenerator.js',
                keygenArgs: {
                        source: '/poseidon/stor/usage/request/$year/$month/$day',
                        regex: 'h[0-9][0-9]'
                },
                linkPath: mbase + '/request/latest-daily',
                job: {
                        name: 'metering-request-daily-$year-$month-$day',
                        phases: [ {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/sum-columns',
                                        md + '/lib/carrier.js',
                                        md + '/lib/sum-columns.js'
                                ],
                                exec: '/assets' + md + '/bin/sum-columns',
                                count: 1
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                env: {
                        HEADER_CONTENT_TYPE: 'application/x-json-stream',
                        DEST: mbase + '/request/$year/$month/$day/d$day.json',
                        USER_DEST: userbase + '/request/$year/$month/$day/d$day.json'
                },
        },
        monthly: {
                keygen: lbase + '/lib/keygen/FindKeyGenerator.js',
                keygenArgs: {
                        source: '/poseidon/stor/usage/request/$year/$month',
                        regex: 'd[0-9][0-9]'
                },
                linkPath: mbase + '/request/latest-monthly',
                job: {
                        name: 'metering-request-daily-$year-$month',
                        phases: [ {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/sum-columns',
                                        md + '/lib/sum-columns.js',
                                        md + '/lib/carrier.js'
                                ],
                                exec: '/assets' + md + '/bin/sum-columns',
                                count: 1
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                env: {
                        HEADER_CONTENT_TYPE: 'application/x-json-stream',
                        DEST: mbase + '/request/$year/$month/m$month.json',
                        USER_DEST: userbase + '/request/$year/$month/m$month.json'
                }
        }
};
/*
c.jobs.compute = {
        hourly: {
                phases [ {
                },
                ...
        },
        daily: {
                phases [ {
                },
                ...
        },
        monthly: {
                phases [{
                },
                ...
        }
}
*/

if (require.main === module) {
        console.log(JSON.stringify(c));
}

module.exports = c;
