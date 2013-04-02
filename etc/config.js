#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

/*
 * Dynamically generated JSON configuration file for mackerel
 *
 * If run directly, prints out config as JSON for easy parsing/printing (e.g.
 * pipe to 'json').
 *
 * SECTIONS:
 * - local helper variables
 * - mahi config
 * - workflow config
 * - assets
 * - backoff settings
 * - job configuration
 */

var c = {};

// manta client config file
c.mantaConfigFile = '/opt/smartdc/common/etc/config.json';

/******************************/
/*   LOCAL HELPER VARIABLES   */
/******************************/

var user, mbase, md, lbase, ld, userbase;

// running the file should always generate valid json, so wrap this in try/catch
try {
    user = require(c.mantaConfigFile).manta.user;
} catch (e) {
    user = 'poseidon';
}


/*
 * Modify these variables to match your environment.
 */

mbase = '/' + user + '/stor/usage'; // manta base directory
md = mbase + '/assets'; // manta assets directory
userbase = '/reports/usage'; // user-accessible base directory

lbase = '/opt/smartdc/mackerel'; // local base directory
ld = lbase + '/assets'; // local assets directory


/******************************/
/*         MAHI CONFIG        */
/******************************/

c.mahi = {
    // replaced with $(mdata-get auth_cache_name) at zone setup time
    host: 'REDIS_HOST',
    port: 6379,

    // optional client options
    maxParallel: undefined, // maximum parallel requests sent to redis
    redis_options: undefined, // additional options passed to the client

    // for retry
    connectTimeout: undefined,
    checkInterval: undefined,
    retries: undefined,
    minTimeout: undefined,
    maxTimeout: undefined
}


/******************************/
/*   WORKFLOW CLIENT CONFIG   */
/******************************/

c.workflow = {
    // TODO replace with mdata-get ??? at zone setup time
    url: 'http://localhost:8080',
    path: lbase + '/lib/workflows'
};


/******************************/
/*           ASSETS           */
/******************************/

// assets is a mapping from the manta object path to the local file path
c.assets = {};
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
c.assets[md + '/lib/memorystream.js'] = lbase + '/node_modules/memorystream-mcavage/index.js';
c.assets[md + '/lib/storage-map.js'] = ld + '/lib/storage-map.js';
c.assets[md + '/lib/storage-reduce1.js'] = ld + '/lib/storage-reduce1.js';
c.assets[md + '/lib/storage-reduce3.js'] = ld + '/lib/storage-reduce3.js';
c.assets[md + '/lib/deliver-usage.js'] = ld + '/lib/deliver-usage.js';
c.assets[md + '/etc/ranges.json'] = ld + '/etc/ranges.json';
c.assets[md + '/lib/node_modules/ipaddr.js'] = lbase + '/node_modules/range_check/node_modules/ipaddr.js/lib/ipaddr.js';
c.assets[md + '/lib/php.js'] = lbase + '/node_modules/range_check/php.js';
c.assets[md + '/lib/range_check.js'] = lbase + '/node_modules/range_check/range_check.js';
// since the lookup file is generated at job run time, meter.js needs to know
// the manta path for the lookup file
c.mantaLookupPath = md + '/etc/lookup.json';
c.assets[c.mantaLookupPath] = ld + '/etc/lookup.json';


/******************************/
/*       BACKOFF SETTINGS     */
/******************************/

// retry configuration settings for job result monitoring
c.monitorBackoff = {
    initialDelay: 1000, // 1 second
    maxDelay: 120000, // 2 minutes
    failAfter: 30 // ~ 50 minutes total
};


/******************************/
/*      JOB CONFIGURATION     */
/******************************/

// job configuration
// each job object must have a 'keygen' field pointing to the path of the
// keygen file, and a 'job' field containing the job manifest.
c.jobs = {};
c.jobs.storage = {
    hourly: {
        // keygen path (required)
        keygen: lbase + '/lib/keygen/StorageHourlyKeyGenerator.js',

        // additional keygen arguments (date will always be passed in)
        keygenArgs: {
            // where to find source keys
            source: '/poseidon/stor/manatee_backups'
        },

        // name of the workflow. depends on which key generator is
        // used for this job
        workflow: 'storage-hourly-runjob',

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
                memory: 2048,
                assets : [
                    md + '/bin/storage-reduce1',
                    md + '/lib/carrier.js',
                    md + '/lib/storage-reduce1.js'
                ],
                exec: '/assets' + md + '/bin/storage-reduce1',
                count: 1
            }, {
                type: 'reduce',
                assets : [
                    md + '/bin/storage-reduce2',
                    md + '/lib/carrier.js',
                    md + '/lib/sum-columns.js'
                ],
                exec: '/assets' + md + '/bin/storage-reduce2',
                count: 1
            }, {
                type: 'reduce',
                assets: [
                    md + '/bin/storage-reduce3',
                    md + '/lib/carrier.js',
                    md + '/lib/storage-reduce3.js'
                ],
                exec: '/assets' + md + '/bin/storage-reduce3',
                count: 1
            }, {
                type: 'reduce',
                assets: [
                    md + '/bin/deliver-usage',
                    md + '/etc/lookup.json',
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
            // NOTE: the order in which the namespaces appear here will be
            // the order in which links appearing in multiple namespaces will
            // be counted
            // example: if links pointing to the same object appear in both
            // /stor and /public, and the desired behavior is to count the
            // object under the /stor namespace over the public namespace,
            // put "stor" before "public" in the space-separated list below:
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
        workflow: 'find-runjob',
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
                    md + '/etc/lookup.json',
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
        workflow: 'find-runjob',
        linkPath: mbase + '/storage/latest-monthly',
        job: {
            name: 'metering-storage-monthly-$year-$month',
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
                    md + '/etc/lookup.json',
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
        workflow: 'find-runjob',
        linkPath: mbase + '/request/latest-hourly',
        job: {
            name: 'metering-request-hourly-$year-$month-$day-$hour',
            phases: [ {
                type: 'storage-map',
                assets: [
                    md + '/bin/request-map',
                    md + '/lib/ip_addr.js',
                    md + '/lib/php.js',
                    md + '/lib/range_check.js',
                    md + '/lib/request-map.js'
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
                    md + '/etc/lookup.json',
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
        workflow: 'find-runjob',
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
                    md + '/etc/lookup.json',
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
        workflow: 'find-runjob',
        linkPath: mbase + '/request/latest-monthly',
        job: {
            name: 'metering-request-monthly-$year-$month',
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
                    md + '/etc/lookup.json',
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
