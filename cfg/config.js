#!/usr/bin/env node
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

/*
 * Configuration file for mackerel job runner
 */

var c = {};

// -- manta config
// manta client config file
c.manta_config_file = '/opt/smartdc/common/etc/config.json';
var user = require(c.manta_config_file).manta.user;

// -- assets
var mbase = '/' + user + '/stor/usage'; // manta base directory
var md = mbase + '/assets'; // manta assets directory
var lbase = '/opt/smartdc/mackerel'; // local base directory
var ld = lbase + '/assets'; // local assets directory
var userbase = '/reports/usage'; // user-accessible base directory

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
c.assets[md + '/cfg/config.sh'] = ld + '/cfg/auto-generated-config.sh';


// retry configuration settings for job result monitoring
c.backoff = {};
c.backoff.initialDelay = 1000; // 1 second
c.backoff.maxDelay = 120000; // 2 minutes
c.backoff.failAfter = 20; // ~ 30 minutes total

// job configuration
// each job object must have a 'keygen' field pointing to the path of the
// keygen file, and a 'job' field containing the job manifest.
c.jobs = {};
c.jobs.storage = {
        hourly: {
                // keygen path (required)
                keygen: lbase + '/lib/keygen/storage-hourly.js',

                // additional keygen arguments
                keygenArgs: {
                        // where to find source keys
                        source: '/poseidon/stor/manatee_backups'
                },

                // job manifest (required)
                job: {
                        name: 'metering-storage-hourly-$year-$month-$day-$hour',
                        phases: [ {
                                type : 'storage-map',
                                assets : [
                                        md + '/bin/storage-map',
                                        md + '/cfg/config.sh',
                                        md + '/lib/carrier.js',
                                        md + '/lib/storage-map.js'
                                ],
                                exec : '/assets' + md + '/bin/storage-map'
                        }, {
                                type: 'reduce',
                                assets : [
                                        md + '/bin/storage-reduce1',
                                        md + '/cfg/config.sh',
                                        md + '/lib/carrier.js',
                                        md + '/lib/storage-reduce1.js'
                                ],
                                exec: '/assets' + md + '/bin/storage-reduce1',
                                count: 2
                        }, {
                                type: 'reduce',
                                assets : [
                                        md + '/bin/storage-reduce2',
                                        md + '/cfg/config.sh',
                                        md + '/lib/carrier.js',
                                        md + '/lib/sum-columns.js'
                                ],
                                exec: '/assets' + md + '/bin/storage-reduce2',
                                count: 2
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/storage-reduce3',
                                        md + '/cfg/config.sh',
                                        md + '/lib/carrier.js',
                                        md + '/lib/storage-reduce3.js'
                                ],
                                exec: '/assets' + md + '/bin/storage-reduce3',
                                count: 2
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/config.sh',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },

                // manta destination path for link to latest report
                linkPath: mbase + '/storage/latest-hourly',
                // -- job specific environment variables
                // manta destination path (for final mpipe)
                DEST: mbase + '/storage/$year/$month/$day/$hour/h$hour.json',
                // manta destination path for user reports
                // prepend user login
                USER_DEST: userbase + '/storage/$year/$month/$day/$hour/h$hour.json'
                // manta destination path for link to latest report (for users)
        },
        daily: {
                keygen: lbase + '/lib/keygen/findOneLevel.js',
                keygenArgs: {
                        source: '/poseidon/stor/usage/storage/$year/$month/$day',
                        regex: 'h[0-9][0-9]'
                },
                job: {
                        name: 'metering-storage-daily-$year-$month-$day',
                        phases: [ {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/avg-columns',
                                        md + '/cfg/config.sh',
                                        md + '/lib/avg-columns.js',
                                        md + '/lib/carrier.js'
                                ],
                                exec: '/assets' + md + '/bin/avg-columns',
                                count: 1
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/config.sh',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                linkPath: mbase + '/storage/latest-daily',
                DEST: mbase + '/storage/$year/$month/$day/d$day.json',
                USER_DEST: userbase + '/storage/$year/$month/$day/d$day.json'
        },
        monthly: {
                keygen: lbase + '/lib/keygen/findOneLevel.js',
                keygenArgs: {
                        source: '/poseidon/stor/usage/storage/$year/$month',
                        regex: 'd[0-9][0-9]'
                },
                job: {
                        name: 'metering-storage-daily-$year-$month',
                        phases: [ {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/avg-columns',
                                        md + '/cfg/config.sh',
                                        md + '/lib/avg-columns.js',
                                        md + '/lib/carrier.js'
                                ],
                                exec: '/assets' + md + '/bin/avg-columns',
                                count: 1
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/config.sh',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                linkPath: mbase + '/storage/latest-monthly',
                DEST: mbase + '/storage/$year/$month/m$month.json',
                USER_DEST: userbase + '/storage/$year/$month/m$month.json'
        }
};
c.jobs.request = {
        hourly: {
                keygen: lbase + '/lib/keygen/findOneLevel.js',
                keygenArgs: {
                        source: '/poseidon/stor/logs/muskie/$year/$month/$day/$hour'
                },
                job: {
                        name: 'metering-request-hourly-$year-$month-$day-$hour',
                        phases: [ {
                                type: 'storage-map',
                                assets: [
                                        md + '/bin/request-map',
                                        md + '/cfg/config.sh'
                                ],
                                exec: '/assets' + md + '/bin/request-map'
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/request-reduce',
                                        md + '/cfg/config.sh'
                                ],
                                exec: '/assets' + md + '/bin/request-reduce',
                                count: 2
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/config.sh',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                linkPath: mbase + '/request/latest-hourly',
                DEST: mbase + '/request/$year/$month/$day/$hour/h$hour.json',
                USER_DEST: userbase + '/request/$year/$month/$day/$hour/h$hour.json'
        },
        daily: {
                keygen: lbase + '/lib/keygen/findOneLevel.js',
                keygenArgs: {
                        source: '/poseidon/stor/usage/request/$year/$month/$day',
                        regex: 'h[0-9][0-9]'
                },
                job: {
                        name: 'metering-request-daily-$year-$month-$day',
                        phases: [ {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/sum-columns',
                                        md + '/cfg/config.sh',
                                        md + '/lib/carrier.js',
                                        md + '/lib/sum-columns.js'
                                ],
                                exec: '/assets' + md + '/bin/sum-columns',
                                count: 1
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/config.sh',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                linkPath: mbase + '/request/latest-daily',
                DEST: mbase + '/request/$year/$month/$day/d$day.json',
                USER_DEST: userbase + '/request/$year/$month/$day/d$day.json'
        },
        monthly: {
                keygen: lbase + '/lib/keygen/findOneLevel.js',
                keygenArgs: {
                        source: '/poseidon/stor/usage/request/$year/$month',
                        regex: 'd[0-9][0-9]'
                },
                job: {
                        name: 'metering-request-daily-$year-$month',
                        phases: [ {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/sum-columns',
                                        md + '/cfg/config.sh',
                                        md + '/lib/sum-columns.js',
                                        md + '/lib/carrier.js'
                                ],
                                exec: '/assets' + md + '/bin/sum-columns',
                                count: 1
                        }, {
                                type: 'reduce',
                                assets: [
                                        md + '/bin/deliver-usage',
                                        md + '/cfg/config.sh',
                                        md + '/cfg/lookup.json',
                                        md + '/lib/carrier.js',
                                        md + '/lib/deliver-usage.js',
                                        md + '/lib/memorystream.js'
                                ],
                                exec: '/assets' + md + '/bin/deliver-usage',
                                count: 1 // final reduce phases must have exactly one reducer to collate results
                        } ]
                },
                linkPath: mbase + '/request/latest-monthly',
                DEST: mbase + '/request/$year/$month/m$month.json',
                USER_DEST: userbase + '/request/$year/$month/m$month.json'
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


/**** Custom config settings needed for metering ****/

// things to include in the config.sh asset that is generated each metering run
c.job_env = {
        ZCAT: 'gzcat',
        HEADER_CONTENT_TYPE: 'application/x-json-stream',
        NAMESPACES: 'stor public jobs reports',
        STORAGE_NUM_REDUCERS1_HOURLY: c.jobs.storage.hourly.job.phases[1].count,
        STORAGE_NUM_REDUCERS2_HOURLY: c.jobs.storage.hourly.job.phases[2].count,
        STORAGE_NUM_REDUCERS3_HOURLY: c.jobs.storage.hourly.job.phases[3].count,
        STORAGE_NUM_REDUCERS_DAILY: c.jobs.storage.daily.job.phases[0].count,
        STORAGE_NUM_REDUCERS_MONTHLY: c.jobs.storage.monthly.job.phases[0].count,
        REQUEST_NUM_REDUCERS_HOURLY: c.jobs.request.hourly.job.phases[1].count,
        REQUEST_NUM_REDUCERS_DAILY: c.jobs.request.daily.job.phases[0].count,
        REQUEST_NUM_REDUCERS_MONTHLY: c.jobs.request.monthly.job.phases[0].count
};

// redis config
c.redis = {
        port: 6379,
        hostname: 'auth.beta.joyent.us',
        clientOpts: undefined
}


// if run directly, prints out config as JSON
if (require.main === module) {
        console.log(JSON.stringify(c));
}

module.exports = c;
