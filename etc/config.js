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

var path = require('path');
var c = {};
module.exports = c;

c.manta = require('./config.json').manta;
c.mahi = require('./config.json').mahi;
c.workflow = require('./config.json').workflow;
c.monitorBackoff = require('./config.json').monitorBackoff;

/******************************/
/*   LOCAL HELPER VARIABLES   */
/******************************/

var user, mbase, md, lbase, ld, userbase;

user = c.manta.user;

/*
 * Modify these variables to match your environment.
 */

mbase = '/' + user + '/stor/usage'; // manta base directory
md = mbase + '/assets'; // manta assets directory
userbase = '/reports/usage'; // user-accessible base directory

lbase = path.resolve(__dirname, '..'); // local base directory
ld = lbase + '/assets'; // local assets directory

c.mantaBaseDir = mbase;
c.localBaseDir = lbase;

/******************************/
/*           ASSETS           */
/******************************/

// assets is a mapping from the manta object path to the local file path
c.assets = {};
c.assets[md + '/bin/request-map'] = ld + '/bin/request-map';
c.assets[md + '/bin/storage-map'] = ld + '/bin/storage-map';
c.assets[md + '/bin/storage-reduce1'] = ld + '/bin/storage-reduce1';
c.assets[md + '/bin/storage-reduce3'] = ld + '/bin/storage-reduce3';
c.assets[md + '/bin/sum-columns'] = ld + '/bin/sum-columns';
c.assets[md + '/lib/compute-map.js'] = ld + '/lib/compute-map.js';
c.assets[md + '/bin/compute-map'] = ld + '/bin/compute-map';
c.assets[md + '/lib/deliver-access.js'] = ld + '/lib/deliver-access.js';
c.assets[md + '/lib/deliver-usage.js'] = ld + '/lib/deliver-usage.js';
c.assets[md + '/bin/deliver-usage'] = ld + '/bin/deliver-usage';
c.assets[md + '/lib/request-map.js'] = ld + '/lib/request-map.js';
c.assets[md + '/lib/storage-map.js'] = ld + '/lib/storage-map.js';
c.assets[md + '/lib/storage-reduce1.js'] = ld + '/lib/storage-reduce1.js';
c.assets[md + '/lib/storage-reduce3.js'] = ld + '/lib/storage-reduce3.js';
c.assets[md + '/lib/sum-columns.js'] = ld + '/lib/sum-columns.js';
c.assets[md + '/node_modules.tar'] = ld +'/node_modules.tar';
// since the lookup file is generated at job run time, meter.js needs to know
// the manta path for the lookup file
c.mantaLookupPath = md + '/etc/lookup.json';
c.assets[c.mantaLookupPath] = ld + '/etc/lookup.json';


/******************************/
/*      JOB CONFIGURATION     */
/******************************/

// job configuration
// each job object must have a 'keygen' field pointing to the path of the
// keygen file, and a 'job' field containing the job manifest.
c.jobs = {};

c.jobs.storage = {

    /******************/
    /* STORAGE HOURLY */
    /******************/

    hourly: {
        // keygen name (required)
        keygen: 'StorageHourlyKeyGenerator',

        // additional keygen arguments (date will always be passed in)
        keygenArgs: {
            // where to find source keys
            source: '/poseidon/stor/manatee_backups'
        },

        // name of the workflow. depends on which key generator is
        // used for this job
        workflow: 'runjob',

        // manta destination path for link to latest report
        linkPath: mbase + '/storage/latest-hourly',

        // job manifest (required)
        job: {
            name: 'metering-storage-hourly-$year-$month-$dayT$hour',
            phases: [ {
                type : 'map',
                assets : [
                    md + '/node_modules.tar',
                    md + '/bin/storage-map',
                    md + '/lib/storage-map.js'
                ],
                exec : '/assets' + md + '/bin/storage-map'
            }, {
                type: 'reduce',
                memory: 2048,
                assets : [
                    md + '/node_modules.tar',
                    md + '/bin/storage-reduce1',
                    md + '/lib/storage-reduce1.js'
                ],
                exec: '/assets' + md + '/bin/storage-reduce1',
                count: 1
            }, {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js',
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1
            }, {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/storage-reduce3',
                    md + '/lib/storage-reduce3.js'
                ],
                exec: '/assets' + md + '/bin/storage-reduce3',
                count: 1
            }, {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/deliver-usage',
                    md + '/etc/lookup.json',
                    md + '/lib/deliver-usage.js',
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

    /*****************/
    /* STORAGE DAILY */
    /*****************/

    daily: {
        keygen: 'FindKeyGenerator',
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
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js',
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1 // final reduce phases must have exactly one reducer to collate results
            } ]
        }
    },

    /*******************/
    /* STORAGE MONTHLY */
    /*******************/

    monthly: {
        keygen: 'FindKeyGenerator',
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
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js',
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1 // final reduce phases must have exactly one reducer to collate results
            } ]
        }
    }
};

c.jobs.request = {

    /******************/
    /* REQUEST HOURLY */
    /******************/

    hourly: {
        keygen: 'FindKeyGenerator',
        keygenArgs: {
            source: '/poseidon/stor/logs/muskie/$year/$month/$day/$hour'
        },
        workflow: 'find-runjob',
        linkPath: mbase + '/request/latest-hourly',
        job: {
            name: 'metering-request-hourly-$year-$month-$dayT$hour',
            phases: [ {
                type: 'map',
                assets: [
                    md + '/etc/lookup.json',
                    md + '/node_modules.tar',
                    md + '/bin/request-map',
                    md + '/lib/deliver-access.js',
                    md + '/lib/request-map.js'
                ],
                exec: '/assets' + md + '/bin/request-map'
            }, {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js',
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1
            }, {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/deliver-usage',
                    md + '/etc/lookup.json',
                    md + '/lib/deliver-usage.js',
                ],
                exec: '/assets' + md + '/bin/deliver-usage',
                count: 1 // final reduce phases must have exactly one reducer to collate results
            } ]
        },
        env: {
            HEADER_CONTENT_TYPE: 'application/x-json-stream',
            DEST: mbase + '/request/$year/$month/$day/$hour/h$hour.json',
            // location for customer-accessible, sanitized muskie access logs
            ACCESS_DEST: '/reports/access-logs/$year/$month/$day/$hour/h$hour.json',
            USER_DEST: userbase + '/request/$year/$month/$day/$hour/h$hour.json'
        }
    },

    /*****************/
    /* REQUEST DAILY */
    /*****************/

    daily: {
        keygen: 'FindKeyGenerator',
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
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js'
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1
            } ]
        }
    },

    /*******************/
    /* REQUEST MONTHLY */
    /*******************/

    monthly: {
        keygen: 'FindKeyGenerator',
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
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js',
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1
            } ]
        }
    }
};

c.jobs.compute = {

    /******************/
    /* COMPUTE HOURLY */
    /******************/

    hourly: {
        keygen: 'FindKeyGenerator',
        keygenArgs: {
            source: '/poseidon/stor/logs/marlin-agent/$year/$month/$day/$hour'
        },
        workflow: 'find-runjob',
        linkPath: mbase + '/compute/latest-hourly',
        job: {
            name: 'metering-compute-hourly-$year-$month-$dayT$hour',
            phases: [ {
                type: 'map',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/compute-map',
                    md + '/lib/compute-map.js'
                ],
                exec: '/assets' + md + '/bin/compute-map'
            }, {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js',
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1
            }, {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/deliver-usage',
                    md + '/etc/lookup.json',
                    md + '/lib/deliver-usage.js',
                ],
                exec: '/assets' + md + '/bin/deliver-usage',
                count: 1 // final reduce phases must have exactly one reducer to collate results
            } ]
        },
        env: {
            HEADER_CONTENT_TYPE: 'application/x-json-stream',
            DEST: mbase + '/compute/$year/$month/$day/$hour/h$hour.json',
            USER_DEST: userbase + '/compute/$year/$month/$day/$hour/h$hour.json'
        }
    },

    /*****************/
    /* COMPUTE DAILY */
    /*****************/

    daily: {
        keygen: 'FindKeyGenerator',
        keygenArgs: {
            source: '/poseidon/stor/usage/compute/$year/$month/$day',
            regex: 'h[0-9][0-9]'
        },
        workflow: 'find-runjob',
        linkPath: mbase + '/compute/latest-daily',
        job: {
            name: 'metering-compute-daily-$year-$month-$day',
            phases: [ {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js'
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1
            } ]
        }
    },

    /*******************/
    /* COMPUTE MONTHLY */
    /*******************/

    monthly: {
        keygen: 'FindKeyGenerator',
        keygenArgs: {
            source: '/poseidon/stor/usage/compute/$year/$month',
            regex: 'd[0-9][0-9]'
        },
        workflow: 'find-runjob',
        linkPath: mbase + '/compute/latest-monthly',
        job: {
            name: 'metering-compute-monthly-$year-$month',
            phases: [ {
                type: 'reduce',
                assets: [
                    md + '/node_modules.tar',
                    md + '/bin/sum-columns',
                    md + '/lib/sum-columns.js',
                ],
                exec: '/assets' + md + '/bin/sum-columns',
                count: 1
            } ]
        }
    }
}


if (require.main === module) {
    console.log(JSON.stringify(c, null, 2));
}

