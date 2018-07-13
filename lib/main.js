#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var mod_meter = require('./meter');
var mod_path = require('path');
var mod_generateLookup = require('./generateLookup');

var usageMsg = [
'Usage:',
'meter -j jobName -d date [-r] [-w] [-c] [-f configFile]'
].join('\n');

var LOG = mod_bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'mackerel',
        stream: process.stderr,
        serializers: mod_bunyan.stdSerializers
});

var DEFAULT_CONFIG = mod_path.resolve(__dirname, '../etc/config.json');

function ifError(err) {
        if (err) {
                LOG.fatal('Error', err);
                process.exit(1);
        }
}


function usage(msg) {
        console.error(msg);
        console.error(usageMsg);
        if (msg) {
                process.exit(1);
        } else {
                process.exit(0);
        }
}


function getOpts(argv) {
        var opts = {};
        var usageErr = '';
        var parser = new mod_getopt.BasicParser('crwd:f:j:', argv);
        var option;

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'c':
                        opts.configOnly = true;
                        break;
                case 'r':
                        opts.retry = true;
                        break;
                case 'w':
                        opts.workflow = true;
                        break;
                case 'd':
                        if (isNaN(Date.parse(option.optarg))) {
                                usage('invalid date: ' + option.optarg);
                        }
                        opts.date = new Date(Date.parse(option.optarg));
                        break;
                case 'f':
                        opts.config = option.optarg;
                        break;
                case 'j':
                        opts.jobName = option.optarg;
                        break;
                default:
                        /* error message already emitted by getopt */
                        usage('unrecognized option ' + option.option);
                        break;
                }
        }

        opts.config = opts.config || DEFAULT_CONFIG;
        opts.config = mod_path.resolve(opts.config);

        if (!opts.date) {
                usageErr += 'Date is required (-d <date>)\n';
        }

        if (!opts.jobName) {
                usageErr += 'job name is required (-j <job name>)\n';
        }

        if (usageErr !== '') {
                usage(usageErr);
        }

        return (opts);
}


function main() {
        var opts = getOpts(process.argv);
        var config = require(opts.config);

        var jobs = require(mod_path.resolve(__dirname, '..', config.jobsFile));
        var lookupFile = mod_path.resolve(__dirname, '..', config.lookupFile);
        var jobConfig = mod_jsprim.pluck(jobs.jobs, opts.jobName);
        if (!jobConfig) {
                console.warn('job ' + opts.jobName + ' not found');
                process.exit(1);
        }

        if (config.disableAllJobs === true) {
                console.log('all jobs are disabled, exiting.');
                process.exit(0);
        }

        /* check if this job is enabled or not. */
        var isEnabled = true;
        switch (opts.jobName) {
                case 'storage':
                        isEnabled = (config.meterStorageEnabled !== false);
                        break;
                case 'compute':
                        isEnabled = (config.meterComputeEnabled !== false);
                        break;
                case 'request':
                case 'accessLogs':
                        isEnabled = (config.meterRequestEnabled !== false);
                        break;
                case 'summarizeDaily':
                        isEnabled = (config.meterDailyEnabled !== false);
                        break;
                default:
                        console.log('unknown job name');
                        break;
        }

        /* exit if job is not enabled. */
        if (isEnabled === false) {
                console.log(opts.jobName, 'job is disabled, exiting.');
                process.exit(0);
        }


        if (opts.configOnly) {
                mod_meter.configureJob({
                        jobConfig: jobConfig,
                        mantaDir: config.mantaBaseDirectory,
                        date: opts.date
                });
                console.log(JSON.stringify(jobConfig, null, 2));
                return;
        }

        config.mahi.log = LOG;
        mod_generateLookup(config.mahi, function (err, result) {
                ifError(err);
                LOG.debug(result);
                try {
                        mod_fs.mkdirSync(mod_path.dirname(lookupFile));
                } catch (e) {
                        if (e.code !== 'EEXIST') {
                                console.log(e.message);
                                process.exit(1);
                        }
                }
                mod_fs.writeFileSync(lookupFile, JSON.stringify(result));
                if (opts.workflow) {
                        mod_meter.createJob({
                                date: opts.date,
                                jobConfig: jobConfig,
                                config: config,
                                log: LOG
                        }, function (err2, res) {
                                LOG.debug({err: err2, res: res});
                        });
                } else {
                        mod_meter.meter({
                                date: opts.date,
                                jobConfig: jobConfig,
                                config: config,
                                retry: opts.retry,
                                log: LOG
                        }, function (err2, res) {
                                LOG.info({err: err2, res: res});
                        });
                }
        });
}

if (require.main === module) {
        main();
}
