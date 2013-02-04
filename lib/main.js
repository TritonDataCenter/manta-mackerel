#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_getopt = require('posix-getopt');
var mod_jobrunner = require('./jobrunner');
var mod_manta = require('manta');
var mod_meter = require('./meter');
var mod_path = require('path');
var mod_vasync = require('vasync');

// TODO var helpMsg =
var usageMsg = [
'Usage:',
'meter -p period -s service -d date [-r] [-c configPath]',
'meter -b [-c configPath]'
].join('\n');

var LOG = mod_bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'mackerel',
        stream: process.stderr
});

var PERIODS = {
        HOURLY:  'hourly',
        DAILY:   'daily',
        MONTHLY: 'monthly'
};

var SERVICES = {
        STORAGE: 'storage',
        REQUEST: 'request'
//        COMPUTE: 'compute' // XXX not implemented yet
};

var DEFAULT_CONFIG = '../etc/config.js';


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
        var parser = new mod_getopt.BasicParser('d:p:s:c:br', argv);
        var option, period, service;
        // TODO -h help

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'b':
                        opts.backfill = true;
                        break;
                case 'c':
                        opts.config = option.optarg;
                        break;
                case 'd':
                        if (isNaN(Date.parse(option.optarg))) {
                                usage('invalid date: ' + option.optarg);
                        }
                        opts.date = new Date(Date.parse(option.optarg));
                        break;
                case 'p':
                        period = option.optarg.toUpperCase();
                        if (!PERIODS[period]) {
                                usage('invalid period');
                        }
                        opts.period = PERIODS[period];
                        break;
                case 'r':
                        opts.record = true;
                        break;
                case 's':
                        service = option.optarg.toUpperCase();
                        if (!SERVICES[service]) {
                                usage('invalid service');
                        }
                        opts.service = SERVICES[service];
                        break;
                default:
                        /* error message already emitted by getopt */
                        usage('unrecognized option');
                        break;
                }
        }

        opts.config = opts.config || DEFAULT_CONFIG;
        opts.config = mod_path.resolve(__dirname, opts.config);

        if (opts.backfill) {
                if (opts.date || opts.period || opts.service || opts.record) {
                        usageErr = 'Invalid option(s) with option -b';
                        usage(usageErr);
                }
                return (opts);
        }

        if (!opts.date) {
                usageErr += 'Date is required (-d <date>)\n';
        }

        if (!opts.period) {
                usageErr += 'Period is required (-p <period>)\n';
        }

        if (!opts.service) {
                usageErr += 'Service is required (-s <service>)\n';
        }

        if (usageErr !== '') {
                usage(usageErr);
        }

        return (opts);
}


function assertConfig(config) {
        mod_assert.object(config, 'config');
        mod_assert.string(config.mantaConfigFile, 'config.mantaConfigFile');
        mod_assert.object(config.assets, 'config.assets');
        mod_assert.object(config.jobs, 'config.jobs');
        mod_assert.object(config.redis, 'config.redis');
}


/*
 * Returns an array of Dates that start from startDate and end at endDate
 * (inclusive), and includes dates representing each hour/day/month in between
 * the two.
 *
 * - startDate: javascript Date object
 * - endDate: javascript Date object
 * - period: one of 'hourly', 'daily', or 'monthly'
 */
function getDateRange(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.startDate, 'opts.startDate');
        mod_assert.optionalObject(opts.endDate, 'opts.endDate');
        mod_assert.string(opts.period, 'opts.period');

        var startDate = opts.startDate;
        var endDate = opts.endDate;
        var period = opts.period;

        var dates = [];
        var msBetween, t, endTime;

        dates.push(startDate);

        if (!endDate) {
                return (dates);
        }

        endTime = endDate.getTime();

        switch (period) {
        case PERIODS.HOURLY:
                msBetween = 3600000;
                break;
        case PERIODS.DAILY:
                msBetween = 86400000;
                break;
        case PERIODS.MONTHLY:
                msBetween = 2.62974e9;
                break;
        default:
                LOG.fatal('Unknown period in getDateRange: ' + period);
                process.exit(1);
                break;
        }


        t = startDate.getTime() + msBetween; // skip start date since it's
                                             // already been added (above)
        for (; t <= endTime; t += msBetween) {
                dates.push(new Date(t));
        }

        return (dates);
}





function main() {
        var opts = getOpts(process.argv);
        var config = require(opts.config);
        assertConfig(config);
        var mantaConfig = config.mantaConfigFile;
        var client = mod_manta.createClientFromFileSync(mantaConfig, LOG);
        var jobConfig, runner;

        function runJob() {
                mod_meter.meter({
                        date: opts.date,
                        jobConfig: jobConfig,
                        log: LOG,
                        client: client,
                        runner: runner
                }, function (err, res) {
                        if (opts.record) {
                                LOG.info('Recording result.');
                                mod_meter.recordResult({
                                        client: client,
                                        backfillPath: config.backfill.path,
                                        jobConfig: jobConfig,
                                        date: opts.date,
                                        results: res,
                                        errors: err,
                                        log: LOG
                                }, function onRecord(err2) {
                                        if (err2) {
                                                LOG.error(err2);
                                                return;
                                        }
                                });
                        }

                        if (err) {
                                LOG.error(err);
                                return;
                        }

                        client.ln(res.outputs[0], jobConfig.linkPath,
                                function onLink(err2) {

                                if (err2) {
                                        LOG.error(err2);
                                        return;
                                }

                                LOG.info('Link created ' +
                                        jobConfig.linkPath + ' -> ' +
                                        res.outputs[0]);
                        });
                });
        }

        runner = new mod_jobrunner.JobRunner({
                assets: config.assets,
                client: client,
                log: LOG,
                monitorBackoff: config.monitorBackoff,
                retryBackoff: config.retryBackoff
        });

        if (opts.backfill) {
                client.mkdirp(config.backfill.path, function onMkdirp(err) {
                        if (err) {
                                LOG.fatal(err);
                                return;
                        }
                        mod_meter.backfill({
                                client: client,
                                backfillPath: config.backfill.path,
                                alarmAfter: config.backfill.alarmAfter,
                                runner: runner,
                                log: LOG
                        }, function onBackfill(err2) {
                                if (err2) {
                                        // error printed by parent
                                        return;
                                }
                                LOG.info('Backfill complete.');
                        });
                        return;
                });
                return;
        }

        jobConfig = config.jobs[opts.service][opts.period];

        // the job manifest passed to marlin
        mod_meter.configureJobManifest({
                jobConfig: jobConfig,
                date: opts.date,
                log: LOG
        });

        mod_meter.generateLookup(config.redis, LOG,  function onLook(err, res) {
                ifError(err);
                LOG.info(res);
                mod_fs.writeFileSync(config.assets[config.mantaLookupPath],
                        JSON.stringify(res));
                client.mkdirp(config.backfill.path, function onMkdirp(err) {
                        if (err) {
                                LOG.fatal(err);
                                return;
                        }
                        runJob();
                });
        });
}


if (require.main === module) {
        main();
}
