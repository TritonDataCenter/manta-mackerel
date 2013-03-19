#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_getopt = require('posix-getopt');
var mod_meter = require('./meter');
var mod_path = require('path');

// TODO var helpMsg =
var usageMsg = [
'Usage:',
'meter -p period -s service -d date [-w] [-c configPath]'
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
        var parser = new mod_getopt.BasicParser('d:p:s:c:w', argv);
        var option, period, service;
        // TODO -h help

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
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
                case 'w':
                        opts.workflow = true;
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
                        usage('unrecognized option ' + option.option);
                        break;
                }
        }

        opts.config = opts.config || DEFAULT_CONFIG;
        opts.config = mod_path.resolve(opts.config);

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

        dates.push(startDate);
        if (!endDate) {
                return (dates);
        }
        endTime = endDate.getTime();

        t = startDate.getTime() + msBetween;
        for (; t <= endTime; t += msBetween) {
                dates.push(new Date(t));
        }

        return (dates);
}


function main() {
        var opts = getOpts(process.argv);
        var config = require(opts.config);
        mod_meter.generateLookup(config.redis, LOG, function (err, result) {
                ifError(err);
                LOG.debug(result);
                mod_fs.writeFileSync(config.assets[config.mantaLookupPath],
                        JSON.stringify(result));
                if (opts.workflow) {
                        mod_meter.createJob({
                                date: opts.date,
                                service: opts.service,
                                period: opts.period,
                                config: config,
                                log: LOG
                        }, function (err, res) {
                                LOG.debug({err: err, res: res});
                        });
                } else {
                        mod_meter.meter({
                                date: opts.date,
                                service: opts.service,
                                period: opts.period,
                                config: config,
                                log: LOG
                        }, function (err, res) {
                                LOG.info({err: err, res: res});
                        });
                }
        });
}

if (require.main === module) {
        main();
}
