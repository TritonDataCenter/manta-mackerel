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
'meter -p period -c category -d date [-w] [-f configFile]'
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

var CATEGORIES = {
        STORAGE: 'storage',
        REQUEST: 'request',
        COMPUTE: 'compute'
};

var DEFAULT_CONFIG = mod_path.resolve(__dirname, '../etc/config.js');

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
        var parser = new mod_getopt.BasicParser('wd:p:s:f:c:', argv);
        var option, period, category;
        // TODO -h help

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'f':
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
                case 'c':
                        category = option.optarg.toUpperCase();
                        if (!CATEGORIES[category]) {
                                usage('invalid category');
                        }
                        opts.category = CATEGORIES[category];
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

        if (!opts.category) {
                usageErr += 'Category is required (-c <category>)\n';
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
}


function main() {
        var opts = getOpts(process.argv);
        var config = require(opts.config);
        config.mahi.log = LOG;
        mod_meter.generateLookup(config.mahi, function (err, result) {
                ifError(err);
                LOG.debug(result);
                mod_fs.writeFileSync(config.assets[config.mantaLookupPath],
                        JSON.stringify(result));
                if (opts.workflow) {
                        mod_meter.createJob({
                                date: opts.date,
                                category: opts.category,
                                period: opts.period,
                                config: config,
                                log: LOG
                        }, function (err2, res) {
                                LOG.debug({err: err2, res: res});
                        });
                } else {
                        mod_meter.meter({
                                date: opts.date,
                                category: opts.category,
                                period: opts.period,
                                config: config,
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
