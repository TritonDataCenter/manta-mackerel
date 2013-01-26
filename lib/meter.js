#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_assert = require('assert-plus');
var mod_backoff = require('backoff');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_getopt = require('posix-getopt');
var mod_jobrunner = require('./jobrunner');
var mod_keygen; // retrived once job config is loaded
var mod_manta = require('manta');
var mod_path = require('path');
var mod_redis = require('./redis');
var mod_vasync = require('vasync');

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
var MANTA_CONFIG_PATH = '/poseidon/stor/usage/assets/cfg/config.sh';
var MANTA_LOOKUP_PATH = '/poseidon/stor/usage/assets/cfg/lookup.json';


var usageMsg = [
'Usage:',
'meter -p period -s service -d date [-c configPath] [-e endDate]'
].join('\n');


function usage(msg) {
        console.error(msg);
        console.error(usageMsg);
        if (msg) {
                process.exit(1);
        } else {
                process.exit(0);
        }
}


function ifError(err) {
        if (err) {
                LOG.fatal('Error', err);
                process.exit(1);
        }
}


function getOpts(argv) {
        var opts = {};
        var usageErr = '';
        var parser = new mod_getopt.BasicParser('d:p:s:c:e:', argv);
        var option, period, service, temp;

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
                case 'e':
                        if (isNaN(Date.parse(option.optarg))) {
                                usage('invalid date: ' + option.optarg);
                        }
                        opts.endDate = new Date(Date.parse(option.optarg));
                        break;
                case 'p':
                        period = option.optarg.toUpperCase();
                        if (!PERIODS[period]) {
                                usage('invalid period');
                        }
                        opts.period = PERIODS[period];
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

        // swap start and end date if start date is later than end date
        if (opts.endDate && opts.date.getTime() > opts.endDate.getTime()) {
                temp = opts.date;
                opts.date = opts.endDate;
                opts.endDate = temp;
        }


        return (opts);
}


/*
 * Returns a string with any occurrences of '$year', '$month' '$day' and
 * '$hour' in str with their respective strings replaced ('2013' for '$year',
 * '01' for '$month' etc.).
 *
 * - str: source string
 * - date: javascript Date object
 */
function dateStringFormat(str, date) {
        mod_assert.string(str, 'str');
        mod_assert.object(date, 'date');

        // date methods return numbers not strings, so pad if needed
        function pad(num) { return (num < 10 ? '0' + num : num); }

        var year = date.getUTCFullYear();
        var month = pad(date.getUTCMonth() + 1); // Months start at 0, so add 1
        var day = pad(date.getUTCDate());
        var hour = pad(date.getUTCHours());

        var result = str.replace(/\$year/g, year);
        result = result.replace(/\$month/g, month);
        result = result.replace(/\$day/g, day);
        result = result.replace(/\$hour/g, hour);

        return (result);
}


/*
 * Generates the lookup table that maps uuid->login via redis.
 *
 * - opts: redis client config
 * - cb: callback in the form f(result), where result is a javascript object
 *   that maps uuid -> login.
 */
function generateLookup(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.number(opts.port, 'opts.port');
        mod_assert.string(opts.host, 'opts.host');
        mod_assert.optionalObject(opts.options, 'opts.options');
        mod_assert.optionalNumber(opts.maxParallel, 'opts.maxParallel');
        mod_assert.func(cb, 'cb');

        var result = {};
        var queue = mod_vasync.queue(getLogin, opts.maxParallel || 10);
        var client;

        function getLogin(uuid, callback) {
                client.get('/uuid/' + uuid, function onGet(err, login) {
                        ifError(err);
                        result[uuid] = login;
                        callback();
                });
        }

        // called every time a getLogin finishes
        function finishedGet() {
                if (queue.queued.length === 0 && queue.npending === 0) {
                        client.quit();
                        cb(null, result);
                }
        }


        mod_redis.createClient({
                host: opts.host,
                log: LOG,
                options: opts.options,
                port: opts.port,
                connectTimeout: opts.connectTimeout,
                retries: opts.retries,
                minTimeout: opts.minTimeout,
                maxTimeout: opts.maxTimeout
        }, function onClient(err, c) {
                ifError(err);

                client.on('error', ifError.bind(null));

                client.smembers('uuid', function onSmembers(err2, res) {
                        ifError(err2);
                        queue.push(res, finishedGet);
                });
        });


}


/*
 * Configures the job manifest to include any environment variables and
 * date string formatting.
 *
 * - jobConfig: job config
 * - date: javascript Date object
 */
function getJobManifest(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.jobConfig, 'opts.jobConfig');
        mod_assert.object(opts.date, 'opts.date');

        var jobConfig = opts.jobConfig;
        var date = opts.date;

        var jobManifest = jobConfig.job;
        var envString = '';
        var p, prepend, exec, numReducers;

        // insert date into any environment variables that need it (e.g.
        // destination path: /user/stor/usage/2013/01/01) and create the string
        // of environment variables to prepend to the exec string in the form
        // 'VAR0="value0" VAR1="value1" ... VARN="valuen"'
        Object.keys(jobConfig.env).forEach(function (k) {
                jobConfig.env[k] = dateStringFormat(jobConfig.env[k], date);
                envString += k + '="' + jobConfig.env[k] + '" ';
        });

        for (p = 0; p < jobManifest.phases.length; p++) {
                prepend = envString;
                exec = jobManifest.phases[p].exec;

                // check if the next phase is a reduce phase and make available
                // the reducer count in the previous phase (for msplit)
                if (p + 1 < jobManifest.phases.length &&
                        jobManifest.phases[p + 1].type === 'reduce') {

                        numReducers = jobManifest.phases[p + 1].count || 1;
                        prepend += 'NUM_REDUCERS=' + numReducers + ' ';
                }
                jobManifest.phases[p].exec = prepend + exec;
        }

        jobManifest.name = dateStringFormat(jobManifest.name, date);
        LOG.info('Job manifest ' +  JSON.stringify(jobManifest));
        return (jobManifest);
}


/*
 * Configures the job input key generator to find the correct input keys given
 * the date and source path.
 *
 * - args: any additional arguments that should be passed to the key generator
 *   besides the manta client and a logger. Includes date and source path
 * - client: manta client
 * - date: javascript Date object
 */
function getKeygen(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.args, 'opts.args');
        mod_assert.object(opts.client, 'opts.client');
        mod_assert.object(opts.date, 'opts.date');

        var args = opts.args;
        var client = opts.client;
        var date = opts.date;

        args.date = date;
        args.source = dateStringFormat(args.source, date);
        var keygen = mod_keygen.keygen({
                client: client,
                log: LOG,
                args: args
        });

        mod_assert.func(keygen.start, 'Keygen must have a start method.');

        return (keygen);
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


function assertConfig(config) {
        mod_assert.object(config, 'config');
        mod_assert.string(config.mantaConfigFile, 'config.mantaConfigFile');
        mod_assert.object(config.assets, 'config.assets');
        mod_assert.object(config.backoff, 'config.backoff');
        mod_assert.object(config.jobs, 'config.jobs');
        mod_assert.object(config.redis, 'config.redis');
}


function assertJobConfig(jobConfig) {
        mod_assert.object(jobConfig, 'jobConfig');
        mod_assert.string(jobConfig.keygen, 'jobConfig.keygen');
        mod_assert.optionalObject(jobConfig.keygenArgs, 'jobConfig.keygenArgs');
        mod_assert.optionalString(jobConfig.linkPath, 'jobConfig.linkPath');
        mod_assert.object(jobConfig.job, 'jobConfig.job');
        mod_assert.optionalString(jobConfig.job.name, 'jobConfig.job.name');
        mod_assert.arrayOfObject(jobConfig.job.phases, 'jobConfig.job.phases');
        mod_assert.ok(jobConfig.job.phases.length > 0);
        mod_assert.optionalObject(jobConfig.env, 'jobConfig.env');
}

// runs the job
function runJob(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.runner, 'opts.runner');
        mod_assert.object(opts.jobManifest, 'opts.jobManifest');
        mod_assert.object(opts.keygen, 'opts.keygen');

        var runner = opts.runner;
        var jobManifest = opts.jobManifest;
        var keygen = opts.keygen;

        runner.doJob(jobManifest, keygen, function (err, res) {
                ifError(err);

                var isFailure = res.failures.length > 0 ||
                        res.errors.length > 0;
                var noOutput = res.outputs.length === 0;
                var result = {outputs: res.outputs, jobPath: res.jobPath};
                var errors;

                if (isFailure) {
                        LOG.error('Failures or errors for job ' + res.jobPath);
                        errors = {errors: res.errors, failures: res.failures};
                } else if (noOutput) {
                        LOG.error('No output for job ' + res.jobPath);
                        errors = {errors: res.errors, failures: res.failures};
                }

                cb(errors, result);
        });
}

function main() {
        var opts = getOpts(process.argv);

        var config = require(opts.config);
        assertConfig(config);
        var mantaConfig = config.mantaConfigFile;

        // create the job runner
        var client = mod_manta.createClientFromFileSync(mantaConfig, LOG);
        var runner = new mod_jobrunner.JobRunner({
                assets: config.assets,
                client: client,
                log: LOG,
                backoffStrategy: config.monitorBackoff
        });

        // the json blob for the service & period
        var jobConfig = config.jobs[opts.service][opts.period];
        assertJobConfig(jobConfig);

        // get the keygen module for this job
        mod_keygen = require(jobConfig.keygen);

        var dates = getDateRange({
                startDate: opts.date,
                endDate: opts.endDate,
                period: opts.period
        });

        function runJobs() {
                var d, date, jobManifest, keygen;
                for (d = 0; d < dates.length; d++) {
                        date = dates[d];
                        // the job manifest passed to marlin
                        jobManifest = getJobManifest({
                                jobConfig: jobConfig,
                                date: date
                        });

                        // generates job input keys
                        keygen = getKeygen({
                                args: jobConfig.keygenArgs,
                                client: client,
                                date: date
                        });

                        runJobWithRetry(jobManifest, keygen, function (err) {
                        });
                }
        }

        function runJobWithRetry(jobManifest, keygen, cb) {
                var retry = mod_backoff.call(runJob, {
                        runner: runner,
                        jobManifest: jobManifest,
                        keygen: keygen
                }, function (err, res) {
                        if (err) {
                                LOG.fatal('Retry limit exceeded.');
                                process.exit(1);
                        }
                        LOG.info('Job successful');
                        linkToLatest(res.outputs);
                });

                retry.setStrategy(new mod_backoff.ExponentialStrategy({
                        initialDelay: config.backoff.initialDelay,
                        maxDelay: config.backoff.maxDelay
                }));

                retry.failAfter(config.backoff.failAfter);

                retry.on('backoff', function (attempts, delayms) {
                        LOG.info('Job attempt ' + attempts + '. Retry in ' +
                                delayms + 'ms.');
                });
        }


        // makes a link to the latest job result
        function linkToLatest(outputs) {
                client.ln(outputs[0], jobConfig.linkPath, function (err) {
                        ifError(err);
                        LOG.info('Link created ' + jobConfig.linkPath + ' -> ' +
                                outputs[0]);
                });
        }

        generateLookup(config.redis, function (err, res) {
                ifError(err);
                LOG.info(res);
                mod_fs.writeFileSync(config.assets[MANTA_LOOKUP_PATH],
                        JSON.stringify(res));
                runJobs();
        });

}

if (require.main === module) {
        main();
}
