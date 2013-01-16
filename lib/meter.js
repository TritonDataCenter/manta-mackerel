#!/usr/bin/env node
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_getopt = require('posix-getopt');
var mod_jobrunner = require('./jobrunner');
var mod_manta = require('manta');
var mod_path = require('path');
var mod_redis = require('redis');
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

var DEFAULT_CONFIG = '../cfg/config.js';

function usage(msg) {
        console.error(msg);
        console.error('Usage: meter -p period -s service -d date' +
                ' [-c configPath]');
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
        var option;
        var parser = new mod_getopt.BasicParser('d:p:s:c:', argv);

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'c':
                        opts.config = option.optarg;
                        break;
                case 'd':
                        if (isNaN(Date.parse(option.optarg))) {
                                usage('invalid date: ' + option.optarg);
                        }
                        opts.date = new Date(option.optarg);
                        break;
                case 'p':
                        var period = option.optarg.toUpperCase();
                        if (!PERIODS[period]) {
                                usage('invalid period');
                        }
                        opts.period = PERIODS[period];
                        break;
                case 's':
                        var service = option.optarg.toUpperCase();
                        if (!SERVICES[service]) {
                                usage('invalid service');
                        }
                        opts.service = SERVICES[service];
                        break;
                default:
                        /* error message already emitted by getopt */
                        usage();
                        break;
                }
        }

        opts.config = opts.config || DEFAULT_CONFIG;

        var usageMsg = '';
        if (!opts.date) {
                usageMsg += 'Date is required (-d <date>)\n';
        }

        if (!opts.period) {
                usageMsg += 'Period is required (-p <period>)\n';
        }

        if (!opts.service) {
                usageMsg += 'Service is required (-s <service>)\n';
        }

        if (usageMsg !== '') {
                usage(usageMsg);
        }

        return (opts);
}


/*
 * replaces $year, $month, etc in str with the appropriate values
 */
function replaceWithDate(str, date) {

        // pad with leading zero if needed
        function pad(num) { return (num < 10 ? '0' + num : num); }

        var year = date.getFullYear();
        var month = pad(date.getMonth() + 1);
        var day = pad(date.getDate());
        var hour = pad(date.getHours());

        var result = str.replace(/\$year/g, year);
        result = result.replace(/\$month/g, month);
        result = result.replace(/\$day/g, day);
        result = result.replace(/\$hour/g, hour);

        return (result);
}


/*
 * generates the lookup table that maps uuid->login via redis
 */
function generateLookup(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.number(opts.port, 'opts.port');
        mod_assert.string(opts.hostname, 'opts.hostname');

        var client = mod_redis.createClient(opts.port, opts.hostname,
                opts.clientOpts);
        var result = {};

        function get(key, callback) {
                client.get(key, function (err, res) {
                        callback(null, res);
                });
        }

        client.on('error', function (err) {
                cb(err);
        });

        client.keys('/uuid/*', function (err, res) {
                if (err) {
                        cb(err); return;
                }

                mod_vasync.forEachParallel({
                        func: get,
                        inputs: res
                }, function (err2, results) {
                        if (err2) {
                                cb(err2); return;
                        }

                        for (var k = 0; k < res.length; k++) {
                                var uuid = res[k].split('/')[2];
                                var login = results.successes[k];
                                result[uuid] = login;
                        }

                        client.quit();
                        cb(null, result);
                });
        });

}


function checkConfig(config) {
        mod_assert.object(config, 'config');
        mod_assert.string(config.manta_config_file, 'config.manta_config_file');
        mod_assert.object(config.assets, 'config.assets');
        mod_assert.object(config.backoff, 'config.backoff');
        mod_assert.object(config.jobs, 'config.jobs');
        mod_assert.object(config.job_env, 'config.job_env');
        mod_assert.object(config.redis, 'config.redis');
        mod_assert.number(config.redis.port, 'config.redis.port');
        mod_assert.string(config.redis.hostname, 'config.redis.hostname');
}


function main() {
        var opts = getOpts(process.argv);

        var config = require(opts.config);
        checkConfig(config);
        var manta_config = config.manta_config_file;
        var client = mod_manta.createClientFromFileSync(manta_config, LOG);

        // create the job runner
        var runner = new mod_jobrunner.JobRunner({
                assets: config.assets,
                client: client,
                log: LOG,
                backoffStrategy: config.backoff
        });

        // the json blob for the service & period
        var jobConfig = config.jobs[opts.service][opts.period];


        // the marlin job manifest
        var jobManifest = jobConfig.job;
        jobManifest.name = replaceWithDate(jobManifest.name, opts.date);
        LOG.info('job manifest', JSON.stringify(jobManifest));


        // ----  key generator setup
        jobConfig.keygenArgs.date = opts.date;
        jobConfig.keygenArgs.source = replaceWithDate(
                jobConfig.keygenArgs.source, opts.date);
        var keygen = require(jobConfig.keygen).keygen({
                client: client,
                log: LOG,
                args: jobConfig.keygenArgs
        });


        // ---- some custom setup for date-sensitive job settings

        // generate the job config asset from which the shell scripts will pull
        // environment variables (e.g. date, zip program, destination paths)
        var configStr = '';

        // insert date into destination path (e.g. /user/stor/usage/2013/01/01)
        var dest = replaceWithDate(jobConfig.DEST, opts.date);
        var userDest = replaceWithDate(jobConfig.USER_DEST, opts.date);
        configStr += 'export DEST="' + dest + '"\n';
        configStr += 'export USER_DEST="' + userDest + '"\n';

        Object.keys(config.job_env).forEach(function (k) {
                configStr += 'export ' +  k + '="' + config.job_env[k] + '"\n';
        });

        var mantaConfigPath = '/poseidon/stor/usage/assets/cfg/config.sh';
        mod_fs.writeFileSync(config.assets[mantaConfigPath], configStr);



        // generate the lookup table
        var mantaLookupPath = '/poseidon/stor/usage/assets/cfg/lookup.json';
        function genLookup() {
                generateLookup(config.redis, function (err, res) {
                        ifError(err);
                        LOG.info(res);
                        mod_fs.writeFileSync(config.assets[mantaLookupPath],
                                JSON.stringify(res));
                        makedirp();
                });
        }

        // make any intermediate directories
        function makedirp() {
                client.mkdirp(mod_path.dirname(dest), function (err) {
                        ifError(err);
                        LOG.info('created directory ' + mod_path.dirname(dest));
                        runJob();
                });
        }

        // run the job
        function runJob() {
                runner.doJob(jobManifest, keygen, function (err, res) {
                        ifError(err);
                        LOG.info(res);
                        if (res.failures.length === 0 &&
                                res.errors.length === 0 &&
                                res.outputs.length > 0) {

                                linkToLatest(res.outputs);
                        } else if (res.failures.length > 0 ||
                                res.errors.length > 0) {

                                LOG.error('Failures or errors for job' +
                                        res.jobPath);
                        } else if (res.outputs.length === 0) {
                                LOG.error('No output for job ' + res.jobPath);
                        }
                });
        }

        // make a link to the latest job result
        function linkToLatest(outputs) {
                client.ln(outputs[0], jobConfig.linkPath, function (err) {
                        ifError(err);
                        LOG.info('link created ' + jobConfig.linkPath + ' -> ' +
                                outputs[0]);
                });
        }

        genLookup();

}


if (require.main === module) {
        main();
}
