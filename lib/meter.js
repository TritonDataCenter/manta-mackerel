// Copyright (c) 2013, Joyent, Inc. All rights reserved.


var mod_assert = require('assert-plus');
var mod_jobrunner = require('./jobrunner');
var mod_manta = require('manta');
var mod_redis = require('./redis');
var mod_vasync = require('vasync');
var mod_WFClient = require('wf-client');

module.exports = {
        meter: meter,
        createJob: createJob,
        generateLookup: generateLookup,
        retryFailedJobs: retryFailedJobs,
        shouldRun: shouldRun
};


//TODO move floordate to main? or only use when testing for retry?


function init() {
        // TODO create manta directories - ? don't think there are any to create
}

/*
 * config.jobConfig.job is modified
 * API: cb(err, outputs)
 * does not use wf
 * does not record result
 */
function meter(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.string(opts.service, 'opts.service');
        mod_assert.string(opts.period, 'opts.period');
        mod_assert.object(opts.config, 'opts.config');
        mod_assert.object(opts.log, 'opts.log');

        var date = floorDate(opts.date, opts.period);

        var jobConfig = opts.config.jobs[opts.service][opts.period];
        var mantaConfig = require(opts.config.mantaConfigFile).manta;
        mantaConfig.log = opts.log;
        var client = new mod_manta.createClient(mantaConfig);

        configureJob({
                jobConfig: jobConfig,
                date: date,
                log: opts.log
        });

        var keygen = require(jobConfig.keygen).keygen({
                client: client,
                log: opts.log,
                args: jobConfig.keygenArgs
        });

        function doJob() {
                mod_jobrunner.doJob({
                        assets: opts.config.assets,
                        jobManifest: jobConfig.job,
                        keygen: keygen,
                        log: opts.log,
                        client: client,
                }, function jobDone(err, results) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        opts.log.info('Done job');
                        cb();
                });
        }

        doJob();
}


/*
 * config.jobConfig.job is modified
 * API: cb(err, job)
 */
function createJob(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.string(opts.service, 'opts.service');
        mod_assert.string(opts.period, 'opts.period');
        mod_assert.object(opts.config, 'opts.config');
        mod_assert.object(opts.log, 'opts.log');
        mod_assert.optionalObject(opts.exec_after, 'opts.exec_after');

        var date = floorDate(opts.date, opts.period);

        var wf = new mod_WFClient({
                url: opts.config.workflow.url,
                path: opts.config.workflow.path,
                log: opts.log
        });

        var jobConfig = opts.config.jobs[opts.service][opts.period];
        var mantaConfig = require(opts.config.mantaConfigFile);

        configureJob({
                jobConfig: jobConfig,
                date: date,
                log: opts.log
        });

        // TODO assert params
        var params = {
                // metadata for queries
                name: jobConfig.job.name,
                date: date.toISOString(),
                period: opts.period,
                service: opts.service,

                // job parameters
                assets: opts.config.assets,
                jobManifest: jobConfig.job,
                keygenArgs: jobConfig.keygenArgs,
                mantaConfig: mantaConfig.manta,
                monitorBackoff: opts.config.monitorBackoff,
                retryBackoff: opts.config.retryBackoff,

                // use the name of the job as the workflow target to ensure
                // no two jobs are running for the same service/period/date
                // at the same time
                target: jobConfig.job.name,

                // schedule the job to run at a later date if desired
                exec_after: opts.exec_after
        }

        opts.log.info(params, 'Job parameters');

        wf.loadWorkflow(jobConfig.workflow, function onLoadWF(err) {
                if (err) {
                        opts.log.error(err);
                        cb(err);
                        return;
                }

                wf.createJob(jobConfig.workflow, params,
                        function onCreateJob(err, job) {

                        if (err) {
                                opts.log.error(err);
                                cb(err);
                                return;
                        }

                        opts.log.info({job: job}, 'Job created.');
                        cb(null, job);
                });
        });
}


/*
 * Generates the lookup table that maps uuid->login via redis.
 *
 * - opts: redis client config
 * - cb: callback in the form f(err, result), where result is a javascript
 *   object that maps uuid -> login.
 */
function generateLookup(opts, log, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.number(opts.port, 'opts.port');
        mod_assert.string(opts.host, 'opts.host');
        mod_assert.optionalObject(opts.options, 'opts.options');
        mod_assert.optionalNumber(opts.maxParallel, 'opts.maxParallel');
        mod_assert.object(log, 'log');
        mod_assert.func(cb, 'cb');

        //TODO remove this
        cb(null, {'foo':'bar'});
        return;
        var result = {};
        var queue = mod_vasync.queue(getLogin, opts.maxParallel || 10);
        var redis;

        function getLogin(uuid, callback) {
                redis.get('/uuid/' + uuid, function onGet(err, login) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        result[uuid] = login;
                        callback();
                });
        }

        queue.drain = function lookupDrain() {
                redis.quit();
                cb(null, result);
        };

        mod_redis.createClient({
                host: opts.host,
                log: log,
                options: opts.options,
                port: opts.port,
                connectTimeout: opts.connectTimeout,
                retries: opts.retries,
                minTimeout: opts.minTimeout,
                maxTimeout: opts.maxTimeout
        }, function onClient(err, client) {
                if (err) {
                        cb(err);
                        return;
                }
                redis = client;

                redis.on('error', function onError(err2, res) {
                        if (err2) {
                                cb(err2);
                                return;
                        }
                });

                redis.smembers('uuid', function onSmembers(err2, res) {
                        if (err2) {
                                cb(err2);
                                return;
                        }
                        if (res.length <= 0) {
                                log.fatal('Empty response from redis.');
                                process.exit(1);
                        }
                        queue.push(res);
                });
        });
}


/*
 * Configures the job to include any environment variables and date string
 * formatting.
 *
 * - jobConfig: job configuration
 * - date: javascript Date object
 * - log: logger
 */
function configureJob(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.jobConfig, 'opts.jobConfig');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.object(opts.log, 'opts.log');

        /*
         * Returns a string with any occurrences of '$year', '$month' '$day'
         * and '$hour' in str with their respective strings replaced ('2013'
         * for '$year', '01' for '$month' etc.).
         *
         * - str: source string - date: javascript Date object
         */
        function dateStringFormat(str, date) {
                mod_assert.string(str, 'str');
                mod_assert.object(date, 'date');

                // date methods return numbers not strings, so pad if needed
                function pad(num) { return (num < 10 ? '0' + num : num); }

                var year = date.getUTCFullYear();
                var month = pad(date.getUTCMonth() + 1); // zero-based months...
                var day = pad(date.getUTCDate());
                var hour = pad(date.getUTCHours());

                var result = str.replace(/\$year/g, year);
                result = result.replace(/\$month/g, month);
                result = result.replace(/\$day/g, day);
                result = result.replace(/\$hour/g, hour);

                return (result);
        }

        var jobConfig = opts.jobConfig;
        var jobManifest = jobConfig.job;
        var keygenArgs = jobConfig.keygenArgs;
        var date = opts.date;
        var log = opts.log;

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

        // insert date into keygen source path
        keygenArgs.source = dateStringFormat(keygenArgs.source, date);
        keygenArgs.date = date.toISOString();

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
        log.info('Job manifest ' +  JSON.stringify(jobManifest));
}


/*
 * Returns the top of the hour, midnight of the day, or first of the month.
 * Used to normalize input dates
 */
function floorDate(date, period) {
        var result;
        if (period === 'hourly') {
                result = new Date(
                        date.getUTCFullYear(),
                        date.getUTCMonth(),
                        date.getUTCDate(),
                        date.getUTCHours());
        }
        if (period === 'daily') {
                result = new Date(
                        date.getUTCFullYear(),
                        date.getUTCMonth(),
                        date.getUTCDate());
        }
        if (period === 'monthly') {
                result = new Date(
                        date.getUTCFullYear(),
                        date.getUTCMonth(),
                        1);
        }
        return (result);
}


function shouldRun(initialTime, runTime, backoffStrategy) {
        var elapsed = runTime.getTime() - initialTime.getTime();
        if (elapsed === 0) { return ('backoff'); }

        var elapsedDelay = backoffStrategy.initialDelay;
        var delay = elapsedDelay;
        for (var i = 0; i < backoffStrategy.failAfter; i++) {
                delay = Math.max(delay * 2, backoffStrategy.maxDelay);
                if (elapsedDelay === elapsed) {
                       return ('ready');
                }
                if (elapsedDelay > elapsed) {
                        return ('backoff');
                }
                elapsedDelay += delay;
        }
        return ('fail');
}


function retryFailedJobs(opts, cb) {
        mod_assert.object(opts.config, 'opts.config');
        mod_assert.object(opts.log, 'opts.log');

        var mantaConfig = require(opts.config.mantaConfigfile);
        var backoff = opts.config.retryBackoff;
        var recorder = new mod_jobrecorder({
                config: opts.config.moray,
                bucket: opts.config.bucket,
                log: opts.log
        });

        recorder.getFailedJobs(function gotFailedJobs(err, jobs) {
                if (err) {
                        cb(err);
                        return;
                }

                var initialTime, runTime, count;

                for (var i = 0; i < jobs.length; i++) {
                        initialTime = new Date(jobs[i].date);
                        runTime = floorDate(new Date(), jobs[i].period);
                        var run = shouldRun(initialTime, runTime, backoff);
                        if (run === 'ready') {
                                log.info(jobs[i], 'job re-run');
                                createJob({
                                        date: jobs[i].date,
                                        service: jobs[i].service,
                                        period: jobs[i].period,
                                        config: opts.config,
                                        log: opts.log
                                }, function (err, job) {
                                        // logged by parent
                                        count++;
                                });
                        } else if (run === 'backoff') {
                                log.info(jobs[i], 'job not re-run');
                        } else if (run === 'fail') {
                                //TODO fail
                        }
                }
        });
}
