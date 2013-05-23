// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_assert = require('assert-plus');
var mod_jobrunner = require('./jobrunner');
var mod_libmanta = require('libmanta');
var mod_manta = require('manta');
var mod_path = require('path');
var mod_WFClient = require('wf-client');
var mod_jsprim = require('jsprim');

module.exports = {
        meter: meter,
        createJob: createJob,
        generateLookup: generateLookup,
        floorDate: floorDate
};


/*
 * Creates and dispatches a workflow job that manages a marlin job.
 *
 * - date: Date object, the date for which to meter
 * - category: the category of usage to meter (storage, request, compute)
 * - period: the period over which to meter (hourly, daily, monthly)
 * - config: config object containing the marlin job manifests, manta config
 *   settings and asset mapping
 * - log: logger
 * - cb: callback in the form f(err, job), where job is the workflow job created
 */
function createJob(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.string(opts.category, 'opts.category');
        mod_assert.string(opts.period, 'opts.period');
        mod_assert.object(opts.config, 'opts.config');
        mod_assert.object(opts.log, 'opts.log');

        var date = floorDate(opts.date, opts.period);

        var wfpath = mod_path.resolve(__dirname, '..',
                opts.config.workflow.path);

        var wf = new mod_WFClient({
                url: opts.config.workflow.url,
                path: wfpath,
                log: opts.log
        });

        var mantaConfig = opts.config.manta;

        // jobConfig is a object containing the marlin job manifest as well as
        // other configuration details like the input key generator and any
        // environment variables to be set
        var jobName = opts.category + '.' + opts.period;
        var jobConfig = mod_jsprim.pluck(opts.config.jobs, jobName);

        // jobConfig is modified as part of configureJob
        configureJob({
                jobConfig: jobConfig,
                date: date
        });

        opts.log.info(jobConfig, 'Job configured.');

        var assets = generateAssetMap({
                job: jobConfig.job,
                mantaDir: opts.config.jobs.mantaBaseDirectory,
                localDir: mod_path.resolve(__dirname, '..')
        });

        opts.log.info(assets, 'Asset map generated.');

        var params = {
                // metadata useful for workflow queries
                name: jobConfig.job.name,
                date: date.toISOString(),
                period: opts.period,
                category: opts.category,

                // job parameters
                assets: assets,
                jobManifest: jobConfig.job,
                keygen: jobConfig.keygen,
                keygenArgs: jobConfig.keygenArgs,
                mantaConfig: mantaConfig.manta,
                monitorBackoff: opts.config.monitorBackoff,

                // use the name of the job as the workflow target to ensure
                // no two jobs are running for the same category/period/date
                // at the same time
                target: jobConfig.job.name
        };

        opts.log.info(params, 'Job parameters');

        wf.loadWorkflow('runjob', function onLoadWF(err) {
                if (err) {
                        opts.log.error(err, 'Error loading workflow.');
                        cb(err);
                        wf.close();
                        return;
                }

                wf.createJob('runjob', params,
                        function onCreateJob(err2, job) {

                        if (err2) {
                                opts.log.error(err2,
                                               'Error creating workflow job.');
                                cb(err2);
                                wf.close();
                                return;
                        }

                        opts.log.info({job: job}, 'Workflow job created.');
                        wf.close();
                        cb(null, job);
                        return;
                });
        });
}


/*
 * Creates and dispatches a metering job to marlin and waits for it to complete.
 * This function bypasses the workflow.
 *
 * config.jobConfig.job is modified as a side-effect.
 *
 * - date: Date object, the date for which to meter
 * - category: the category of usage to meter (storage, request, compute)
 * - period: the period over which to meter (hourly, daily, monthly)
 * - config: config object containing the marlin job manifests, manta config
 *   settings and asset mapping
 * - log: logger
 * - cb: callback in the form f(err, result), where result is in the format
 *      result: {
 *              'jobPath': jobPath,
 *              'outputs': [...],
 *              'errors': [...],
 *              'failures': [...]
 *      }
 */
function meter(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.string(opts.category, 'opts.category');
        mod_assert.string(opts.period, 'opts.period');
        mod_assert.object(opts.config, 'opts.config');
        mod_assert.object(opts.log, 'opts.log');

        var date = floorDate(opts.date, opts.period);

        var mantaConfig = opts.config.manta;
        mantaConfig.log = opts.log;
        var client = new mod_manta.createClient(mantaConfig);

        // jobConfig is a object containing the marlin job manifest as well as
        // other configuration details like the input key generator and any
        // environment variables to be set
        var jobName = opts.category + '.' + opts.period;
        var jobConfig = mod_jsprim.pluck(opts.config.jobs, jobName);

        // jobConfig is modified as part of configureJob
        configureJob({
                jobConfig: jobConfig,
                date: date
        });

        opts.log.info(jobConfig, 'Job configured.');

        var assets = generateAssetMap({
                job: jobConfig.job,
                mantaDir: opts.config.jobs.mantaBaseDirectory,
                localDir: mod_path.resolve(__dirname, '..')
        });

        opts.log.info(assets, 'Asset map generated.');

        var keygen = require(__dirname + '/keygen/' + jobConfig.keygen).keygen({
                client: client,
                log: opts.log,
                args: jobConfig.keygenArgs
        });

        mod_jobrunner.doJob({
                assets: assets,
                jobManifest: jobConfig.job,
                keygen: keygen,
                log: opts.log,
                client: client,
                monitorBackoff: opts.config.monitorBackoff
        }, function jobDone(err, result) {
                client.close();
                if (err) {
                        cb(err);
                        return;
                }
                opts.log.info('Done job');
                cb(null, result);
        });
}


/*
 * Generates the lookup table that maps uuid->login via redis.
 *
 * - host: mahi host
 * - port: mahi port
 * - log: logger passed to the client
 * - (optional) maxParallel
 * - (optioal) redis_options
 * - cb: callback in the form f(err, result), where result is an object that
 *   maps uuid -> login.
 */
function generateLookup(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.func(cb, 'cb');

        function getLogin(uuid, callback) {
                mahi.userFromUUID(uuid, function gotUser(err, user) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        result[uuid] = user.login;
                        callback();
                });
        }

        var result = {};
        var mahi = mod_libmanta.createMahiClient(opts);
        var queue = mod_libmanta.createQueue({
                limit: 10,
                worker: getLogin
        });

        queue.once('error', cb.bind(null));
        queue.once('end', function onEnd() {
                mahi.close();
                cb(null, result);
        });

        mahi.once('error', cb.bind(null));
        mahi.once('connect', function onConnect() {
                mahi.setMembers('uuid', function gotUuids(err, uuids) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        for (var uuid in uuids) {
                                queue.push(uuids[uuid]);
                        }
                        queue.close();
                });
        });
}


/*
 * Configures the job to include any environment variables and date string
 * formatting.
 *
 * - jobConfig: job configuration
 * - date: javascript Date object
 */
function configureJob(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.jobConfig, 'opts.jobConfig');
        mod_assert.object(opts.date, 'opts.date');

        /*
         * Returns a string with any occurrences of '$year', '$month' '$day'
         * and '$hour' in str with their respective strings replaced ('2013'
         * for '$year', '01' for '$month' etc.).
         *
         * - str: source string - date: javascript Date object
         */
        function dateStringFormat(str, d) {
                mod_assert.string(str, 'str');
                mod_assert.object(d, 'date');

                // date methods return numbers not strings, so pad if needed
                function pad(num) { return (num < 10 ? '0' + num : num); }

                var year = d.getUTCFullYear();
                var month = pad(d.getUTCMonth() + 1); // zero-based months...
                var day = pad(d.getUTCDate());
                var hour = pad(d.getUTCHours());

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

        var envString = '';
        var p, prepend, exec, numReducers;

        // insert date into any environment variables that need it (e.g.
        // destination path: /user/stor/usage/2013/01/01) and create the string
        // of environment variables to prepend to the exec string in the form
        // 'VAR0="value0" VAR1="value1" ... VARN="valuen"'
        if (jobConfig.env) {
                Object.keys(jobConfig.env).forEach(function (k) {
                        jobConfig.env[k] =
                                dateStringFormat(jobConfig.env[k], date);
                        envString += k + '="' + jobConfig.env[k] + '" ';
                });
        }

        // insert date into keygen source path
        keygenArgs.source = keygenArgs.source ?
                dateStringFormat(keygenArgs.source, date) : undefined;
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
}


/*
 * Takes each asset in the job and adds a corresponding entry in a map
 * that points to the local path for that asset. E.g.
 * /poseidon/stor/usage/assets/node_modules.tar ->
 * /opt/smartdc/mackerel/assets/node_modules.tar
 */
function generateAssetMap(opts) {
        var job = opts.job;
        var mantaDir = opts.mantaDir;
        var localDir = opts.localDir;
        var map = {};
        job.phases.forEach(function (phase) {
                phase.assets.forEach(function (asset) {
                        map[asset] = map[asset] ||
                                     asset.replace(mantaDir, localDir);
                });
        });
        return (map);
}


/*
 * Returns the top of the hour, midnight of the day, or first of the month.
 * Used to normalize input dates
 */
function floorDate(date, period) {
        var result;
        if (period === 'hourly' || period === 'deliver') {
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
        if (!result) {
                throw new Error('Invalid period: ' + period);
        }
        return (result);
}
