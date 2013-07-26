// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_assert = require('assert-plus');
var mod_jobrunner = require('./jobrunner');
var mod_manta = require('manta');
var mod_path = require('path');
var mod_WFClient = require('wf-client');

module.exports = {
        meter: meter,
        createJob: createJob,
        configureJob: configureJob,
        generateAssetMap: generateAssetMap
};


/*
 * Creates and dispatches a workflow job that manages a marlin job.
 *
 * - date: Date object, the date for which to meter
 * - config: config object containing the marlin job manifests, manta config
 *   settings and asset mapping
 * - log: logger
 * - cb: callback in the form f(err, job), where job is the workflow job created
 */
function createJob(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.config, 'opts.config');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.object(opts.jobConfig, 'opts.jobConfig');
        mod_assert.object(opts.log, 'opts.log');

        var date = opts.date;

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
        var jobConfig = opts.jobConfig;

        // jobConfig is modified as part of configureJob
        configureJob({
                jobConfig: jobConfig,
                date: date
        });

        opts.log.info(jobConfig, 'Job configured.');

        var assets = generateAssetMap({
                job: jobConfig.job,
                mantaDir: opts.config.mantaBaseDirectory,
                localDir: mod_path.resolve(__dirname, '..')
        });

        opts.log.info(assets, 'Asset map generated.');

        var params = {
                // metadata useful for workflow queries
                name: jobConfig.job.name,
                date: date.toISOString(),

                // job parameters
                assets: assets,
                jobManifest: jobConfig.job,
                keygen: jobConfig.keygen,
                keygenArgs: jobConfig.keygenArgs,
                mantaConfig: mantaConfig.manta,
                monitorBackoff: opts.config.monitorBackoff,
                linkPath: jobConfig.linkPath,

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
        mod_assert.object(opts.config, 'opts.config');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.object(opts.jobConfig, 'opts.jobConfig');
        mod_assert.object(opts.log, 'opts.log');

        var date = opts.date;

        var mantaConfig = opts.config.manta;
        mantaConfig.log = opts.log;
        var client = new mod_manta.createClient(mantaConfig);

        // jobConfig is a object containing the marlin job manifest as well as
        // other configuration details like the input key generator and any
        // environment variables to be set
        var jobConfig = opts.jobConfig;

        // jobConfig is modified as part of configureJob
        configureJob({
                jobConfig: jobConfig,
                date: date
        });

        opts.log.info(jobConfig, 'Job configured.');

        var assets = generateAssetMap({
                job: jobConfig.job,
                mantaDir: opts.config.mantaBaseDirectory,
                localDir: mod_path.resolve(__dirname, '..')
        });

        opts.log.info(assets, 'Asset map generated.');

        var keygen = require(__dirname + '/keygen/' + jobConfig.keygen).keygen({
                client: client,
                log: opts.log,
                args: jobConfig.keygenArgs
        });

        mod_jobrunner.doJobWithRetry({
                assets: assets,
                jobManifest: jobConfig.job,
                keygen: keygen,
                log: opts.log,
                client: client,
                monitorBackoff: opts.config.monitorBackoff
        }, function jobDone(err, result) {
                if (err) {
                        cb(err);
                        opts.log.fatal(err, 'job error');
                        client.close();
                        return;
                }
                opts.log.info('Done job');
                if (result.outputs.length && jobConfig.linkPath) {
                        client.ln(result.outputs[0], jobConfig.linkPath,
                                function (lnerr) {

                                client.close();
                                if (lnerr) {
                                        opts.log.warn(lnerr,
                                                'Failed to create link.');
                                        cb(lnerr);
                                        return;
                                }
                                opts.log.info('Link created ' +
                                        jobConfig.linkPath + ' -> ' +
                                        result.outputs[0]);
                                cb(null, result);
                                return;
                        });
                }
                client.close();
                cb(null, result);
                return;
        });
}


/*
 * Configures the job to include any environment variables and date string
 * formatting. Modifies opts.jobConfig. No return value.
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
                        if (typeof (jobConfig.env[k]) !== 'string') {
                                return;
                        }
                        jobConfig.env[k] =
                                dateStringFormat(jobConfig.env[k], date);
                        envString += k + '="' + jobConfig.env[k] + '" ';
                });
        }

        // insert date into keygen source path
        if (typeof (keygenArgs.source) === 'string') {
                keygenArgs.source = dateStringFormat(keygenArgs.source, date);
        } else if (Array.isArray(keygenArgs.source)) {
                for (var i = 0; i < keygenArgs.source.length; i++) {
                        keygenArgs.source[i] =
                                dateStringFormat(keygenArgs.source[i], date);
                }
        }

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
