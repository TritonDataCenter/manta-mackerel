/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_assert = require('assert-plus');
var mod_jobrunner = require('./jobrunner');
var mod_manta = require('manta');
var mod_path = require('path');

module.exports = {
        meter: meter,
        configureJob: configureJob,
        generateAssetMap: generateAssetMap
};

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
        mod_assert.optionalBool(opts.retry, 'opts.retry');

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
                mantaDir: opts.config.mantaBaseDirectory,
                date: date
        });

        opts.log.info(jobConfig, 'Job configured.');

        var assets = generateAssetMap({
                job: jobConfig.job,
                mantaDir: opts.config.mantaBaseDirectory,
                localDir: mod_path.resolve(__dirname, '..'),
                overrides: opts.config.assetOverrides
        });

        opts.log.info(assets, 'Asset map generated.');

        var keygen = require(__dirname + '/keygen/' + jobConfig.keygen).keygen({
                client: client,
                log: opts.log,
                args: jobConfig.keygenArgs
        });

        var func = opts.retry ? 'doJobWithRetry' : 'doJobError';
        mod_jobrunner[func]({
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
                        if (jobConfig.linkPath[0] !== '/') {
                                jobConfig.linkPath =
                                        opts.config.mantaBaseDirectory + '/' +
                                        jobConfig.linkPath;
                        }
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
                } else {
                        client.close();
                        cb(null, result);
                        return;
                }
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
        mod_assert.string(opts.mantaDir, 'opts.mantaDir');

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
        var mantaDir = opts.mantaDir;

        var envString = '';
        var a, p, prepend, exec, numReducers, assets, init;

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
                        jobConfig.env[k] = jobConfig.env[k].replace(/\$base/g,
                                mantaDir);
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
                assets = jobManifest.phases[p].assets;
                init = jobManifest.phases[p].init;

                // prepend the manta base directory to relative asset paths
                if (assets) {
                        for (a = 0; a < assets.length; a++) {
                                if (assets[a][0] !== '/') {
                                        assets[a] = mantaDir + '/' + assets[a];
                                }
                        }
                }

                // check if the next phase is a reduce phase and make available
                // the reducer count in the previous phase (for msplit)
                if (p + 1 < jobManifest.phases.length &&
                        jobManifest.phases[p + 1].type === 'reduce') {

                        numReducers = jobManifest.phases[p + 1].count || 1;
                        prepend += 'NUM_REDUCERS=' + numReducers + ' ';
                }

                // if exec or init is relative, add the manta base directory
                if (exec[0] !== '/') {
                        exec = '/assets' + mantaDir + '/' + exec;
                }
                if (init && init[0] !== '/') {
                        jobManifest.phases[p].init = '/assets' + mantaDir +
                                '/' + init;
                }

                // add all the environment variables
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
        var overrides = opts.overrides || {};
        var map = {};
        job.phases.forEach(function (phase) {
                if (phase.assets) {
                        phase.assets.forEach(function (asset) {
                                map[asset] = overrides[asset] ||
                                             map[asset] ||
                                             asset.replace(mantaDir, localDir);
                        });
                }
        });
        return (map);
}
