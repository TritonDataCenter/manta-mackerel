// Copyright (c) 2013, Joyent, Inc. All rights reserved.
/*
 * API:
 * Partial job operations:
 * - uploadAssets(jobManifest)
 * - createJob(jobManifest)
 * - addInputKeys(jobPath, keygen)
 * - endJobInput(jobPath)
 * - monitorJob(jobPath)
 * - getResults(jobPath)
 * - getOutput(jobPath)
 * - getErrors(jobPath)
 * - getFailures(jobPath)
 * - getTimes(jobPath)
 *
 * End-to-end job operations (these do all of the above)
 * - doJob(jobManifest, keygen)
 * - doJobError(jobManifest, keygen)
 * - doJobWithRetry(jobManifest, keygen)
 */

var mod_assert = require('assert-plus');
var mod_backoff = require('backoff');
var mod_crypto = require('crypto');
var mod_fs = require('fs');
var mod_libmanta = require('libmanta');
var mod_path = require('path');
var mod_util = require('util');
var mod_vasync = require('vasync');


module.exports = {
        uploadAssets: uploadAssets,
        createJob: createJob,
        addInputKeys: addInputKeys,
        endJobInput: endJobInput,
        monitorJob: monitorJob,
        getResults: getResults,
        getOutput: getOutput,
        getErrors: getErrors,
        getFailures: getFailures,
        getTimes: getTimes,
        doJob: doJob,
        doJobError: doJobError,
        doJobWithRetry: doJobWithRetry
};


/*
 * uploads all assets specified in the job manifest
 * each asset must have an entry in 'assets' object in the configuration that
 * maps the manta path to the local path where the asset can be found
 * e.g.
 * assets : {
 *      '/user/stor/manta/asset': '/home/user/path/to/file',
 *      ...
 * }
 *
 * - assets: mapping from manta asset to local file
 * - jobManifest: job manifest object (see marlin docs)
 * - client: manta client
 * - log: logger
 * - cb: callback in the form f(err)
 */
function uploadAssets(opts, cb) {
        mod_assert.object(opts);
        mod_assert.object(opts.assets, 'assets');
        mod_assert.object(opts.jobManifest, 'jobManifest');
        mod_assert.object(opts.client, 'client');
        mod_assert.object(opts.log, 'log');
        mod_assert.func(cb, 'callback');

        var assets = opts.assets;
        var jobManifest = opts.jobManifest;
        var client = opts.client;
        var log = opts.log;

        var assetList = {};
        var queue, p, a;

        // look up the asset's local file path
        function upload(mantaPath, callback) {
                var localPath = assets[mantaPath];

                if (!localPath) {
                        var errMsg = 'Mapping for asset ' + mantaPath +
                                ' not found.';
                        log.error({err: errMsg});
                        cb(errMsg);
                        return;
                }

                stat(localPath, mantaPath, callback);
        }

        // get the asset's local file stats
        function stat(localPath, mantaPath, callback) {
                mod_fs.stat(localPath, function onStat(err, stats) {
                        if (err) {
                                log.error({err: err, path: localPath});
                                cb(err);
                                return;
                        }

                        if (!stats.isFile()) {
                                log.error({err: 'Not a file'}, localPath);
                                cb('Not a file');
                                return;
                        }

                        getHash(localPath, mantaPath, stats, callback);
                });
        }

        // md5 the local file
        function getHash(localPath, mantaPath, stats, callback) {
                var hash = mod_crypto.createHash('md5');
                var stream = mod_fs.createReadStream(localPath);
                var md5;

                stream.on('data', function onData(data) {
                        hash.update(data);
                });

                stream.once('end', function finishHash() {
                        md5 = hash.digest('base64');
                        getInfo(localPath, mantaPath, stats, md5, callback);
                });
        }

        // info the asset and check hash against the local file's hash
        function getInfo(localPath, mantaPath, stats, md5, callback) {
                client.info(mantaPath, function onInfo(err, info) {
                        if (err && err.statusCode !== 404) {
                                log.error({err: err, path: mantaPath}, 'info');
                                cb(err);
                                return;
                        }

                        if (info && info.md5 === md5) {
                                log.info(
                                        'Manta asset ' + mantaPath +
                                        ' and local asset ' + localPath +
                                        ' do not differ. Not uploading.');
                                callback();
                                return;
                        }

                        mkdirp(localPath, mantaPath, stats, callback);
                });
        }

        // create any intermediate directories
        function mkdirp(localPath, mantaPath, stats, callback) {
                var mantaDir = mod_path.dirname(mantaPath);
                client.mkdirp(mantaDir, function onMkdirp(err) {
                        if (err) {
                                log.error({err: err});
                                cb(err);
                                return;
                        }
                        log.info('Directory ' + mantaDir + ' created');
                        put(localPath, mantaPath, stats, callback);
                });
        }

        // put the asset in manta
        function put(localPath, mantaPath, stats, callback) {
                var options = {
                        copies: 2,
                        size: stats.size
                };
                var stream = mod_fs.createReadStream(localPath);

                log.info('Uploading asset ' + mantaPath);

                stream.pause();
                stream.on('open', function onOpen() {
                        client.put(mantaPath, stream, options,
                                function onPut(err) {

                                if (err) {
                                        log.error({err: err});
                                        cb(err);
                                        return;
                                }

                                callback();
                        });
                });
        }

        // find all assets listed in the job's phases
        for (p in jobManifest.phases) {
                for (a in jobManifest.phases[p].assets) {
                        assetList[jobManifest.phases[p].assets[a]] = true;
                }
        }

        log.info('Assets to upload', Object.keys(assetList));

        if (Object.keys(assetList).length === 0) {
                log.info('Finished uploading assets.');
                cb();
                return;
        }

        // upload each asset in series
        queue = mod_libmanta.createQueue({
                limit: 1,
                worker: upload
        });

        queue.on('end', function onEnd() {
                // all files done
                log.info('Finished uploading assets.');
                cb();
        });

        Object.keys(assetList).forEach(function (asset) {
                queue.push(asset);
        });

        queue.close();
}


/*
 * creates the job described by jobManifest
 *
 * - jobManifest: job manifest object (see marlin docs)
 * - log: logger
 * - client: manta client
 * - cb: callback in the form f(err, jobPath)
 */
function createJob(opts, cb) {
        mod_assert.object(opts);
        mod_assert.object(opts.jobManifest, 'jobManifest');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var jobManifest = opts.jobManifest;
        var log = opts.log;
        var client = opts.client;

        log.info('Begin create job');

        client.createJob(jobManifest, function onCreateJob(err, jobPath) {
                if (err) {
                        log.error({err: err});
                } else {
                        log.info({jobPath: jobPath}, 'Job created.');
                }
                cb(err, jobPath);
        });
}


/*
 * adds Manta keys generated by the keygen to the job input
 * keys that fail to be added are logged and skipped - the job will run with
 * any keys that were successfully added
 *
 * - keygen: event emitter that emits ('key', key) for each key and
 *   ('end') when finished, and begins emitting keys when start() is called.
 * - jobPath: job path
 * - log: logger
 * - client: manta client
 * - cb: callback in the form f(err, jobPath, count), where count is the number
 *   of input keys successfully added to the job
 */
function addInputKeys(opts, cb) {
        mod_assert.object(opts);
        mod_assert.object(opts.keygen, 'keygen');
        mod_assert.string(opts.jobPath, 'jobPath');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var jobPath = opts.jobPath;
        var log = opts.log;
        var client = opts.client;
        var keygen = opts.keygen;

        var barrier = mod_vasync.barrier();
        var errors = [];
        var count = 0;

        function onKey(key) {
                log.info({jobPath: jobPath, key: key}, 'Adding key ' + key);
                barrier.start('key' + key);
                client.addJobKey(jobPath, key, function onAddKey(err, _) {
                        if (err) {
                                log.error({err: err, jobPath: jobPath});
                                errors.push({err: err, jobPath: jobPath});
                                barrier.done('key' + key);
                                return;
                        }
                        count++;
                        barrier.done('key' + key);
                });
        }

        function onError(err) {
                errors.push({err: err, jobPath: jobPath});
                barrier.done('addkeys');
        }

        log.info({jobPath: jobPath}, 'Begin adding job input keys.');

        barrier.once('drain', function onDrain() {
                // all keys added
                keygen.removeListener('key', onKey);
                keygen.removeListener('error', onError);

                if (count <= 0) {
                        var errMsg = 'No input keys added to job ' + jobPath;
                        log.error({err: errMsg, jobPath: jobPath});
                        cb(errMsg, jobPath, count);
                        return;
                }

                log.info({jobPath: jobPath}, 'Added ' + count + ' keys.');

                if (errors.length > 0) {
                        cb(errors, jobPath, count);
                } else {
                        cb(null, jobPath, count);
                }
        });

        barrier.start('addkeys');

        keygen.on('error', onError);

        keygen.on('key', onKey);

        keygen.once('end', function onEnd() {
                barrier.done('addkeys');
        });


        keygen.start();
}


/*
 * ends input for the job
 *
 * - jobPath: job path
 * - log: logger
 * - client: manta client
 * - cb: callback in the form f(err, jobPath)
 */
function endJobInput(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.jobPath, 'jobPath');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var jobPath = opts.jobPath;
        var log = opts.log;
        var client = opts.client;

        log.info({jobPath: jobPath}, 'Ending input.');

        client.endJob(jobPath, function onEndJob(err, _) {
                if (err) {
                        log.error({err: err, jobPath: jobPath});
                        cb({err: err, jobPath: jobPath}, jobPath);
                } else {
                        log.info({jobPath: jobPath}, 'Job input ended.');
                        cb(null, jobPath);
                }
        });
}


/*
 * monitors job for done state
 *
 * - monitorBackoff (optional): exponential backoff strategy in the form:
 *      {
 *              initialDelay: 1000, // in milliseconds
 *              maxDelay: 120000, // in milliseconds
 *              failAfter: 20
 *      }
 * - jobPath: job path
 * - log: logger
 * - client: manta client
 * - cb: callback in the form f(err, jobPath)
 */
function monitorJob(opts, cb) {
        mod_assert.object(opts);
        mod_assert.optionalObject(opts.monitorBackoff, 'monitorBackoff');
        mod_assert.string(opts.jobPath, 'jobPath');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var jobPath = opts.jobPath;
        var log = opts.log;
        var client = opts.client;

        var monitorBackoff = opts.monitorBackoff || {
                initialDelay: 1000,
                maxDelay: 120000,
                failAfter: 20
        };

        var monitor = mod_backoff.exponential({
                initialDelay: monitorBackoff.initialDelay,
                maxDelay: monitorBackoff.maxDelay
        });

        log.info({jobPath: jobPath}, 'Monitoring job.');

        monitor.failAfter(monitorBackoff.failAfter);

        monitor.on('ready', function onReady(attempts, delayms) {
                log.info({jobPath: jobPath, attempt: attempts, next: delayms},
                        'Checking job status.');

                client.job(jobPath, function onJob(err, job) {
                        if (err) {
                                log.warn({jobPath: jobPath, err: err});
                                monitor.backoff();
                                return;
                        }

                        if (job.state !== 'done') {
                                monitor.backoff();
                                return;
                        }

                        log.info({jobPath: jobPath}, 'Job done.');
                        monitor.reset(); // stop monitoring
                        monitor.removeAllListeners('fail');
                        monitor.removeAllListeners('ready');
                        cb(null, jobPath);
                });

        });

        monitor.on('fail', function onFail() {
                var errMsg = 'Monitoring exceeded retry limit for for job '
                                        + jobPath;
                log.error({err: errMsg, jobPath: jobPath}, errMsg);
                cb({err: errMsg, jobPath: jobPath});
        });

        monitor.backoff();
}


/*
 * creates a list of output keys for the job
 *
 * - jobPath: job path
 * - log: logger
 * - client: manta client
 * - cb: callback in the form f(err, outputs), where outputs is an array
 */
function getOutput(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.jobPath, 'jobPath');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var outputs = [];
        var jobPath = opts.jobPath;
        var client = opts.client;

        client.jobOutput(jobPath, function onOutput(err, res) {
                if (err) {
                        cb({err: err, jobPath: jobPath}); return;
                }

                res.on('key', function (k) {
                        outputs.push(k);
                });

                res.once('end', cb.bind(null, null, outputs));
        });
}


/*
 * creates a list of errors for the job
 *
 * - jobPath: job path
 * - log: logger
 * - client: manta client
 * - cb: callback in the form f(err, errors), where errors is an array
 */
function getErrors(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.jobPath, 'jobPath');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var errors = [];
        var jobPath = opts.jobPath;
        var client = opts.client;

        client.jobErrors(jobPath, function (err, res) {
                if (err) {
                        cb({err: err, jobPath: jobPath}); return;
                }

                res.on('err', function (e) {
                        errors.push(e);
                });

                res.once('end', cb.bind(null, null, errors));
        });
}


/*
 * creates a list of failures for the job
 *
 * - jobPath: job path
 * - log: logger
 * - client: manta client
 * - cb: callback in the form f(err, failures), where failures is an array
 */
function getFailures(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.jobPath, 'jobPath');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var failures = [];
        var jobPath = opts.jobPath;
        var client = opts.client;

        client.jobFailures(jobPath, function (err, res) {
                if (err) {
                        cb({err: err, jobPath: jobPath}); return;
                }

                res.on('key', function (k) {
                        failures.push(k);
                });

                res.once('end', cb.bind(null, null, failures));
        });
}


/*
 * gets the creation time and finish time for the job
 *
 * - jobPath: job path
 * - log: logger
 * - client: manta client
 * - cb: callback in the form f(err, timeCreated, timeDone), both date strings
 */
function getTimes(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.jobPath, 'jobPath');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var jobPath = opts.jobPath;
        var client = opts.client;

        client.job(jobPath, function (err, res) {
                if (err) {
                        cb({err: err, jobPath: jobPath}); return;
                }

                cb(null, res.timeCreated, res.timeDone);
        });
}


/*
 * collects the results (outputs, errors, and failures) and the creation and
 * finish time of the job
 *
 * - jobPath: job path
 * - cb: callback in the form f(err, results), where results is in the form
 *      result: {
 *              'jobPath': jobPath,
 *              'timeCreated': timeCreated,
 *              'timeDone': timeDone,
 *              'outputs': [...],
 *              'errors': [...],
 *              'failures': [...]
 *      }
 */
function getResults(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.jobPath, 'jobPath');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.func(cb, 'callback');

        var barrier = mod_vasync.barrier();
        var jobPath = opts.jobPath;
        var client = opts.client;
        var log = opts.log;
        var result = {jobPath: jobPath};

        barrier.on('drain', function () {
                log.info({jobPath: jobPath}, 'Results fetched.');
                log.info({jobPath: jobPath, result: result});
                cb(null, result);
        });

        barrier.start('getOutput');
        barrier.start('getErrors');
        barrier.start('getFailures');
        barrier.start('getTimes');

        getOutput({
                jobPath: jobPath,
                client: client,
                log: log
        }, function (err, outputs) {
                if (err) {
                        cb({err: err, jobPath: jobPath}); return;
                }
                result.outputs = outputs;
                barrier.done('getOutput');
        });

        getErrors({
                jobPath: jobPath,
                client: client,
                log: log
        }, function (err, errors) {
                if (err) {
                        cb({err: err, jobPath: jobPath}); return;
                }
                result.errors = errors;
                barrier.done('getErrors');
        });

        getFailures({
                jobPath: jobPath,
                client: client,
                log: log
        }, function (err, failures) {
                if (err) {
                        cb({err: err, jobPath: jobPath}); return;
                }
                result.failures = failures;
                barrier.done('getFailures');
        });

        getTimes({
                jobPath: jobPath,
                client: client,
                log: log
        }, function (err, timeCreated, timeDone) {
                if (err) {
                        cb({err: err, jobPath: jobPath}); return;
                }
                result.timeCreated = timeCreated;
                result.timeDone = timeDone;
                barrier.done('getTimes');
        });
}


/*
 * does all the things
 *
 * - jobManifest: json job manifest (see marlin docs)
 * - keygen: event emitter that emits ('key', key) for each key, and ('end')
 *   when done
 * - cb: callback in the form f(err, result), where result is in the form
 *      result: {
 *              'jobPath': jobPath,
 *              'outputs': [...],
 *              'errors': [...],
 *              'failures': [...]
 *      }
 */
function doJob(opts, cb) {
        mod_assert.object(opts);
        mod_assert.object(opts.assets, 'assets');
        mod_assert.object(opts.jobManifest, 'jobManifest');
        mod_assert.object(opts.keygen, 'keygen');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.optionalObject(opts.monitorBackoff, 'monitorBackoff');
        mod_assert.func(cb, 'callback');

        var assets = opts.assets;
        var jobManifest = opts.jobManifest;
        var keygen = opts.keygen;
        var log = opts.log;
        var monitorBackoff = opts.monitorBackoff;
        var client = opts.client;

        function _uploadAssets() {
                uploadAssets({
                        jobManifest: jobManifest,
                        assets: assets,
                        client: client,
                        log: log
                }, function (err, _) {
                        if (err) {
                                cb(err); return;
                        }
                        _createJob();
                });
        }

        function _createJob() {
                createJob({
                        jobManifest: jobManifest,
                        client: client,
                        log: log
                }, function (err, jobPath) {
                        if (err) {
                                cb(err); return;
                        }
                        _addInputKeys(jobPath);
                });
        }

        function _addInputKeys(jobPath) {
                addInputKeys({
                        keygen: keygen,
                        jobPath: jobPath,
                        log: log,
                        client: client
                }, function (err, _, count) {
                        if (err) {
                                opts.log.error('Error adding key(s)', err);
                        }

                        if (count <= 0) {
                                endJobInput({
                                        jobPath: jobPath,
                                        log: log,
                                        client: client
                                }, function (err2) {
                                        if (err2) {
                                                cb(err2); return;
                                        }
                                        opts.log.error('Not monitoring job.');
                                        cb(err);
                                        return;
                                });
                        } else {
                                _endJobInput(jobPath);
                        }
                });
        }

        function _endJobInput(jobPath) {
                endJobInput({
                        jobPath: jobPath,
                        log: log,
                        client: client
                }, function (err) {
                        if (err) {
                                cb(err); return;
                        }
                        _monitorJob(jobPath);
                });
        }

        function _monitorJob(jobPath) {
                monitorJob({
                        monitorBackoff: monitorBackoff,
                        jobPath: jobPath,
                        log: log,
                        client: client
                }, function (err) {
                        if (err) {
                                cb(err); return;
                        }
                        _getResults(jobPath);
                });
        }

        function _getResults(jobPath) {
                getResults({
                        jobPath: jobPath,
                        log: log,
                        client: client
                }, function (err, results) {
                        if (err) {
                                cb(err); return;
                        }
                        cb(null, results);
                });
        }

        _uploadAssets();
}


/*
 * Runs the job and return an error if it does not succeed
 *
 * Success is defined as the job completing and having:
 * (1) at least one output key,
 * (2) zero failures, and
 * (3) zero errors
 *
 * - jobManifest: json job manifest (see marlin docs)
 * - keygen: event emitter that emits ('key', key) for each key, and ('end')
 *   when done
 * - cb: callback in the form f(err, result), where result is in the format:
 *      result: {
 *              jobPath: jobPath,
 *              outputs: [...]
 *      },
 *   and err, if it exists, is in the format:
 *      err: {
 *              jobPath: jobPath,
 *              errors: [...],
 *              failures: [...]
 *      }
 */
function doJobError(opts, cb) {
        mod_assert.object(opts);
        mod_assert.object(opts.assets, 'assets');
        mod_assert.object(opts.jobManifest, 'jobManifest');
        mod_assert.object(opts.keygen, 'keygen');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.optionalObject(opts.monitorBackoff, 'monitorBackoff');
        mod_assert.func(cb, 'callback');

        doJob({
                assets: opts.assets,
                jobManifest: opts.jobManifest,
                keygen: opts.keygen,
                log: opts.log,
                client: opts.client,
                monitorBackoff: opts.monitorBackoff
        }, function (err, res) {
                if (err) {
                        cb(err); return;
                }

                // translate job results into success/failure
                var isFailure = res.failures.length > 0 ||
                        res.errors.length > 0;
                var noOutput = res.outputs.length === 0;
                var result = {
                        outputs: res.outputs,
                        jobPath: res.jobPath
                };
                var errors;

                if (isFailure) {
                        opts.log.error({jobPath: res.jobPath},
                                'Failures or errors for job ' + res.jobPath);
                        errors = {
                                jobPath: res.jobPath,
                                errors: res.errors,
                                failures: res.failures
                        };
                        cb(errors);
                        return;
                }

                if (noOutput) {
                        opts.log.error({jobPath: res.jobPath},
                                'No output for job ' + res.jobPath);
                        errors = {
                                jobPath: res.jobPath,
                                errors: res.errors,
                                failures: res.failures
                        };
                        cb(errors);
                        return;
                }

                cb(errors, result);
        });
}


/*
 * Runs a job and retries it if it does not succeed.
 *
 * - jobManifest: json job manifest (see marlin docs)
 * - keygen: event emitter that emits ('key', key) for each key, and ('end')
 *   when done
 * - cb: callback in the form f(err, result), where result is in the format:
 *      result: {
 *              jobPath: jobPath,
 *              outputs: [...]
 *      },
 *   and err, if it exists, is in the format:
 *      err: {
 *              jobPath: jobPath,
 *              errors: [...],
 *              failures: [...]
 *      }
 */
function doJobWithRetry(opts, cb) {
        mod_assert.object(opts);
        mod_assert.object(opts.assets, 'assets');
        mod_assert.object(opts.jobManifest, 'jobManifest');
        mod_assert.object(opts.keygen, 'keygen');
        mod_assert.object(opts.log, 'log');
        mod_assert.object(opts.client, 'client');
        mod_assert.optionalObject(opts.monitorBackoff, 'monitorBackoff');
        mod_assert.optionalObject(opts.retryBackoff, 'retryBackoff');
        mod_assert.func(cb, 'callback');

        var log = opts.log;
        var jobManifest = opts.jobManifest;

        var retryBackoff = opts.retryBackoff || {
                initialDelay: 60000,
                maxDelay: 600000,
                failAfter: 5
        };

        var retry = mod_backoff.call(doJobError, {
                assets: opts.assets,
                jobManifest: opts.jobManifest,
                keygen: opts.keygen,
                log: opts.log,
                client: opts.client,
                monitorBackoff: opts.monitorBackoff
        }, function onFinish(err, res) {
                if (err) {
                        log.error('Retry limit exceeded for job ' +
                                jobManifest.name);
                        cb(err);
                        return;
                }

                log.info('Job successful');
                cb(err, res);
        });

        retry.setStrategy(new mod_backoff.ExponentialStrategy({
                initialDelay: retryBackoff.initialDelay,
                maxDelay: retryBackoff.maxDelay
        }));

        retry.failAfter(retryBackoff.failAfter);

        retry.on('backoff', function onBackoff(attempts, delayms) {
                log.info('(retry) Job ' + jobManifest.name + ' attempt ' +
                        attempts + '. Retry in ' + delayms + 'ms.');
        });
}
