// Copyright (c) 2013, Joyent, Inc. All rights reserved.
/*
 * JobRunner
 *
 * API:
 * Partial job operations:
 * - uploadAssets(jobManifest)
 * - createJob(jobManifest)
 * - addInputKeys(jobPath, keygen)
 * - endJobInput(jobPath)
 * - monitorInput(jobPath)
 * - getResults(jobPath)
 * - getOutput(jobPath)
 * - getErrors(jobPath)
 * - getFailures(jobPath)
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
var mod_path = require('path');
var mod_vasync = require('vasync');

module.exports.JobRunner = JobRunner;


/*
 * JobRunner constructor
 *
 * Backoff strategies should contain fields for 'initialDelay' and 'maxDelay' as
 * number of milliseconds, and 'failAfter' as a number.
 *
 * - assets: mapping from manta asset to local file
 * - client: manta client
 * - log: logger
 * - monitorBackoff (optional): backoff strategy when monitor a job for output
 * - retryBackoff (optional): backoff strategy when retrying jobs
 */
function JobRunner(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.assets, 'opts.assets');
        mod_assert.object(opts.client, 'opts.client');
        mod_assert.object(opts.log, 'opts.log');
        mod_assert.optionalObject(opts.monitorBackoff, 'opts.monitorBackoff');
        mod_assert.optionalObject(opts.retryBackoff, 'opts.retryBackoff');

        this.monitorBackoff = opts.monitorBackoff || {
                initialDelay: 1000,
                maxDelay: 120000,
                failAfter: 20
        };

        this.retryBackoff = opts.retryBackoff || {
                initialDelay: 60000,
                maxDelay: 600000,
                failAfter: 3
        };

        this.assets = opts.assets;
        this.client = opts.client;
        this.log = opts.log.child({component: 'jobrunner'}, true);
}


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
 * - jobManifest: job manifest object (see marlin docs)
 * - cb: callback in the form f(err)
 */
JobRunner.prototype.uploadAssets = function uploadAssets(jobManifest, cb) {
        mod_assert.object(jobManifest, 'jobManifest');
        mod_assert.func(cb, 'callback');

        var self = this;

        var assetList = {};
        var queue, p, a;

        // look up the asset's local file path
        function upload(mantaPath, callback) {
                var localPath = self.assets[mantaPath];

                if (!localPath) {
                        var errMsg = 'Mapping for asset ' + mantaPath +
                                ' not found.';
                        self.log.error(errMsg);
                        cb(errMsg);
                        return;
                }

                stat(localPath, mantaPath, callback);
        }

        // get the asset's local file stats
        function stat(localPath, mantaPath, callback) {
                mod_fs.stat(localPath, function onStat(err, stats) {
                        if (err) {
                                self.log.error(err, localPath);
                                cb(err);
                                return;
                        }

                        if (!stats.isFile()) {
                                self.log.error('Not a file', localPath);
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

                stream.once('end', function onEnd() {
                        md5 = hash.digest('base64');
                        getInfo(localPath, mantaPath, stats, md5, callback);
                });
        }

        // info the asset and check hash against the local file's hash
        function getInfo(localPath, mantaPath, stats, md5, callback) {
                self.client.info(mantaPath, function onInfo(err, info) {
                        if (err && err.statusCode !== 404) {
                                self.log.error(err);
                                cb(err);
                                return;
                        }

                        if (info && info.md5 === md5) {
                                self.log.info(
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
                self.client.mkdirp(mantaDir, function onMkdirp(err) {
                        if (err) {
                                self.log.error(err);
                                cb(err);
                                return;
                        }
                        self.log.info('Directory ' + mantaDir + ' created');
                        put(localPath, mantaPath, stats, callback);
                });
        }

        // put the asset in manta
        function put(localPath, mantaPath, stats, callback) {
                var opts = {
                        copies: 2,
                        size: stats.size
                };
                var stream = mod_fs.createReadStream(localPath);

                self.log.info('Uploading asset ' + mantaPath);

                stream.pause();
                stream.on('open', function onOpen() {
                        self.client.put(mantaPath, stream, opts,
                                function onPut(err) {

                                if (err) {
                                        self.log.error(err);
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

        self.log.info('Assets to upload', Object.keys(assetList));

        // upload each asset in series
        queue = mod_vasync.queue(upload, 1);

        queue.drain = function uploadDrain() {
                // all files done
                self.log.info('Finished uploading assets.');
                cb();
        };

        Object.keys(assetList).forEach(function (asset) {
                queue.push(asset);
        });

};


/*
 * creates the job described by jobManifest
 *
 * - jobManifest: job manifest object (see marlin docs)
 * - cb: callback in the form f(err, jobPath)
 */
JobRunner.prototype.createJob = function createJob(jobManifest, cb) {
        mod_assert.object(jobManifest, 'jobManifest');
        mod_assert.func(cb, 'callback');

        var self = this;

        self.log.info('Begin create job');

        self.client.createJob(jobManifest, function onCreateJob(err, jobPath) {
                if (err) {
                        self.log.error(err);
                } else {
                        self.log.info('Job created. jobPath: ' + jobPath);
                }
                cb(err, jobPath);
        });
};


/*
 * adds Manta keys generated by the keygen to the job input
 * keys that fail to be added are logged and skipped - the job will run with
 * any keys that were successfully added
 *
 * - keygen: event emitter that emits ('key', key) for each key and ('end')
 *   when finished, and begins emitting keys when start() is called.
 * - jobPath: job path
 * - cb: callback in the form f(err, jobPath, count), where count is the number
 *   of input keys successfully added to the job
 */
JobRunner.prototype.addInputKeys = function addInputKeys(keygen, jobPath, cb) {
        mod_assert.object(keygen, 'keygen');
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        var barrier = mod_vasync.barrier();
        var errors = [];
        var count = 0;

        function onKey(key) {
                self.log.info('Adding key ' + key + ' to job ' + jobPath);
                barrier.start('key' + key);
                self.client.addJobKey(jobPath, key, function onAddKey(err, _) {
                        if (err) {
                                self.log.error(err);
                                errors.push(err);
                                barrier.done('key' + key);
                                return;
                        }
                        count++;
                        barrier.done('key' + key);
                });
        }

        self.log.info('Begin adding job input keys for job ' + jobPath);

        barrier.once('drain', function onDrain() {
                // all keys added
                keygen.removeListener('key', onKey);

                if (count <= 0) {
                        var errMsg = 'No input keys added to job ' + jobPath;
                        self.log.error(errMsg);
                        cb(errMsg, jobPath, count);
                        return;
                }

                self.log.info('Added ' + count + ' keys to job ' + jobPath);

                if (errors.length > 0) {
                        cb(errors, jobPath, count);
                } else {
                        cb(null, jobPath, count);
                }
        });

        barrier.start('addkeys');

        keygen.on('key', onKey);

        keygen.once('end', function onEnd() {
                barrier.done('addkeys');
        });

        keygen.once('error', function onError(err) {
                errors.push(err);
                barrier.done('addkeys');
        });

        keygen.start();
};


/*
 * ends input for the job
 *
 * - jobPath: job path
 * - cb: callback in the form f(err, jobPath)
 */
JobRunner.prototype.endJobInput = function endJobInput(jobPath, cb) {
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        self.log.info('Ending input for job ' + jobPath);

        self.client.endJob(jobPath, function onEndJob(err, _) {
                if (err) {
                        self.log.error(err);
                } else {
                        self.log.info('Job input for ' + jobPath + ' ended.');
                }
                cb(err, jobPath);
        });
};


/*
 * monitors job for done state
 *
 * - jobPath: job path
 * - cb: callback in the form f(err, jobPath)
 */
JobRunner.prototype.monitorJob = function monitorJob(jobPath, cb) {
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        var monitor = mod_backoff.exponential({
                initialDelay: self.monitorBackoff.initialDelay,
                maxDelay: self.monitorBackoff.maxDelay
        });

        self.log.info('Monitoring job ' + jobPath);

        monitor.failAfter(self.monitorBackoff.failAfter);

        monitor.on('ready', function onReady(attempts, delayms) {
                self.log.info('Checking job ' + jobPath + ' . Attempt ' +
                        attempts + '. Next retry in ' + delayms + 'ms');

                self.client.job(jobPath, function onJob(err, job) {
                        if (err) {
                                self.log.warn(err);
                                monitor.backoff();
                                return;
                        }

                        if (job.state !== 'done') {
                                monitor.backoff();
                                return;
                        }

                        self.log.info('Job ' + jobPath + ' done.');
                        monitor.reset(); // stop monitoring
                        monitor.removeAllListeners('fail');
                        monitor.removeAllListeners('ready');
                        cb(null, jobPath);
                });

        });

        monitor.on('fail', function onFail() {
                var errMsg = 'Monitoring exceeded retry limit for for job '
                        + jobPath;
                self.log.error(errMsg);
                cb(errMsg, jobPath);
        });

        monitor.backoff();
};


/*
 * creates a list of output keys for the job
 *
 * - jobPath: job path
 * - cb: callback in the form f(err, outputs), where outputs is an array
 */
JobRunner.prototype.getOutput = function getOutput(jobPath, cb) {
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        var outputs = [];
        self.client.jobOutput(jobPath, function onOutput(err, res) {
                if (err) {
                        cb(err); return;
                }

                res.on('key', function (k) {
                        outputs.push(k);
                });

                res.once('end', cb.bind(null, null, outputs));
        });
};


/*
 * creates a list of errors for the job
 *
 * - jobPath: job path
 * - cb: callback in the form f(err, errors), where errors is an array
 */
JobRunner.prototype.getErrors = function getErrors(jobPath, cb) {
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        var errors = [];
        self.client.jobErrors(jobPath, function (err, res) {
                if (err) { cb(err); }

                res.on('err', function (e) {
                        errors.push(e);
                });

                res.once('end', cb.bind(null, null, errors));
        });
};


/*
 * creates a list of failures for the job
 *
 * - jobPath: job path
 * - cb: callback in the form f(err, failures), where failures is an array
 */
JobRunner.prototype.getFailures = function getFailures(jobPath, cb) {
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        var failures = [];
        self.client.jobFailures(jobPath, function (err, res) {
                if (err) { cb(err); }

                res.on('key', function (k) {
                        failures.push(k);
                });

                res.once('end', cb.bind(null, null, failures));
        });
};


/*
 * collects the results (outputs, errors, and failures) of the job
 *
 * - jobPath: job path
 * - cb: callback in the form f(err, results), where results is in the form
 *      result: {
 *              'jobPath': jobPath,
 *              'outputs': [...],
 *              'errors': [...],
 *              'failures': [...]
 *      }
 */
JobRunner.prototype.getResults = function getResults(jobPath, cb) {
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var barrier = mod_vasync.barrier();
        var result = {jobPath: jobPath};

        var self = this;

        barrier.on('drain', function () {
                self.log.info('Results fetched for job ' + jobPath);
                self.log.info(result);
                cb(null, result);
        });

        barrier.start('getOutput');
        barrier.start('getErrors');
        barrier.start('getFailures');

        self.getOutput(jobPath, function (err, outputs) {
                if (err) {
                        cb(err); return;
                }
                result.outputs = outputs;
                barrier.done('getOutput');
        });
        self.getErrors(jobPath, function (err, errors) {
                if (err) {
                        cb(err); return;
                }
                result.errors = errors;
                barrier.done('getErrors');
        });
        self.getFailures(jobPath, function (err, failures) {
                if (err) {
                        cb(err); return;
                }
                result.failures = failures;
                barrier.done('getFailures');
        });
};


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
JobRunner.prototype.doJob = function doJob(jobManifest, keygen, cb) {
        mod_assert.object(jobManifest, 'jobManifest');
        mod_assert.object(keygen, 'keygen');
        mod_assert.func(cb, 'callback');

        var self = this;

        function _uploadAssets() {
                self.uploadAssets(jobManifest, function (err, _) {
                        if (err) {
                                cb(err); return;
                        }
                        _createJob();
                });
        }

        function _createJob() {
                self.createJob(jobManifest, function (err, jobPath) {
                        if (err) {
                                cb(err); return;
                        }
                        _addInputKeys(jobPath);
                });
        }

        function _addInputKeys(jobPath) {
                self.addInputKeys(keygen, jobPath, function (err, _, count) {
                        if (err) {
                                self.log.error('Error adding key(s)', err);
                        }

                        if (count <= 0) {
                                self.endJobInput(jobPath, function (err2) {
                                        if (err2) {
                                                cb(err2); return;
                                        }
                                        self.log.error('Not monitoring job.');
                                        cb(err);
                                        return;
                                });
                        } else {
                                _endJobInput(jobPath);
                        }
                });
        }

        function _endJobInput(jobPath) {
                self.endJobInput(jobPath, function (err) {
                        if (err) {
                                cb(err); return;
                        }
                        _monitorJob(jobPath);
                });
        }

        function _monitorJob(jobPath) {
                self.monitorJob(jobPath, function (err) {
                        if (err) {
                                cb(err); return;
                        }
                        _getResults(jobPath);
                });
        }

        function _getResults(jobPath) {
                self.getResults(jobPath, function (err, results) {
                        if (err) {
                                cb(err); return;
                        }
                        cb(null, results);
                });
        }

        _uploadAssets();
};


/*
 * Runs the job and return an error if it does not succeed
 *
 * Success is defined as the job completing and having:
 * (1) non-zero output keys,
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
JobRunner.prototype.doJobError = function doJobError(jobManifest, keygen, cb) {
        mod_assert.object(jobManifest, 'jobManifest');
        mod_assert.object(keygen, 'keygen');
        mod_assert.func(cb, 'callback');

        var self = this;

        self.doJob(jobManifest, keygen, function (err, res) {
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
                        self.log.error('Failures or errors for job '
                                + res.jobPath);
                        errors = {
                                jobPath: res.jobPath,
                                errors: res.errors,
                                failures: res.failures
                        };
                        cb(errors);
                        return;
                }

                if (noOutput) {
                        self.log.error('No output for job '
                                + res.jobPath);
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
};


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
JobRunner.prototype.doJobWithRetry =
        function doJobWithRetry(jobManifest, keygen, cb) {

        mod_assert.object(jobManifest, 'jobManifest');
        mod_assert.object(keygen, 'keygen');
        mod_assert.func(cb, 'callback');

        var self = this;

        var retry = mod_backoff.call(self.doJobError.bind(self),
                jobManifest, keygen, function onFinish(err, res) {

                if (err) {
                        self.log.error('Retry limit exceeded for job ' +
                                jobManifest.name);
                        cb(err);
                        return;
                }

                self.log.info('Job successful');
                cb(err, res);
        });

        retry.setStrategy(new mod_backoff.ExponentialStrategy({
                initialDelay: self.retryBackoff.initialDelay,
                maxDelay: self.retryBackoff.maxDelay
        }));

        retry.failAfter(self.retryBackoff.failAfter);

        retry.on('backoff', function onBackoff(attempts, delayms) {
                self.log.info('(retry) Job ' + jobManifest.name + ' attempt ' +
                        attempts + '. Retry in ' + delayms + 'ms.');
        });
};
