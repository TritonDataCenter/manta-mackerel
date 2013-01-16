#!/usr/bin/env node
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var mod_assert = require('assert-plus');
var mod_backoff = require('backoff');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_vasync = require('vasync');

function JobRunner(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.optionalObject(opts.backoffStrategy, 'opts.backoffStrategy');
        mod_assert.object(opts.assets, 'opts.assets');
        mod_assert.object(opts.client, 'opts.client');
        mod_assert.object(opts.log, 'opts.log');

        this.backoffStrategy = opts.backoffStrategy || {
                initialDelay: 1000,
                maxDelay: 120000,
                failAfter: 20
        };

        this.assets = opts.assets;
        this.client = opts.client;
        this.log = opts.log;
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
        var errors = [];
        var queue, p, a;

        // find all assets listed in the job's phases
        for (p in jobManifest.phases) {
                for (a in jobManifest.phases[p].assets) {
                        assetList[jobManifest.phases[p].assets[a]] = true;
                }
        }

        self.log.info('assets to upload', Object.keys(assetList));

        // upload each asset in series
        queue = mod_vasync.queue(upload, 1);
        Object.keys(assetList).forEach(function (asset) {
                queue.push(asset, finishUpload);
        });

        // create the necessary directories and upload an object
        function upload(mantaPath, callback) {
                // look up the asset's local file path
                var localPath = self.assets[mantaPath];

                if (!localPath) {
                        var errMsg = 'Local file for asset ' + mantaPath +
                                ' not found.';
                        errors.push(errMsg);
                        self.log.warn(errMsg);
                        callback();
                        return;
                }

                self.log.info('Uploading asset', mantaPath, localPath);

                // create the intermediate directories, then upload
                self.client.mkdirp(mod_path.dirname(mantaPath), stat);

                function stat(err) {
                        if (err) {
                                errors.push(err);
                                self.log.warn(err);
                                callback();
                                return;
                        }
                        mod_fs.stat(localPath, function (err2, stats) {
                                //TODO md5, and only upload if modified
                                if (err2) {
                                        errors.push(err2);
                                        self.log.warn(err2);
                                        callback();
                                        return;
                                }

                                if (!stats.isFile()) {
                                        errors.push('Not a file');
                                        self.log.warn('Not a file');
                                        callback();
                                        return;
                                }

                                var fstream;
                                var opts = {
                                        copies: 2,
                                        size: stats.size
                                };

                                // upload
                                fstream = mod_fs.createReadStream(localPath);
                                fstream.pause();
                                fstream.on('open', function () {
                                        put(mantaPath, fstream, opts, callback);
                                });
                        });
                }

                function put(path, stream, opts, func) {
                        self.client.put(path, stream, opts, function (err) {
                                if (err) {
                                        errors.push(err);
                                        self.log.warn(err);
                                }
                                func();
                        });
                }
        }

        // called every time a file is finished uploading
        function finishUpload() {
                if (queue.queued.length === 0) {
                        // all files done
                        if (errors.length > 0) {
                                cb(errors);
                        } else {
                                cb();
                        }
                }
        }
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

        self.log.info('begin create job');

        self.client.createJob(jobManifest, function (err, jobPath) {
                if (err) {
                        cb(err);
                } else {
                        cb(null, jobPath);
                }
        });
};


/*
 * adds keys generated by the keygen to the job
 *
 * - keygen: event emitter that emits ('key', key) for each key and ('end')
 *   when finished, and begins emitting keys when start() is called.
 * - jobPath: job path
 * - cb: callback in the form f(err)
 */
JobRunner.prototype.addKeys = function addKeys(keygen, jobPath, cb) {
        mod_assert.object(keygen, 'keygen');
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        var barrier = mod_vasync.barrier();
        var errors = [];

        self.log.info('begin adding keys');

        barrier.once('drain', function () {
                // all keys added
                if (errors.length > 0) {
                        cb(errors);
                } else {
                        cb();
                }
        });

        barrier.start('addkeys');

        keygen.on('key', function (key) {
                self.log.info('adding key', key);
                barrier.start('key' + key);
                self.client.addJobKey(jobPath, key, function (err, _) {
                        if (err) {
                                errors.push(err);
                        }
                        barrier.done('key' + key);
                });
        });

        keygen.once('end', function () {
                barrier.done('addkeys');
        });

        keygen.on('error', function (err) {
                cb(err);
        });

        keygen.start();
};


/*
 * ends input for the job
 *
 * - jobPath: job path
 * - cb: callback in the form f(err, jobPath)
 */
JobRunner.prototype.endJob = function endJob(jobPath, cb) {
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        self.client.endJob(jobPath, cb.bind(null));
};


/*
 * monitors job for done state
 *
 * - jobPath: job path
 * - cb: callback in the form f(err)
 */
JobRunner.prototype.monitorJob = function monitorJob(jobPath, cb) {
        mod_assert.string(jobPath, 'jobPath');
        mod_assert.func(cb, 'callback');

        var self = this;

        var retry = mod_backoff.exponential(self.backoffStrategy);

        self.log.info('monitoring job ' + jobPath);

        retry.failAfter(self.backoffStrategy.failAfter);

        retry.on('ready', function (attempts, delayms) {
                self.log.info('Checking job ' + jobPath + ' . Attempt ' +
                        attempts + '. Next retry in ' + delayms + 'ms');

                self.client.job(jobPath, function (err, job) {
                        if (job.state === 'done') {
                                retry.reset(); // stop retrying
                                cb();
                        } else {
                                retry.backoff();
                        }
                });

        });

        retry.on('fail', function () {
                cb('Exceeded retry limit for output for job', jobPath);
        });

        retry.backoff();
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
        self.client.jobOutput(jobPath, function (err, res) {
                if (err) {
                        cb(err); return;
                }

                res.on('key', function (k) {
                        outputs.push(k);
                });

                res.on('end', cb.bind(null, false, outputs));
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

                res.on('end', cb.bind(null, false, errors));
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

                res.on('end', cb.bind(null, false, failures));
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
 *              outputs: [...],
 *              errors: [...],
 *              failures: [...]
 *      }
 */
JobRunner.prototype.doJob = function doJob(jobManifest, keygen, cb) {
        var self = this;

        function _uploadAssets() {
                self.uploadAssets(jobManifest, function (err) {
                        if (err) {
                                cb(err); return;
                        }
                        self.log.info('finished uploading assets');
                        _createJob();
                });
        }

        function _createJob() {
                self.createJob(jobManifest, function (err, jobPath) {
                        if (err) {
                                cb(err); return;
                        }
                        self.log.info('finished creating job',  jobPath);
                        _addKeys(jobPath);
                });
        }

        function _addKeys(jobPath) {
                self.addKeys(keygen, jobPath, function (err) {
                        if (err) {
                                self.log.warn('Error adding key(s)', err);
                        }
                        self.log.info('finished adding keys');
                        _endJob(jobPath);
                });
        }

        function _endJob(jobPath) {
                self.endJob(jobPath, function (err) {
                        if (err) {
                                cb(err); return;
                        }
                        self.log.info('input for job ' + jobPath + ' ended');
                        _monitorJob(jobPath);
                });
        }

        function _monitorJob(jobPath) {
                self.monitorJob(jobPath, function (err) {
                        if (err) {
                                cb(err); return;
                        }
                        self.log.info('job ' + jobPath + ' complete');
                        _getResults(jobPath);
                });
        }

        function _getResults(jobPath) {
                var barrier = mod_vasync.barrier();
                var result = {jobPath: jobPath};
                barrier.on('drain', function () {
                        self.log.info('results fetched for job ' + jobPath);
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
        }
        _uploadAssets();
};


module.exports.JobRunner = JobRunner;
