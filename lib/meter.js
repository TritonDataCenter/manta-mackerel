// Copyright (c) 2013, Joyent, Inc. All rights reserved.

/*
 * dateStringFormat(str date)
 * getJobManifest(jobConfig, date, log)
 * getKeygen(args, client, date, log)
 *
 * getJSONObject(path, client, log)
 * writeJSONObject(path, obj, client, log)
 * getOrCreateRecord(path, client, jobConfig, date, log)
 * updateRecord(
 * removeRecord
 * getJobFailureHistory
 *
 * meter
 * backfill
 */

var mod_assert = require('assert-plus');
var mod_backoff = require('backoff');
var mod_keygen; // retrieved once job config is loaded
var mod_MemoryStream = require('memorystream');
var mod_redis = require('./redis');
var mod_vasync = require('vasync');

module.exports = {
        configureJobManifest: configureJobManifest,
        meter: meter,
        recordResult: recordResult,
        backfill: backfill,
        generateLookup: generateLookup
};


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
 * Configures the job manifest to include any environment variables and
 * date string formatting.
 *
 * - jobConfig: unconfigured job manifest
 * - date: javascript Date object
 * - log: logger
 */
function configureJobManifest(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.jobConfig, 'opts.jobConfig');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.object(opts.log, 'opts.log');

        var jobConfig = opts.jobConfig;
        var jobManifest = jobConfig.job;
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
 * Configures the job input key generator to find the correct input keys given
 * the date and source path.
 *
 * - args: any additional arguments that should be passed to the key generator
 *   besides the manta client and a logger. 'date' and 'source' fields are
 *   automatically set.
 * - client: manta client
 * - date: javascript Date object
 * - log: logger
 */
function getKeygen(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.args, 'opts.args');
        mod_assert.object(opts.client, 'opts.client');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.object(opts.keygenModule, 'opts.keygenModule');
        mod_assert.object(opts.log, 'opts.log');

        var args = opts.args;
        var client = opts.client;
        var date = opts.date;
        var log = opts.log;
        var keygenModule = opts.keygenModule;

        args.date = date;
        args.source = dateStringFormat(args.source, date);
        var keygen = keygenModule.keygen({
                client: client,
                log: log,
                args: args
        });

        mod_assert.func(keygen.start, 'Keygen must have a start method.');

        return (keygen);
}


/*
 * Gets the record at the specified path and parses it as a JSON object, parsing
 * the date field into a javascript Date object
 *
 * - path: Manta object path
 * - client: Manta client
 * - log: logger
 * - cb: callback in the form f(err, obj) where obj is a javascript object
 *   parsed from the Manta object
 */
function getJSONObject(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.path);
        mod_assert.object(opts.client);
        mod_assert.object(opts.log);
        mod_assert.func(cb);

        var path = opts.path;
        var client = opts.client;
        var log = opts.log;

        client.get(path, function onGet(err, stream) {
                if (err) {
                        cb(err);
                        return;
                }

                var string = '';
                stream.setEncoding('utf8');

                stream.on('data', function onData(chunk) {
                        string += chunk;
                });

                stream.once('end', function onEnd() {
                        log.info(path + ' downloaded');
                        var obj = JSON.parse(string);
                        obj.date = new Date(obj.date);
                        cb(null, obj);
                });

        });
}


/*
 * Writes a javascript object as JSON as a Manta object at the given path
 *
 * - path: Manta object path
 * - obj: javascript object to stringify
 * - client: Manta client
 * - log: logger
 * - cb: callback in the form f(err)
 */
function writeJSONObject(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.path);
        mod_assert.object(opts.obj);
        mod_assert.object(opts.client);
        mod_assert.object(opts.log);
        mod_assert.func(cb);

        var path = opts.path;
        var obj = opts.obj;
        var client = opts.client;
        var log = opts.log;

        var string = JSON.stringify(obj);
        var size = Buffer.byteLength(string);
        var stream = new mod_MemoryStream();

        client.put(path, stream, {size: size}, function onPut(err) {
                if (err) {
                        cb(err);
                        log.info(path + ' written.');
                        return;
                }
        });

        process.nextTick(function onNextTick() {
                stream.write(string);
                stream.end();
        });
}


/*
 * Looks for an existing failed job record at the given path, and returns the
 * record if found, or a new record with the given job config and date if it is
 * not found.
 *
 * - path: Manta object path
 * - client: Manta client
 * - jobConfig: job configuration object
 * - date: the date the job is supposed to meter (NOT when the job ran)
 * - log: logger
 * - cb: callback in the form f(err, record), where record is in the form
 *      record: {
 *              jobConfig: {...},
 *              date: date,
 *              results: [...]
 *      }
 */
function getOrCreateRecord(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.path);
        mod_assert.object(opts.client);
        mod_assert.object(opts.jobConfig);
        mod_assert.object(opts.date);
        mod_assert.object(opts.log);
        mod_assert.func(cb);

        var path = opts.path;
        var client = opts.client;
        var jobConfig = opts.jobConfig;
        var date = opts.date;
        var log = opts.log;
        var record;

        client.info(path, function onInfo(err, info) {
                if (err && err.statusCode !== 404) {
                        log.error(err);
                        cb(err);
                        return;
                } else if (err && err.statusCode === 404) {
                        log.info(path + ' not found. Creating new record.');
                        // create new object
                        record = {
                                jobConfig: jobConfig,
                                date: date,
                                results: []
                        };
                        cb(null, record);
                } else {
                        // update existing object
                        getJSONObject({
                                path: path,
                                client: client,
                                log: log
                        }, function onGet(err2, obj) {
                                if (err2) {
                                        cb(err2);
                                        return;
                                }

                                log.info('Downloaded ' + path);
                                record = obj;
                                cb(null, record);
                        });
                }
        });
}


/*
 * Updates or creates a failed job record for the given job
 * A failed job record contains the configuration details for the job, the date
 * the job was supposed to meter over, and an array of results from past
 * failures. To re-run a failed job, use the jobConfig and date from the record.
 *
 * record: {
 *      jobConfig: {...},
 *      date: date,
 *      results: [...]
 * }
 *
 * - backfillPath: Manta directory path for failed job records
 * - client: Manta client
 * - jobConfig: job configuration for the failed job
 * - date: the date the job is supposed to meter as a Date object (NOT when the
 *   job ran)
 * - results: results from the failed job
 * - log: logger
 * - cb: callback in the form f(err)
 */
function updateRecord(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.backfillPath);
        mod_assert.object(opts.client);
        mod_assert.object(opts.jobConfig);
        mod_assert.object(opts.date);
        mod_assert.ok(opts.results);
        mod_assert.object(opts.log);

        var dir = opts.backfillPath;
        var client = opts.client;
        var jobConfig = opts.jobConfig;
        var date = opts.date;
        var results = opts.results;
        var log = opts.log;

        var recordPath = dir + '/' + dateStringFormat(jobConfig.job.name, date);

        log.info('Updating record ' + recordPath);

        getOrCreateRecord({
                path: recordPath,
                client: client,
                jobConfig: jobConfig,
                date: date,
                log: log
        }, function onRecord(err, record) {
                if (err) {
                        cb(err);
                        return;
                }

                record.results.push(results);

                log.info('Updated record: ', record);

                writeJSONObject({
                        path: recordPath,
                        client: client,
                        obj: record,
                        log: log
                }, function onWrite(err2) {
                        if (err2) {
                                cb(err2);
                                return;
                        }
                        cb();
                });
        });
}


function removeRecord(opts, cb) {
        mod_assert.object(opts);
        mod_assert.string(opts.path);
        mod_assert.object(opts.client);
        mod_assert.object(opts.log);

        var path = opts.path;
        var client = opts.client;
        var log = opts.log;

        client.unlink(path, function onUnlink(err) {
                if (err && err.statusCode !== 404) {
                        cb(err);
                        return;
                }
                if (!err) {
                        log.info('Removed ' + path);
                }
                cb();
        });

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


function getFailureHistory(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.client, 'opts.client');
        mod_assert.string(opts.backfillPath, 'opts.backfillPath');
        mod_assert.object(opts.log, 'opts.log');

        var client = opts.client;
        var backfillPath = opts.backfillPath;
        var log = opts.log;

        var jobs = [];
        var barrier = mod_vasync.barrier();
        barrier.on('drain', function onDrain() {
                cb(null, jobs);
        });

        barrier.start('ls');

        log.info('Searching ' + backfillPath + ' for failed job records.');
        client.ls(backfillPath, function onLs(err, res) {
                if (err) {
                        log.error(err);
                        cb(err);
                        return;
                }

                res.on('object', function onObject(obj) {
                        var recordPath = backfillPath + '/' + obj.name;
                        barrier.start(recordPath);
                        getJSONObject({
                                path: recordPath,
                                client: client,
                                log: log
                        }, function onGet(err2, record) {
                                if (err2) {
                                        cb(err2);
                                        return;
                                }
                                jobs.push(record);
                                barrier.done(recordPath);
                        });
                });

                res.once('error', cb.bind(null));

                res.once('end', function onEnd() {
                        barrier.done('ls');
                });
        });
}


/*
 * jobConfig.job is modified
 * API: cb(err, outputs)
 */
function meter(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.date, 'opts.date');
        mod_assert.object(opts.jobConfig, 'opts.jobConfig');
        mod_assert.object(opts.log, 'opts.log');
        mod_assert.object(opts.client, 'opts.client');
        mod_assert.object(opts.runner, 'opts.runner');

        var date = opts.date;
        var jobConfig = opts.jobConfig;
        var log = opts.log;
        var client = opts.client;
        var runner = opts.runner;
        var keygenModule = require(jobConfig.keygen);
        var keygen;

        assertJobConfig(jobConfig);

        // generates job input keys
        keygen = getKeygen({
                args: jobConfig.keygenArgs,
                client: client,
                date: date,
                keygenModule: keygenModule,
                log: log
        });

        runner.doJobWithRetry(jobConfig.job, keygen,
                function jobDone(err, outputs) {

                if (err) {
                        cb(err);
                        return;
                }
                cb(err, outputs);
        });
}


function recordResult(opts, cb) {
        var client = opts.client;
        var backfillPath = opts.backfillPath;
        var jobConfig = opts.jobConfig;
        var date = opts.date;
        var results = opts.results;
        var errors = opts.errors;
        var log = opts.log;

        if (errors) {
                updateRecord({
                        client: client,
                        backfillPath: backfillPath,
                        jobConfig: jobConfig,
                        date: date,
                        results: errors,
                        log: log
                }, function (err) {
                        cb(err);
                        return;
                });
        } else {
                removeRecord({
                        client: client,
                        path: backfillPath + '/' + jobConfig.job.name,
                        log: log
                }, function onRemoveRecord(err) {
                        if (err) {
                                cb(err);
                                return;
                        }
                });
        }
}


function backfill(opts, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.client, 'opts.client');
        mod_assert.string(opts.backfillPath, 'opts.backfillPath');
        mod_assert.number(opts.alarmAfter, 'opts.alarmAfter');
        mod_assert.object(opts.runner, 'opts.runner');
        mod_assert.object(opts.log, 'opts.log');

        var client = opts.client;
        var backfillPath = opts.backfillPath;
        var alarmAfter = opts.alarmAfter;
        var runner = opts.runner;
        var log = opts.log;

        var now = Date.now();
        alarmAfter = alarmAfter * 3600000; // turn hours into milliseconds

        getFailureHistory({
                client: client,
                backfillPath: backfillPath,
                log: log
        }, function onGetFailureHistory(err, res) {
                if (err) {
                        log.error(err);
                        return;
                }

                log.info(res.length + ' failed jobs found.');

                var queue = mod_vasync.queue(backfillJob, 5);
                function backfillJob(jobRecord) {
                        var name = jobRecord.jobConfig.job.name;
                        if (now - jobRecord.date.getTime() > alarmAfter) {
                                log.fatal('Job ' + name + '  must be retried' +
                                        ' manually. See ' + backfillPath + '/' +
                                        name + ' for job failure details.');

                                cb('Unable to auto backfill.');
                                return;
                        }

                        meter({
                                date: jobRecord.date,
                                jobConfig: jobRecord.jobConfig,
                                log: log,
                                client: client,
                                runner: runner
                        }, function (err2, res2) {
                                log.info('Recording result.');
                                recordResult({
                                        client: client,
                                        backfillPath: backfillPath,
                                        jobConfig: jobRecord.jobConfig,
                                        date: jobRecord.date,
                                        results: res2,
                                        errors: err2,
                                        log: log
                                }, function onRecord(err3) {
                                        if (err3) {
                                                log.error(err3);
                                                cb(err3);
                                                return;
                                        }
                                });
                        });

                }

                queue.drain = function backfillDrain() {
                        cb();
                };

                queue.push(res);
        });
}


/*
 * Generates the lookup table that maps uuid->login via redis.
 *
 * - opts: redis client config
 * - cb: callback in the form f(result), where result is a javascript object
 *   that maps uuid -> login.
 */
function generateLookup(opts, log, cb) {
        mod_assert.object(opts, 'opts');
        mod_assert.number(opts.port, 'opts.port');
        mod_assert.string(opts.host, 'opts.host');
        mod_assert.optionalObject(opts.options, 'opts.options');
        mod_assert.optionalNumber(opts.maxParallel, 'opts.maxParallel');
        mod_assert.object(log, 'log');
        mod_assert.func(cb, 'cb');

        var result = {};
        var queue = mod_vasync.queue(getLogin, opts.maxParallel || 10);
        var client;

        function getLogin(uuid, callback) {
                client.get('/uuid/' + uuid, function onGet(err, login) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        result[uuid] = login;
                        callback();
                });
        }

        queue.drain = function lookupDrain() {
                client.quit();
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
        }, function onClient(err, c) {
                if (err) {
                        cb(err);
                        return;
                }
                client = c;

                client.on('error', function onError(err2, res) {
                        if (err2) {
                                cb(err2);
                                return;
                        }
                });

                client.smembers('uuid', function onSmembers(err2, res) {
                        if (err2) {
                                cb(err2);
                                return;
                        }
                        queue.push(res);
                });
        });
}
