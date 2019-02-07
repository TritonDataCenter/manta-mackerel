/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */



/*
 * The number of concurrent upstream connections should be less than the maximum
 * number that Manta allows from a single task before queueing them.  This is
 * part of Marlin's configuration, but it's not exposed to user tasks, so we
 * hardcode the default number here.  A better approach would be to have the
 * server issue a 429 "Too Many Requests" response (instead of queueing them)
 * and have the client back off when this happens.
 */
var msConcurrency = 25;

/*
 * Configure maxSockets based on our desired concurrency.  We have to do this
 * here, before pulling in "restify-clients" (via "manta"), because
 * "restify-clients" reads these from the top-level.
 */
var mod_http = require('http');
mod_http.globalAgent.maxSockets = msConcurrency;

var mod_manta = require('manta');
var mod_vasync = require('vasync');
var mod_retry = require('retry');
var mod_path = require('path');
var mod_fs = require('fs');

function createMantaDirectory(client, dir, cb) {
        client.mkdirp(dir, function (err) {
                // Do not treat this as an error if the directory
                // already exists.
                if (err && err.name != 'DirectoryExistsError') {
                        cb(err);
                        return;
                }
                cb();
        });
}

function doUploadReducerStream(args, cb) {
        var options = {
                size: args.size,
                // Manta recognize those headers and adds the uploaded
                // object as input to the next phase reducer.
                headers:  {
                        'x-manta-stream': 'stdout',
                        'x-manta-reducer': args.idx
                }
        };

        var client = args.client;
        var objectName = args.objectName;
        var istream = args.istream;

        client.put(objectName, istream, options, function (err) {
                cb(err);
        });
}

function doUploadReducerFile(args, cb) {
        var client = args.client;
        var dir = mod_path.dirname(args.objectName);
        var fileName = args.fileName;

        // Create the directory in manta if it doesn't already exist.
        createMantaDirectory(client, dir, function (err1) {
                if (err1) {
                        cb(err1);
                        return;
                }

                mod_fs.stat(fileName, function (err2, stat) {
                        if (err2) {
                                cb(err2);
                                return;
                        }
                        args.size = stat.size;
                        var istream = mod_fs.createReadStream(fileName);

                        istream.on('error', function (error) {
                                cb(error);
                        });
                        // When the file is opened, upload the
                        // stream to manta.
                        istream.on('open', function () {
                                args.istream = istream;
                                doUploadReducerStream(args, cb);
                        });
                });
        });
}

function uploadReducerFile(args, cb) {
        var operation = mod_retry.operation({
                'retries': 2,
                'factor': 2,
                'minTimeout': 1000,
                'maxTimeout': 3000
        });


        /*
         * A failure in doUploadReducerFile() - or any other function it
         * calls - is not fatal to the process. We retry uploading reducer
         * files two times before we bail out.
         */
        operation.attempt(function (_currentAttempt) {
                doUploadReducerFile(args, function (err) {
                        // Retry uploading the file when possible.
                        if (operation.retry(err)) {
                                return;
                        }
                        cb(err ? operation.mainError() : null);
                });
        });
}

function MUploader(log) {
        this._log = log;
}

MUploader.prototype.uploadReducerFiles = function (fileNames, objectNames, cb) {
        var client = mod_manta.createBinClient({
                'log': this._log
        });

        var queue = mod_vasync.queuev({
            'concurrency': msConcurrency,
            'worker': function (args, qcb) {
                uploadReducerFile(args, function (err) {
                        // Failed to upload the file to manta.
                        // Kill the other tasks in the queue
                        // and return the error down stack.
                        if (err) {
                                queue.kill();
                                cb(err);
                                return;
                        }

                        // Continue to process the next task
                        // in the queue.
                        qcb();
                });
            }
        });

        fileNames.forEach(function (fileName, idx) {
                queue.push({
                        idx: idx,
                        fileName: fileName,
                        objectName: objectNames[idx],
                        client: client
                });
        });

        queue.drain = function () {
                // Done uploading all the files to manta.
                client.close();
                cb();
        };
};

module.exports = MUploader;
