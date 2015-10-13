/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_fs = require('fs');
var mod_child_process = require('child_process');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var helper = require('./helper.js');

var storagereduce3 =
        mod_path.resolve(__dirname, '../assets/lib/storage-reduce3.js');

var log = new mod_bunyan({
        'name': 'storagereduce3.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;

function runTest(opts, cb) {
        opts.opts = opts.opts || [];
        var env = {
                env: {
                        'NAMESPACES': 'stor public jobs reports'
                }
        };
        var spawn = mod_child_process.spawn(storagereduce3, opts.opts, env);


        var stdout = '';
        var stderr = '';
        var error;

        spawn.stdout.on('data', function (data) {
                stdout += data;
        });

        spawn.stderr.on('data', function (data) {
                stderr += data;
        });

        spawn.on('error', function (err) {
                error = err;
        });

        spawn.stdin.on('error', function (err) {
                error = err;
        });

        spawn.on('close', function (code) {
                var result = {
                        stdin: opts.stdin,
                        stdout: stdout,
                        stderr: stderr,
                        code: code,
                        error: error
                };
                if (opts.debug) {
                        console.log(result);
                }
                cb(result);
        });

        process.nextTick(function () {
                spawn.stdin.write(opts.stdin || '');
                spawn.stdin.end();
        });
}

/* BEGIN JSSTYLED */
var RECORD1 = {
      "owner": 'fred',
      "namespace": 'stor',
      "directories": 5,
      "keys": 6,
      "objects": 6,
      "bytes": "123"
};

var RECORD2 = {
      "owner": 'fred',
      "namespace": 'public',
      "directories": 3142,
      "keys": 138957,
      "objects": 139807,
      "bytes": "123408435"
};

var RECORD3 = {
      "owner": "fred",
      "namespace": "reports",
      "directories": 0,
      "keys": 0,
      "objects": 0,
      "bytes": "0"
};

var RECORD4 = {
      "owner": "fred",
      "namespace": 'jobs',
      "directories": 58457,
      "keys": 39584698,
      "objects": 345789427646,
      "bytes": "215474598758118347783434"
};

var EXPECTED = {
        "owner": 'fred',
        "storage": {
                "stor": {
                      "directories": 5,
                      "keys": 6,
                      "objects": 6,
                      "bytes": "123"
                },
                "public": {
                      "directories": 3142,
                      "keys": 138957,
                      "objects": 139807,
                      "bytes": "123408435"
                },
                "reports": {
                      "directories": 0,
                      "keys": 0,
                      "objects": 0,
                      "bytes": "0"
                },
                "jobs": {
                      "directories": 58457,
                      "keys": 39584698,
                      "objects": 345789427646,
                      "bytes": "215474598758118347783434"
                }
        }
};
/* END JSSTYLED */

test('basic', function (t) {
        var input = JSON.stringify(RECORD1) + '\n' +
                    JSON.stringify(RECORD2) + '\n' +
                    JSON.stringify(RECORD3) + '\n' +
                    JSON.stringify(RECORD4);
        runTest({
                stdin: input
        }, function (result) {
                t.deepEqual(JSON.parse(result.stdout), EXPECTED);
                t.done();
        });
});


test('missing namespace', function (t) {
        var input = JSON.stringify(RECORD1) + '\n' +
                    JSON.stringify(RECORD2) + '\n' +
                    JSON.stringify(RECORD4);
        runTest({
                stdin: input
        }, function (result) {
                t.deepEqual(JSON.parse(result.stdout), EXPECTED);
                t.done();
        });
});
