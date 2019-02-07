/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var mod_fs = require('fs');
var mod_child_process = require('child_process');
var mod_http = require('http');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_vasync = require('vasync');
var mod_zlib = require('zlib');

var mod_bunyan = require('bunyan');
var helper = require('./helper.js');

var storagefanout = mod_path.resolve(__dirname,
        '../assets/lib/storage-fanout.js');

var log = new mod_bunyan({
        'name': 'storagefanout.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;
var before = helper.before;
var after = helper.after;


var PORT = 9876;
var SERVER = null;
var MANTA_URL = 'http://localhost:' + PORT;
var MANTA_OUTPUT_BASE = '/MANTA_USER/jobs/jobid/stor/reduce.1.';
var LOOKUP_FILE = '../../test/test_data/lookup.json';
var LOOKUP = require('./test_data/lookup.json');

var lineString = Array(910).join('-');
function generateLines(numberOfLines) {
        var ret = '';
        for (var n = 0; n <= numberOfLines; n++) {
                var prefix  = (n === 0) ? '0000000' : '' + (1000000 + n);
                ret += prefix + ' ' + lineString + '\n';
        }
        return (ret);
}

function runTest(opts, cb) {
        opts.opts = opts.opts || [];
        opts.env = opts.env || {};
        opts.env['LOOKUP_FILE'] = LOOKUP_FILE;
        var spawn = mod_child_process.spawn(storagefanout, opts.opts, opts);

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

before(function (cb) {
        SERVER = mod_http.createServer(function (req, res) {
                var body = new Buffer(0);
                req.on('data', function (data) {
                        body = Buffer.concat([body, data]);
                });
                req.on('end', function () {
                        req.body = body;
                        SERVER.requests.push(req);
                        res.writeHead(204);
                        res.end();
                });
        });

        SERVER.listen(PORT, function (err) {
                cb(err);
        });

        SERVER.requests = [];
});

after(function (cb) {
        SERVER.close(function (err) {
                SERVER = null;
                cb();
        });
});

test('output is being sent to stdout', function (t) {
        t.expect(3);
        var lines = '1\n2\n3\n4\n5';

        runTest({
                stdin: lines
        }, function (result) {
                t.ok(result.code === 0);
                t.ok(result.stdout === lines);
                t.ok(result.stderr === '');
                t.done();
        });
});


test('fanout test', function (t) {
        t.expect(5);

        var input = generateLines(100000);
        var nReducers = 5;
        runTest({
                stdin: input,
                env : {
                        'MANTA_URL': MANTA_URL,
                        'MANTA_OUTPUT_BASE': MANTA_OUTPUT_BASE,
                        'MANTA_NO_AUTH': true
                },
                opts: ['-u', '-n', '' + nReducers]
        }, function (result) {
                t.ok(result.code === 0);
                t.ok(result.stdout === '');
                t.ok(result.stderr === '');
                t.ok(SERVER.requests.length == nReducers);
                var r = SERVER.requests.map(function (req) {
                        return (req.body);
                });

                mod_vasync.forEachParallel({
                        'func': mod_zlib.gunzip,
                        'inputs': r
                }, function (err, results) {
                        var out = results.successes.map(function (res) {
                                return (res.toString());
                        });

                        out = out.join('').slice(0, -1)
                                 .split('\n').sort().join('\n');
                        var expectedOutput = input;
                        for (var i = 0; i < nReducers - 1; i++) {
                                expectedOutput = input.split('\n')[0] + '\n' +
                                        expectedOutput;
                        }

                        t.ok(out === expectedOutput.slice(0, -1));
                        t.done();
                });
        });
});
