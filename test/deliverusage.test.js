// Copyright (c) 2013, Joyent, Inc. All rights reserved.
var mod_fs = require('fs');
var mod_child_process = require('child_process');
var mod_path = require('path');
var mod_http = require('http');

var mod_bunyan = require('bunyan');
var helper = require('./helper.js');

var deliverusage =
    mod_path.resolve(__dirname, '../assets/lib/deliver-usage.js');

var log = new mod_bunyan({
        'name': 'deliverusage.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var after = helper.after;
var before = helper.before;
var test = helper.test;

var PORT= 5678;
var SERVER = null;
var MANTA_URL = 'http://localhost:' + PORT;
var LOOKUP_FILE = '../../test/test_data/lookup.json';
var LOOKUP = require('./test_data/lookup.json');
var NAMESPACES = ['stor', 'public', 'jobs', 'reports'];
var COUNTERS = ['directories', 'keys', 'objects', 'bytes'];
var METHODS = ['OPTION', 'GET', 'HEAD', 'POST', 'PUT', 'DELETE'];
var BANDWIDTH = ['in', 'out', 'headerIn', 'headerOut'];

var STORAGE_RECORD = {
        'owner': 'ed5fa8da-fd61-42bb-a24a-515b56c6d581',
        'stor': {
                'directories': 301,
                'keys': 1523,
                'objects': 1523,
                'bytes': '225615978'
        },
        'public': {
                'directories': 0,
                'keys': 0,
                'objects': 0,
                'bytes': '0'
        },
        'jobs': {
                'directories': 0,
                'keys': 1,
                'objects': 1,
                'bytes': '4096'
        },
        'reports': {
                'directories': 0,
                'keys': 0,
                'objects': 0,
                'bytes': '0'
        }
};

var REQUEST_RECORD = {
        'owner': '83081c10-1b9c-44b3-9c5c-36fc2a5218a0',
        'requests': {
                'OPTION': 0,
                'GET': 2,
                'HEAD': 0,
                'POST': 0,
                'PUT': 1,
                'DELETE': 0
        },
        'bandwidth': {
                'in': '6880413',
                'out': '0',
                'headerIn': '2274',
                'headerOut': '714'
        }
};

var COMPUTE_RECORD = {
        'owner': '639aa18f-7aff-4d70-9718-ac75d7cad68f',
        'time': {
                '268435456': 1104,
                '536870912': 60,
                '2147483648': 6
        },
        'bandwidth': {
                'in': '217915',
                'out': '6968'
        }
}


function runTest(opts, cb) {
        var env = {
                env: {
                        'MANTA_URL': MANTA_URL,
                        'MANTA_NO_AUTH': 'true',
                        'USER_DEST': '/reports/usage/2013/06/07/12/h12.json',
                        'LOOKUP_FILE': LOOKUP_FILE
                }
        };

        var spawn = mod_child_process.spawn(deliverusage, opts.opts, env);

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
                var body = '';
                req.on('data', function (data) {
                        body += data;
                });
                req.on('end', function () {
                        req.body = body;
                        SERVER.requests.push(req);
                        res.writeHead(204);
                        res.end();
                });
        }).listen(PORT, function (err) {
                cb(err);
        });
        SERVER.requests = [];
});

after(function (cb) {
        SERVER.close(function (err) {
                SERVER = null;
                cb(err);
        });
});


test('numbers to strings', function (t) {
        runTest({
                stdin: JSON.stringify(STORAGE_RECORD)
        }, function (result) {
                var lines = result.stdout.split('\n');
                var actual = JSON.parse(lines[0]);
                t.equal(result.code, 0);
                t.equal(lines.length, 26);
                t.deepEqual(STORAGE_RECORD, actual);

                // deepEqual does not use strict equality, so check to make sure
                // all the numbers are strings
                NAMESPACES.forEach(function (n) {
                        COUNTERS.forEach(function (c) {
                                t.ok(typeof(actual[n][c]) === 'string');
                        });
                });
                t.done();
        });
});

test('empty record creation (storage)', function (t) {
        runTest({
                stdin: JSON.stringify(STORAGE_RECORD)
        }, function (result) {
                var lines = result.stdout.split('\n');
                var actual = JSON.parse(lines[0]);
                t.equal(result.code, 0);
                t.equal(lines.length, 26);
                t.deepEqual(STORAGE_RECORD, actual);

                for (var i = 1; i < 25; i++) {
                        actual = JSON.parse(lines[i]);
                        NAMESPACES.forEach(function (n) {
                                COUNTERS.forEach(function (c) {
                                       t.strictEqual(actual[n][c], '0');
                                });
                        });
                }
                t.done();
        });
});

test('empty record creation (request)', function (t) {
        runTest({
                stdin: JSON.stringify(REQUEST_RECORD)
        }, function (result) {
                var lines = result.stdout.split('\n');
                var actual = JSON.parse(lines[0]);
                t.equal(result.code, 0);
                t.equal(lines.length, 26);
                t.deepEqual(REQUEST_RECORD, actual);

                for (var i = 1; i < 25; i++) {
                        actual = JSON.parse(lines[i]);
                        METHODS.forEach(function (m) {
                                t.strictEqual(actual.requests[m], '0');
                        });
                        BANDWIDTH.forEach(function (b) {
                                t.strictEqual(actual.bandwidth[b], '0');
                        });
                }
                t.done();
        });
});

test('empty record creation (compute)', function (t) {
        runTest({
                stdin: JSON.stringify(COMPUTE_RECORD)
        }, function (result) {
                var lines = result.stdout.split('\n');
                var actual = JSON.parse(lines[0]);
                t.equal(result.code, 0);
                t.equal(lines.length, 26);
                t.deepEqual(COMPUTE_RECORD, actual);

                for (var i = 1; i < 25; i++) {
                        actual = JSON.parse(lines[i]);
                        t.strictEqual(actual.bandwidth.in, '0');
                        t.strictEqual(actual.bandwidth.out, '0');
                        t.deepEqual(actual.time, {});
                }
                t.done();
        });
});

test('write to user directories', function (t) {
        t.expect(17);
        runTest({
                stdin: JSON.stringify(STORAGE_RECORD)
        }, function (result) {
                var dirs = 0;
                t.equal(result.code, 0);
                t.equal(SERVER.requests.length, 6);
                SERVER.requests.forEach(function (r) {
                        var login = r.url.split('/')[1];
                        var type = r.headers['content-type'];
                        t.equal(r.method, 'PUT');
                        t.equal(login, 'gkevinykchan_work');
                        if (type === 'application/json; type=directory') {
                                dirs++;
                        } else {
                                var body = JSON.parse(r.body);
                                t.ok(typeof(body.owner) === 'undefined');
                                body.owner = STORAGE_RECORD.owner;
                                t.deepEqual(STORAGE_RECORD, body);
                        }
                });
                t.equal(dirs, 5);
                t.done();
        });
});

test('missing lookup entry', function (t) {
        var record = JSON.parse(JSON.stringify(STORAGE_RECORD));
        record.owner = '478c085c-cd66-11e2-844f-7b7007fa0470';
        runTest({
                stdin: JSON.stringify(record)
        }, function (result) {
                var lines = result.stdout.split('\n');
                var actual = JSON.parse(lines[0]);
                t.equal(result.stderr, 'No login found for UUID ' +
                        '478c085c-cd66-11e2-844f-7b7007fa0470\n');
                t.equal(lines.length, 27);
                t.deepEqual(record, actual);

                t.done();
        });
});
