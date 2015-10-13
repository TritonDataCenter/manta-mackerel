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

var requestmap = mod_path.resolve(__dirname, '../assets/lib/request-map.js');

var log = new mod_bunyan({
        'name': 'requestmap.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;

var LOOKUP_FILE = '../../test/test_data/lookup.json';
var LOOKUP = require('./test_data/lookup.json');

/* BEGIN JSSTYLED */
var RECORD = {
        'name': 'muskie',
        'billable_operation': 'PUT',
        'req': {
                'method': 'PUT',
                'url': '/poseidon/stor/manatee_backups',
                'headers': {
                        'accept': 'application/json',
                        'content-type': 'application/json; type=directory',
                        'date': 'Sun, 10 Mar 2013 10:00:02 GMT',
                        'x-request-id': '175cc9ce-6342-44be-877d-9a1eaaa25e6e',
                        'authorization': 'authorization_key',
                        'user-agent': 'restify/2.1.1 (ia32-sunos; ' +
                                'v8/3.11.10.25; OpenSSL/0.9.8w) node/0.8.14',
                        'accept-version': '~1.0',
                        'host': 'manta.beta.joyent.us',
                        'connection': 'keep-alive',
                        'transfer-encoding': 'chunked',
                        'x-forwarded-for': '::ffff:10.3.91.236'
                },
                "caller": {
                        "login": "poseidon",
                        "uuid": "bc50e6fc-e3e0-4cf7-bc3d-eb8229acba56",
                        "groups": [ "operators" ]
                },
                'httpVersion': '1.1',
                'owner': '83081c10-1b9c-44b3-9c5c-36fc2a5218a0'
        },
        'res': {
                'statusCode': 204,
                'headers': {
                        'last-modified': 'Wed, 13 Feb 2013 18:00:02 GMT',
                        'date': 'Sun, 10 Mar 2013 10:00:02 GMT',
                        'server': 'Manta',
                        'x-request-id': '175cc9ce-6342-44be-877d-9a1eaaa25e6e',
                        'x-response-time': 18,
                        'x-server-name': '218e7193-45c8-41e1-b4a4-7a3e6972bea6'
                }
        },
        'hostname': '218e7193-45c8-41e1-b4a4-7a3e6972bea6',
        'pid': 45918,
        'audit': true,
        'level': 30,
        '_audit': true,
        'operation': 'putdirectory',
        'remoteAddress': '10.3.91.236',
        'remotePort': 36997,
        'reqHeaderLength': 806,
        'resHeaderLength': 200,
        'latency': 18,
        'msg': 'handled: 204',
        'time': '2013-03-10T10:00:02.872Z',
        'v': 0
};

var EXPECTED = {
        'owner': '83081c10-1b9c-44b3-9c5c-36fc2a5218a0',
        'requests': {
                'type': {
                        'PUT': 1,
                        'LIST': 0,
                        'GET': 0,
                        'DELETE': 0,
                        'POST': 0,
                        'LIST': 0,
                        'HEAD': 0,
                        'OPTIONS': 0
                },
                'bandwidth': {
                        'in': '0',
                        'out': '0',
                        'headerIn': '806',
                        'headerOut': '200'
                }
        }
};
/* END JSSTYLED */

function runTest(opts, cb) {
        opts.opts = opts.opts || [];
        opts.env = opts.env || {};
        opts.env['LOOKUP_FILE'] = LOOKUP_FILE;
        var spawn = mod_child_process.spawn(requestmap, opts.opts, opts);

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

function clone(x) { return (JSON.parse(JSON.stringify(x))); }

test('ignore bad lines', function (t) {
        var input = '[ Nov 28 21:35:27 Disabled. ]\n' +
                    '[ Nov 28 21:35:27 Rereading configuration. ]\n' +
                    '[ Nov 28 21:35:27 Enabled. ]\n' +
                    JSON.stringify(RECORD);
        runTest({
                stdin: input
        }, function (result) {
                t.equal(0, result.code);
                t.deepEqual(JSON.parse(result.stdout), EXPECTED);
                t.done();
        });
});


test('404', function (t) {
        var record = clone(RECORD);
        var expected = clone(EXPECTED);
        record.res.statusCode = 404;
        runTest({
                stdin: JSON.stringify(record) + '\n' + JSON.stringify(RECORD)
        }, function (result) {
                t.equal(0, result.code);
                t.deepEqual(JSON.parse(result.stdout), expected);
                t.done();
        });
});

test('drop poseidon', function (t) {
        var record = clone(RECORD);
        var expected = clone(EXPECTED);
        expected.requests.type['PUT'] = 0;
        expected.requests.bandwidth.headerIn = 0;
        expected.requests.bandwidth.headerOut = 0;
        runTest({
                stdin: JSON.stringify(record),
                env: {
                        'DROP_POSEIDON_REQUESTS': 'true'
                }
        }, function (result) {
                t.equal(0, result.code);
                t.equal('', result.stdout);
                t.done();
        });
});

test('count unapproved users', function (t) {
        var record = clone(RECORD);
        var expected = clone(EXPECTED);
        expected.owner = 'ed5fa8da-fd61-42bb-a24a-515b56c6d581';
        record.req.owner = 'ed5fa8da-fd61-42bb-a24a-515b56c6d581';
        t.equal(LOOKUP[record.req.owner].approved, false);

        runTest({
                stdin: JSON.stringify(record),
                env: {
                        'COUNT_UNAPPROVED_USERS': 'true'
                }
        }, function (result) {
                t.equal(0, result.code);
                t.deepEqual(JSON.parse(result.stdout), expected);
                t.done();
        });
});

test('do not count unapproved users', function (t) {
        var record = clone(RECORD);
        record.req.owner = 'ed5fa8da-fd61-42bb-a24a-515b56c6d581';
        t.equal(LOOKUP[record.req.owner].approved, false);

        runTest({
                stdin: JSON.stringify(record),
                env: {
                        'COUNT_UNAPPROVED_USERS': 'false'
                }
        }, function (result) {
                t.equal(0, result.code);
                t.equal('', result.stdout);
                t.done();
        });
});

test('malformed line', function (t) {
        var record = clone(RECORD);

        runTest({
                stdin: JSON.stringify(record) + '\n{"incomplete":{"record":',
                env: {
                        'COUNT_UNAPPROVED_USERS': 'true'
                }
        }, function (result) {
                t.equal(1, result.code);
                t.done();
        });
});

test('malformed line limit', function (t) {
        var record = clone(RECORD);

        runTest({
                stdin: JSON.stringify(record) + '\n{"incomplete":{"record":',
                env: {
                        'MALFORMED_LIMIT': '1',
                        'COUNT_UNAPPROVED_USERS': 'true'
                }
        }, function (result) {
                t.equal(0, result.code);
                t.deepEqual(JSON.parse(result.stdout), EXPECTED);
                t.done();
        });
});
