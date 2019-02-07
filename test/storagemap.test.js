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
var mod_zlib = require('zlib');

var mod_bunyan = require('bunyan');
var helper = require('./helper.js');

var storagemap = mod_path.resolve(__dirname, '../assets/lib/storage-map.js');

var log = new mod_bunyan({
        'name': 'storagemap.test.js',
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

function runTest(opts, cb) {
        opts.opts = opts.opts || [];
        opts.env = opts.env || {};
        opts.env['LOOKUP_FILE'] = LOOKUP_FILE;
        var spawn = mod_child_process.spawn(storagemap, opts.opts, opts);

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

test('basic', function (t) {
        t.expect(2);
        var schema = {
                'name': 'manta',
                'keys': [
                        '_id',
                        '_key',
                        '_value',
                        '_etag',
                        '_mtime',
                        'dirname',
                        'owner',
                        'objectid'
                ]
        };

        var _value = {
                'dirname': '/fred/stor/test1',
                'key': '/fred/stor/test1/filea',
                'mtime': 1347493502898,
                'owner': '83081c10-1b9c-44b3-9c5c-36fc2a5218a0',
                'type': 'object',
                'contentLength': 14,
                'contentMD5': 'RWJGkh2n/L4XhjDn2a5rgA==',
                'contentType': 'application/x-www-form-urlencoded',
                'etag': '456246921da7fcbe178630e7d9ae6b80',
                'objectId': 'bd83468a-ae70-4d96-80cc-8fc49068caca',
                'sharks': [
                        {
                                'url': 'url1',
                                'server_uuid': 'server1',
                                'zone_uuid': 'zone1'
                        },
                        {
                                'url': 'url2',
                                'server_uuid': 'server2',
                                'zone_uuid': 'zone2'
                        }
                ]
        };

        var record = {
                'entry': [
                        '1',
                        '/fred/stor/test1/filea',
                        JSON.stringify(_value),
                        '456246921da7fcbe178630e7d9ae6b80',
                        '1347493502898',
                        '/fred/stor/test1',
                        'fred',
                        'bd83468a-ae70-4d96-80cc-8fc49068caca'
                ]
        };

        runTest({
                stdin: JSON.stringify(schema) + '\n' + JSON.stringify(record)
        }, function (result) {
                t.equal(0, result.code);
                t.deepEqual(JSON.parse(result.stdout), _value);
                t.done();
        });
});

test('value index change', function (t) {
        t.expect(2);
        var schema = {
                'name': 'manta',
                'keys': [
                        '_id',
                        '_key',
                        '_etag',
                        '_mtime',
                        'dirname',
                        '_value',
                        'owner',
                        'objectid'
                ]
        };

        var _value = {
                'key': '/path/to/obj',
                'owner': '83081c10-1b9c-44b3-9c5c-36fc2a5218a0',
                'type': 'object'
        };

        var record = {
                'entry': [
                        '0',
                        '1',
                        '2',
                        '3',
                        '4',
                        JSON.stringify(_value),
                        '6',
                        '7'
                ]
        };

        runTest({
                stdin: JSON.stringify(schema) + '\n' + JSON.stringify(record)
        }, function (result) {
                t.equal(0, result.code);
                t.deepEqual(JSON.parse(result.stdout), _value);
                t.done();
        });
});

test('missing schema', function (t) {
        t.expect(1);

        var _value = {
                'dirname': '/fred/stor/test1',
                'key': '/fred/stor/test1/filea',
                'mtime': 1347493502898,
                'owner': 'ed5fa8da-fd61-42bb-a24a-515b56c6d581',
                'type': 'object',
                'contentLength': 14,
                'contentMD5': 'RWJGkh2n/L4XhjDn2a5rgA==',
                'contentType': 'application/x-www-form-urlencoded',
                'etag': '456246921da7fcbe178630e7d9ae6b80',
                'objectId': 'bd83468a-ae70-4d96-80cc-8fc49068caca',
                'sharks': [
                        {
                                'url': 'url1',
                                'server_uuid': 'server1',
                                'zone_uuid': 'zone1'
                        },
                        {
                                'url': 'url2',
                                'server_uuid': 'server2',
                                'zone_uuid': 'zone2'
                        }
                ]
        };

        var record = {
                'entry': [
                        '1',
                        '/fred/stor/test1/filea',
                        JSON.stringify(_value),
                        '456246921da7fcbe178630e7d9ae6b80',
                        '1347493502898',
                        '/fred/stor/test1',
                        'fred',
                        'bd83468a-ae70-4d96-80cc-8fc49068caca'
                ]
        };

        runTest({
                stdin: JSON.stringify(record)
        }, function (result) {
                t.equal(1, result.code);
                t.done();
        });
});

test('do not count unapproved users', function (t) {
        var schema = {
                'name': 'manta',
                'keys': [
                        '_id',
                        '_key',
                        '_value',
                        '_etag',
                        '_mtime',
                        'dirname',
                        'owner',
                        'objectid'
                ]
        };

        var _value = {
                'dirname': '/fred/stor/test1',
                'key': '/fred/stor/test1/filea',
                'mtime': 1347493502898,
                'owner': 'ed5fa8da-fd61-42bb-a24a-515b56c6d581',
                'type': 'object',
                'contentLength': 14,
                'contentMD5': 'RWJGkh2n/L4XhjDn2a5rgA==',
                'contentType': 'application/x-www-form-urlencoded',
                'etag': '456246921da7fcbe178630e7d9ae6b80',
                'objectId': 'bd83468a-ae70-4d96-80cc-8fc49068caca',
                'sharks': [
                        {
                                'url': 'url1',
                                'server_uuid': 'server1',
                                'zone_uuid': 'zone1'
                        },
                        {
                                'url': 'url2',
                                'server_uuid': 'server2',
                                'zone_uuid': 'zone2'
                        }
                ]
        };

        var record = {
                'entry': [
                        '1',
                        '/fred/stor/test1/filea',
                        JSON.stringify(_value),
                        '456246921da7fcbe178630e7d9ae6b80',
                        '1347493502898',
                        '/fred/stor/test1',
                        'fred',
                        'bd83468a-ae70-4d96-80cc-8fc49068caca'
                ]
        };
        runTest({
                stdin: JSON.stringify(schema) + '\n' + JSON.stringify(record),
                env: {
                        'COUNT_UNAPPROVED_USERS': 'false'
                }
        }, function (result) {
                t.equal(0, result.code);
                t.equal(result.stdout, '');
                t.done();
        });
});

test('count unapproved users', function (t) {
        var schema = {
                'name': 'manta',
                'keys': [
                        '_id',
                        '_key',
                        '_value',
                        '_etag',
                        '_mtime',
                        'dirname',
                        'owner',
                        'objectid'
                ]
        };

        var _value = {
                'dirname': '/fred/stor/test1',
                'key': '/fred/stor/test1/filea',
                'mtime': 1347493502898,
                'owner': 'ed5fa8da-fd61-42bb-a24a-515b56c6d581',
                'type': 'object',
                'contentLength': 14,
                'contentMD5': 'RWJGkh2n/L4XhjDn2a5rgA==',
                'contentType': 'application/x-www-form-urlencoded',
                'etag': '456246921da7fcbe178630e7d9ae6b80',
                'objectId': 'bd83468a-ae70-4d96-80cc-8fc49068caca',
                'sharks': [
                        {
                                'url': 'url1',
                                'server_uuid': 'server1',
                                'zone_uuid': 'zone1'
                        },
                        {
                                'url': 'url2',
                                'server_uuid': 'server2',
                                'zone_uuid': 'zone2'
                        }
                ]
        };

        var record = {
                'entry': [
                        '1',
                        '/fred/stor/test1/filea',
                        JSON.stringify(_value),
                        '456246921da7fcbe178630e7d9ae6b80',
                        '1347493502898',
                        '/fred/stor/test1',
                        'fred',
                        'bd83468a-ae70-4d96-80cc-8fc49068caca'
                ]
        };
        runTest({
                stdin: JSON.stringify(schema) + '\n' + JSON.stringify(record),
                env: {
                        'COUNT_UNAPPROVED_USERS': 'true'
                }
        }, function (result) {
                t.equal(0, result.code);
                t.deepEqual(JSON.parse(result.stdout), _value);
                t.done();
        });
});

test('don\'t upload without specifying the number of reducers', function (t) {
        t.expect(3);

        runTest({
                stdin: '',
                opts: ['-u']
        }, function (result) {
                t.ok(result.stderr.length > 0);
                t.ok(result.stdout.length === 0);
                t.equal(1, result.code);
                t.done();
        });
});

test('don\'t upload without specifying MANTA_OUTPUT_BASE in env', function (t) {
        t.expect(3);

        runTest({
                stdin: '',
                opts: ['-u', '-n', '2']
        }, function (result) {
                t.ok(result.stderr.length > 0);
                t.ok(result.stdout.length === 0);
                t.equal(1, result.code);
                t.done();
        });
});

test('basic upload', function (t) {
        t.expect(7);
        var schema = {
                'name': 'manta',
                'keys': [
                        '_id',
                        '_key',
                        '_value',
                        '_etag',
                        '_mtime',
                        'dirname',
                        'owner',
                        'objectid'
                ]
        };

        var _value = {
                'dirname': '/fred/stor/test1',
                'key': '/fred/stor/test1/filea',
                'mtime': 1347493502898,
                'owner': '83081c10-1b9c-44b3-9c5c-36fc2a5218a0',
                'type': 'object',
                'contentLength': 14,
                'contentMD5': 'RWJGkh2n/L4XhjDn2a5rgA==',
                'contentType': 'application/x-www-form-urlencoded',
                'etag': '456246921da7fcbe178630e7d9ae6b80',
                'objectId': 'bd83468a-ae70-4d96-80cc-8fc49068caca',
                'sharks': [
                        {
                                'url': 'url1',
                                'server_uuid': 'server1',
                                'zone_uuid': 'zone1'
                        },
                        {
                                'url': 'url2',
                                'server_uuid': 'server2',
                                'zone_uuid': 'zone2'
                        }
                ]
        };

        var record = {
                'entry': [
                        '1',
                        '/fred/stor/test1/filea',
                        JSON.stringify(_value),
                        '456246921da7fcbe178630e7d9ae6b80',
                        '1347493502898',
                        '/fred/stor/test1',
                        'fred',
                        'bd83468a-ae70-4d96-80cc-8fc49068caca'
                ]
        };

        runTest({
                stdin: JSON.stringify(schema) + '\n' + JSON.stringify(record),
                env : {
                        'MANTA_URL': MANTA_URL,
                        'MANTA_OUTPUT_BASE': MANTA_OUTPUT_BASE,
                        'MANTA_NO_AUTH': true
                },
                opts: ['-u', '-n', '2', '-s', 'owner,type,objectId']
        }, function (result) {
                t.equal(0, result.code);
                t.ok(result.stderr.length === 0);
                t.ok(result.stdout.length === 0);
                t.ok(SERVER.requests.length === 2);
                var r = SERVER.requests.map(function (req) {
                        return (req.body);
                }).sort(function (b1, b2) {
                        return (b1.length - b2.length);
                });
                t.ok(r[0].length < r[1].length);
                mod_zlib.gunzip(r[0], function (err1, r0) {
                        mod_zlib.gunzip(r[1], function (err2, r1) {
                                t.ok(r0.length === 0);
                                t.deepEqual(JSON.parse(r1.toString()), _value);
                                t.done();
                        });
                });
        });
});

test('upload two objects', function (t) {
        t.expect(7);
        var schema = {
                'name': 'manta',
                'keys': [
                        '_id',
                        '_key',
                        '_value',
                        '_etag',
                        '_mtime',
                        'dirname',
                        'owner',
                        'objectid'
                ]
        };

        var owners = [
                {
                        uuid: 'af90b338-1547-11e9-9320-cfb29fdb5c76',
                        name: 'bob'
                },
                {
                        uuid: 'b23bb9c0-1547-11e9-b50c-73acb1a54911',
                        name: 'saly'
                }
        ];

        var _values = owners.map(function (o) {
                return {
                        'dirname': '/' + o.name + '/stor/test1',
                        'key': '/' + o.name + '/stor/test1/filea',
                        'mtime': 1347493502898,
                        'owner': o.uuid,
                        'type': 'object',
                        'contentLength': 14,
                        'contentMD5': 'RWJGkh2n/L4XhjDn2a5rgA==',
                        'contentType': 'application/x-www-form-urlencoded',
                        'etag': '456246921da7fcbe178630e7d9ae6b80',
                        'objectId': 'bd83468a-ae70-4d96-80cc-8fc49068caca',
                        'sharks': [
                                {
                                        'url': 'url1',
                                        'server_uuid': 'server1',
                                        'zone_uuid': 'zone1'
                                },
                                {
                                        'url': 'url2',
                                        'server_uuid': 'server2',
                                        'zone_uuid': 'zone2'
                                }
                        ]
                };
        });

        var records = _values.map(function (v) {
                return {
                        'entry': [
                                '1',
                                v.key,
                                JSON.stringify(v),
                                v.objectId,
                                '1347493502898',
                                v.dirname,
                                'user',
                                'bd83468a-ae70-4d96-80cc-8fc49068caca'
                        ]
                };
        });

        var input = JSON.stringify(schema) + '\n' +
            JSON.stringify(records[0]) + '\n' +
            JSON.stringify(records[1]);

        runTest({
                stdin: input,
                env : {
                        'MANTA_URL': MANTA_URL,
                        'MANTA_OUTPUT_BASE': MANTA_OUTPUT_BASE,
                        'MANTA_NO_AUTH': true
                },
                opts: ['-u', '-n', '2', '-s', 'owner,type,objectId']
        }, function (result) {
                t.equal(0, result.code);
                t.ok(result.stderr.length === 0);
                t.ok(result.stdout.length === 0);
                t.ok(SERVER.requests.length === 2);
                var r = SERVER.requests.map(function (req) {
                        return (req.body);
                }).sort(function (b1, b2) {
                        return (b1.length - b2.length);
                });
                t.ok(r[0].length < r[1].length);
                mod_zlib.gunzip(r[0], function (err1, r0) {
                        mod_zlib.gunzip(r[1], function (err2, r1) {
                                t.deepEqual(JSON.parse(r0.toString()),
                                    _values[0]);
                                t.deepEqual(JSON.parse(r1.toString()),
                                    _values[1]);
                                t.done();
                        });
                });
        });
});
