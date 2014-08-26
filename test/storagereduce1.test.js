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

var storagereduce1 =
        mod_path.resolve(__dirname, '../assets/lib/storage-reduce1.js');

var log = new mod_bunyan({
        'name': 'storagereduce1.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;

function runTest(opts, cb) {
        var env = {
                env: {
                        "NAMESPACES": "stor public jobs reports",
                        "MIN_SIZE": 4096
                }
        };

        var spawn = mod_child_process.spawn(storagereduce1, opts.opts, env);

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

var NAMESPACES = ['stor', 'public', 'jobs', 'reports'];
var COUNTERS = ['directories', 'keys', 'objects', 'bytes'];

var RECORD1 = {
        'dirname': '/fred/stor/test1',
        'key': '/fred/stor/test1/filea',
        'mtime': 1347493502898,
        'owner': 'fred',
        'type': 'object',
        'contentLength': 5000,
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

var RECORD2 = {
        'dirname': '/fred/stor/test1',
        'key': '/fred/stor/test1/fileb',
        'mtime': 1347493503184,
        'owner': 'fred',
        'type': 'object',
        'contentLength': 10,
        'contentMD5': '1heGR2UzDU4HU3EIGm9vcA==',
        'contentType': 'application/x-www-form-urlencoded',
        'etag': 'd617864765330d4e075371081a6f6f70',
        'objectId': 'ba975b18-0990-4254-8aa4-5b210c95254a',
        'sharks': [
                {
                        'url': 'url3',
                        'server_uuid': 'server3',
                        'zone_uuid': 'zone3'
                },
                {
                        'url': 'url4',
                        'server_uuid': 'server4',
                        'zone_uuid': 'zone4'
                }
        ]
};

var RECORD3 = {
        'dirname': '/fred/stor/test1',
        'key': '/fred/stor/test1/linktofilea',
        'mtime': 1347493503388,
        'owner': 'fred',
        'type': 'object',
        'contentLength': 5000,
        'contentMD5': 'RWJGkh2n/L4XhjDn2a5rgA==',
        'contentType': 'application/x-www-form-urlencoded',
        'createdFrom': '/fred/stor/test1/filea',
        'etag': '456246921da7fcbe178630e7d9ae6b80',
        'objectId': 'bd83468a-ae70-4d96-80cc-8fc49068caca',
        'sharks': [
                {
                        'url': 'url5',
                        'server_uuid': 'server5',
                        'zone_uuid': 'zone5'
                },
                {
                        'url': 'url6',
                        'server_uuid': 'server6',
                        'zone_uuid': 'zone6'
                }
        ]
};

var RECORD4 = {
        'dirname': '/fred/stor/test1',
        'key': '/fred/stor/test1/modifiedlinktofilea',
        'mtime': 1347493536910,
        'owner': 'fred',
        'type': 'object',
        'contentLength': 500,
        'contentMD5': 'Qxl2vXm9Lm4HtltFE6f6SQ==',
        'contentType': 'application/x-www-form-urlencoded',
        'etag': '431976bd79bd2e6e07b65b4513a7fa49',
        'objectId': '0557e4c1-6692-46b2-ac85-e8d105b6238f',
        'sharks': [
                {
                        'url': 'url7',
                        'server_uuid': 'server7',
                        'zone_uuid': 'zone7'
                },
                {
                        'url': 'url8',
                        'server_uuid': 'server8',
                        'zone_uuid': 'zone8'
                }
        ]
};


test('basic', function (t) {
        var input = JSON.stringify(RECORD1) + '\n' +
                    JSON.stringify(RECORD2) + '\n' +
                    JSON.stringify(RECORD4);
        runTest({
                stdin: input,
        }, function (result) {
                var lines = result.stdout.split('\n');
                t.equal(lines.length, 5);
                t.equal(lines[lines.length-1], '');
                for (var i = 0; i < lines.length-1; i++) {
                        var actual = JSON.parse(lines[i]);
                        t.equal(actual.owner, 'fred');
                        t.equal(typeof(actual.bytes), 'string');
                        if (actual.namespace === 'stor') {
                                t.equal(actual.keys, 3);
                                t.equal(actual.objects, 3);
                                t.equal(actual.bytes, 2 * (5000 + 4096 + 4096));
                        } else {
                                COUNTERS.forEach(function (counter) {
                                        t.equal(actual[counter], 0)
                                });
                        }
                }
                t.done();
        });
});

test('single link', function (t) {
        var input = JSON.stringify(RECORD1) + '\n' +
                    JSON.stringify(RECORD2) + '\n' +
                    JSON.stringify(RECORD3) + '\n' +
                    JSON.stringify(RECORD4);
        runTest({
                stdin: input
        }, function (result) {
                var lines = result.stdout.split('\n');
                t.equal(lines.length, 5);
                t.equal(lines[lines.length-1], '');
                for (var i = 0; i < lines.length-1; i++) {
                        var actual = JSON.parse(lines[i]);
                        t.equal(actual.owner, 'fred');
                        if (actual.namespace === 'stor') {
                                t.equal(actual.keys, 4);
                                t.equal(actual.objects, 3);
                                t.equal(actual.bytes, 2 * (5000 + 4096 + 4096));
                        } else {
                                COUNTERS.forEach(function (counter) {
                                        t.equal(actual[counter], 0)
                                });
                        }
                }
                t.done();
        });
});

test('multiple links', function (t) {
        var input = JSON.stringify(RECORD1) + '\n' +
                    JSON.stringify(RECORD2) + '\n' +
                    JSON.stringify(RECORD2) + '\n' +
                    JSON.stringify(RECORD3) + '\n' +
                    JSON.stringify(RECORD4) + '\n' +
                    JSON.stringify(RECORD4);
        runTest({
                stdin: input
        }, function (result) {
                var lines = result.stdout.split('\n');
                t.equal(lines.length, 5);
                t.equal(lines[lines.length-1], '');
                for (var i = 0; i < lines.length-1; i++) {
                        var actual = JSON.parse(lines[i]);
                        t.equal(actual.owner, 'fred');
                        if (actual.namespace === 'stor') {
                                t.equal(actual.keys, 6);
                                t.equal(actual.objects, 3);
                                t.equal(actual.bytes, 2 * (5000 + 4096 + 4096));
                        } else {
                                COUNTERS.forEach(function (counter) {
                                        t.equal(actual[counter], 0)
                                });
                        }
                }
                t.done();
        });
});

test('cross-namespace links', function (t) {
        var record1 = JSON.parse(JSON.stringify(RECORD1));
        var record3 = JSON.parse(JSON.stringify(RECORD3));
        record1.key = '/fred/public/test1/filea';
        record3.key = '/fred/reports/test1/filea';

        var input = JSON.stringify(record1) + '\n' +
                    JSON.stringify(RECORD2) + '\n' +
                    JSON.stringify(record3) + '\n' +
                    JSON.stringify(RECORD4);
        runTest({
                stdin: input
        }, function (result) {
                var lines = result.stdout.split('\n');
                t.equal(lines.length, 5);
                t.equal(lines[lines.length-1], '');
                for (var i = 0; i < lines.length-1; i++) {
                        var actual = JSON.parse(lines[i]);
                        t.equal(actual.owner, 'fred');
                        if (actual.namespace === 'stor') {
                                t.equal(actual.keys, 2);
                                t.equal(actual.objects, 2);
                                t.equal(actual.bytes, 2 * (4096 + 4096));
                        } else if (actual.namespace === 'public') {
                                t.equal(actual.keys, 1);
                                t.equal(actual.objects, 1);
                                t.equal(actual.bytes, 2 * 5000);
                        } else if (actual.namespace === 'reports') {
                                t.equal(actual.keys, 1);
                                t.equal(actual.objects, 0);
                                t.equal(actual.bytes, 0);
                        } else {
                                t.equal(actual.namespace, 'jobs');
                                t.equal(actual.keys, 0);
                                t.equal(actual.objects, 0);
                                t.equal(actual.bytes, 0);
                        }
                }
                t.expect(19);
                t.done();
        });
});

test('large integers', function (t) {
        var record1 = JSON.parse(JSON.stringify(RECORD1));
        var record2 = JSON.parse(JSON.stringify(RECORD2));
        record1.contentLength = Math.pow(2, 53);
        record2.contentLength = Math.pow(2, 53);
        var input = JSON.stringify(record1) + '\n' +
                    JSON.stringify(record2);
        runTest({
                stdin: input
        }, function (result) {
                var lines = result.stdout.split('\n');
                t.equal(lines.length, 5);
                t.equal(lines[lines.length-1], '');
                for (var i = 0; i < lines.length-1; i++) {
                        var actual = JSON.parse(lines[i]);
                        t.equal(actual.owner, 'fred');
                        if (actual.namespace === 'stor') {
                                t.equal(actual.keys, 2);
                                t.equal(actual.objects, 2);
                                t.equal(actual.bytes, '36028797018963968');
                        } else {
                                COUNTERS.forEach(function (counter) {
                                        t.equal(actual[counter], 0)
                                });
                        }
                }
                t.done();
        });
});

test('multiple owners', function (t) {
        var record1 = JSON.parse(JSON.stringify(RECORD1));
        record1.owner = 'poseidon';
        record1.objectId = '38b53566-cef0-11e2-ab8e-87da2036778e';

        var input = JSON.stringify(record1) + '\n' +
                    JSON.stringify(RECORD2) + '\n' +
                    JSON.stringify(RECORD3) + '\n' +
                    JSON.stringify(RECORD4);

        runTest({
                stdin: input,
        }, function (result) {
                var lines = result.stdout.split('\n');
                t.equal(lines.length, 9);
                t.equal(lines[lines.length-1], '');
                var freds = 0;
                var poseidons = 0;
                for (var i = 0; i < lines.length-1; i++) {
                        var actual = JSON.parse(lines[i]);
                        if (actual.owner === 'fred') {
                                freds++;
                                if (actual.namespace === 'stor') {
                                        t.equal(actual.keys, 3);
                                        t.equal(actual.objects, 3);
                                        t.equal(actual.bytes,
                                                2 * (5000 + 4096 + 4096));
                                }
                        } else {
                                poseidons++;
                                if (actual.namespace === 'stor') {
                                        t.equal(actual.keys, 1);
                                        t.equal(actual.objects, 1);
                                        t.equal(actual.bytes, 2 * 5000);
                                }
                        }
                }
                t.equal(freds, 4);
                t.equal(poseidons, 4);
                t.done();
        });


});
