// Copyright (c) 2013, Joyent, Inc. All rights reserved.
var mod_fs = require('fs');
var mod_child_process = require('child_process');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var helper = require('./helper.js');

var storagemap = mod_path.resolve(__dirname, '../assets/lib/storage-map.js');

var log = new mod_bunyan({
        'name': 'storagemap.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;

function runTest(opts, cb) {
        var spawn = mod_child_process.spawn(storagemap, opts.opts);

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
                'owner': 'fred',
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
                'owner': 'fred',
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
                'owner': 'fred',
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
