/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var bunyan = require('bunyan');
var test = require('tape');
var h = require('./helper.js');

var StorageMapStream = require('../assets/lib/storage-map.js');

var log = new bunyan({
    'name': 'storagemap.test.js',
    'level': process.env.LOG_LEVEL || 'fatal'
});


function getSchema() {
    var s = {
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

    return (s);
}

function generatePGRecord(override) {
    var _value = {
        'dirname': '/83081c10-1b9c-44b3-9c5c-36fc2a5218a0/stor/test1',
        'key': '/83081c10-1b9c-44b3-9c5c-36fc2a5218a0/stor/test1/filea',
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

    if (override) {
        Object.keys(override).forEach(function (k) {
            _value[k] = override[k];
        });
    }

    var raw = {
        'entry': [
            '1',
            _value.key,
            JSON.stringify(_value),
            _value.etag,
            _value.mtime,
            _value.dirname,
            _value.owner,
            _value.objectId
        ]
    };

    return ({
        raw: raw,
        value: _value
    });
}

test('basic', function (t) {
    var schema = getSchema();
    var record = generatePGRecord();
    var stream = new StorageMapStream({
        log: log,
        excludeUnapproved: false
    });
    var input = JSON.stringify(schema) + '\n' + JSON.stringify(record.raw);
    var expected = record.value;
    h.streamTest(t, stream, input, expected, function () {
        t.end();
    });
});


test('value index change', function (t) {
    var schema = getSchema();
    schema.keys = [
        '_id',
        '_key',
        '_etag',
        '_mtime',
        'dirname',
        '_value',
        'owner',
        'objectid'
    ];
    var record = generatePGRecord();
    var etag = record.raw.entry[5];
    var value = record.raw.entry[2];
    record.raw.entry[5] = value;
    record.raw.entry[2] = etag;

    var stream = new StorageMapStream({
        log: log,
        excludeUnapproved: false
    });
    var input = JSON.stringify(schema) + '\n' + JSON.stringify(record.raw);
    var expected = record.value;
    h.streamTest(t, stream, input, expected, function () {
        t.end();
    });
});


test('missing schema', function (t) {
    var record = generatePGRecord();
    var stream = new StorageMapStream({
        log: log,
        excludeUnapproved: true
    });
    var input = JSON.stringify(record.raw);
    var expected = record.value;
    h.streamTest(t, stream, input, expected, function (err) {
        t.ok(err);
        t.end();
    });
});


test('do not count unapproved users', function (t) {
    var owner = 'ed5fa8da-fd61-42bb-a24a-515b56c6d581';
    var schema = getSchema();
    var record = generatePGRecord({
        owner: owner
    });
    var lookup = {
        owner: {
            approved: false
        }
    };
    var stream = new StorageMapStream({
        log: log,
        excludeUnapproved: true,
        lookup: lookup
    });

    var input = JSON.stringify(schema) + '\n' + JSON.stringify(record.raw);
    var expected = '';
    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});


test('count unapproved users', function (t) {
    var owner = 'ed5fa8da-fd61-42bb-a24a-515b56c6d581';
    var schema = getSchema();
    var record = generatePGRecord({
        owner: owner
    });
    var lookup = {
        owner: {
            approved: false
        }
    };
    var stream = new StorageMapStream({
        log: log,
        excludeUnapproved: false,
        lookup: lookup
    });

    var input = JSON.stringify(schema) + '\n' + JSON.stringify(record.raw);
    var expected = record.value;
    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});
