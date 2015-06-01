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
var fmt = require('util').format;

var NAMESPACES = ['stor', 'public', 'jobs', 'reports'];

var StorageReduceStream = require('../assets/lib/storage-reduce.js');

var log = new bunyan({
    'name': 'storagereduce.test.js',
    'level': process.env.LOG_LEVEL || 'fatal'
});

function generateRecord(override) {
    var record = {
        'dirname': '/83081c10-1b9c-44b3-9c5c-36fc2a5218a0/stor/test1',
        'key': '/83081c10-1b9c-44b3-9c5c-36fc2a5218a0/stor/test1/filea',
        'mtime': 1347493502898,
        'owner': '83081c10-1b9c-44b3-9c5c-36fc2a5218a0',
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

    if (override) {
        Object.keys(override).forEach(function (k) {
            record[k] = override[k];
        });
    }

    return (record);
}

function emptyOutput(override) {
    var output = {
        stor: {
            directories: 0,
            keys: 0,
            objects: 0,
            bytes: '0'
        },
        public: {
            directories: 0,
            keys: 0,
            objects: 0,
            bytes: '0'
        },
        jobs: {
            directories: 0,
            keys: 0,
            objects: 0,
            bytes: '0'
        },
        reports: {
            directories: 0,
            keys: 0,
            objects: 0,
            bytes: '0'
        }
    };

    if (override) {
        Object.keys(override).forEach(function (k) {
            output[k] = override[k];
        });
    }

    return (output);
}


test('basic', function (t) {
    var owner = '83081c10-1b9c-44b3-9c5c-36fc2a5218a0';
    var records = [
        generateRecord(),
        generateRecord({
            key: fmt('/%s/stor/test1/fileb', owner),
            objectId: 'ba975b18-0990-4254-8aa4-5b210c95254a'
        }),
        generateRecord({
            key: fmt('/%s/stor/test1/filec', owner),
            objectId: '0557e4c1-6692-46b2-ac85-e8d105b6238f'
        })
    ];

    var input = records.map(JSON.stringify).join('\n');

    var stream = new StorageReduceStream({
        log: log,
        namespaces: NAMESPACES,
        minSize: 0
    });

    var expected = emptyOutput({
        owner: owner,
        stor: {
            directories: 0,
            keys: 3,
            objects: 3,
            bytes: '30000'
        }
    });

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});


test('single link', function (t) {
    var owner = '83081c10-1b9c-44b3-9c5c-36fc2a5218a0';
    var records = [
        generateRecord(),
        generateRecord({
            key: fmt('/%s/stor/test1/fileb', owner),
            objectId: 'ba975b18-0990-4254-8aa4-5b210c95254a'
        }),
        generateRecord({
            key: fmt('/%s/stor/test1/filec', owner),
            objectId: '0557e4c1-6692-46b2-ac85-e8d105b6238f'
        }),
        generateRecord({
            key: fmt('/%s/stor/test1/linktofilec', owner),
            objectId: '0557e4c1-6692-46b2-ac85-e8d105b6238f'
        })
    ];
    var input = records.map(JSON.stringify).join('\n');
    var stream = new StorageReduceStream({
        log: log,
        namespaces: NAMESPACES,
        minSize: 0
    });

    var expected = emptyOutput({
        owner: owner,
        stor: {
            directories: 0,
            keys: 4,
            objects: 3,
            bytes: '30000'
        }
    });

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});


test('multiple links', function (t) {
    var owner = '83081c10-1b9c-44b3-9c5c-36fc2a5218a0';
    var records = [
        generateRecord(),
        generateRecord({
            key: fmt('/%s/stor/test1/fileb', owner),
            objectId: 'ba975b18-0990-4254-8aa4-5b210c95254a'
        }),
        generateRecord({
            key: fmt('/%s/stor/test1/linktofileb', owner),
            objectId: 'ba975b18-0990-4254-8aa4-5b210c95254a'
        }),
        generateRecord({
            key: fmt('/%s/stor/test1/filec', owner),
            objectId: '0557e4c1-6692-46b2-ac85-e8d105b6238f'
        }),
        generateRecord({
            key: fmt('/%s/stor/test1/linktofilec', owner),
            objectId: '0557e4c1-6692-46b2-ac85-e8d105b6238f'
        })
    ];
    var input = records.map(JSON.stringify).join('\n');
    var stream = new StorageReduceStream({
        log: log,
        namespaces: NAMESPACES,
        minSize: 0
    });

    var expected = emptyOutput({
        owner: owner,
        stor: {
            directories: 0,
            keys: 5,
            objects: 3,
            bytes: '30000'
        }
    });

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});


test('cross-namespace links', function (t) {
    var owner = '83081c10-1b9c-44b3-9c5c-36fc2a5218a0';
    var records = [
        generateRecord({
            key: fmt('/%s/public/test1/filea', owner),
            objectId: 'ba975b18-0990-4254-8aa4-5b210c95254a'
        }),
        generateRecord({
            key: fmt('/%s/stor/test1/linktofilea', owner),
            objectId: 'ba975b18-0990-4254-8aa4-5b210c95254a'
        })
    ];
    var input = records.map(JSON.stringify).join('\n');
    var stream = new StorageReduceStream({
        log: log,
        namespaces: NAMESPACES,
        minSize: 0
    });

    var expected = emptyOutput({
        owner: owner,
        stor: {
            directories: 0,
            keys: 1,
            objects: 1,
            bytes: '10000'
        },
        public: {
            directories: 0,
            keys: 1,
            objects: 0,
            bytes: '0'
        }
    });

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});


test('large integers', function (t) {
    var owner = '83081c10-1b9c-44b3-9c5c-36fc2a5218a0';
    var records = [
        generateRecord({
            key: fmt('/%s/stor/test1/filea', owner),
            objectId: 'ba975b18-0990-4254-8aa4-5b210c95254a',
            contentLength: Math.pow(2, 53)
        }),
        generateRecord({
            key: fmt('/%s/stor/test1/fileb', owner),
            objectId: '0557e4c1-6692-46b2-ac85-e8d105b6238f',
            contentLength: Math.pow(2, 53)
        })
    ];
    var input = records.map(JSON.stringify).join('\n');
    var stream = new StorageReduceStream({
        log: log,
        namespaces: NAMESPACES,
        minSize: 0
    });

    var expected = emptyOutput({
        owner: owner,
        stor: {
            directories: 0,
            keys: 2,
            objects: 2,
            bytes: '36028797018963968'
        }
    });

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});


test('cross-account links', function (t) {
    var owner1 = 'bf39eed6-0569-11e5-9f3f-975fd4059faf';
    var owner2 = '83081c10-1b9c-44b3-9c5c-36fc2a5218a0';
    var records = [
        generateRecord({
            owner: owner1,
            key: fmt('/%s/stor/test1/filea', owner1)
        }),
        generateRecord({
            owner: owner2,
            key: fmt('/%s/stor/test1/filea', owner2)
        })
    ];
    var input = records.map(JSON.stringify).join('\n');
    var stream = new StorageReduceStream({
        log: log,
        namespaces: NAMESPACES,
        minSize: 0
    });

    var expected = [
        emptyOutput({
            owner: owner1,
            stor: {
                directories: 0,
                keys: 1,
                objects: 1,
                bytes: '10000'
            }
        }),
        emptyOutput({
            owner: owner2,
            stor: {
                directories: 0,
                keys: 1,
                objects: 1,
                bytes: '10000'
            }
        })
    ];

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});
