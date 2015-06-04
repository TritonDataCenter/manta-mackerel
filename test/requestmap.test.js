/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bunyan = require('bunyan');
var test = require('tape');
var h = require('./helper.js');

var RequestMapStream = require('../assets/lib/request-map.js');

var log = new bunyan({
    'name': 'requestmap.test.js',
    'level': process.env.LOG_LEVEL || 'fatal'
});

var BILLABLE_OPS = ['DELETE', 'GET', 'HEAD', 'LIST', 'OPTIONS', 'POST', 'PUT'];

function generateRecord(override) {
    var record = {
        'name': 'muskie',
        'hostname': 'c84b3cab-1c20-4566-a880-0e202b6b63dd',
        'pid': 15503,
        'component': 'HttpServer',
        'audit': true,
        'level': 30,
        '_audit': true,
        'operation': 'getstorage',
        'billable_operation': 'GET',
        'bytesTransferred': '4000',
        'reqHeaderLength': 100,
        'req': {
            'method': 'GET',
            'url': '/poseidon/stor/test.json',
            'headers': {
                'accept': '*/*',
                'date': 'elided',
                'x-request-id': 'elided',
                'authorization': 'elided',
                'user-agent': 'elided',
                'accept-version': '~1.0',
                'host': 'us-east.manta.joyent.com',
                'connection': 'keep-alive',
                'x-forwarded-for': '::ffff:165.225.128.232'
            },
            'httpVersion': '1.1',
            'owner': 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a',
            'caller': {
                'login': 'poseidon',
                'uuid': 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a',
                'groups': [
                    'operators'
                ],
                'user': null
            },
            'timers': {
                'elided': 0
            }
        },
        'resHeaderLength': 200,
        'res': {
            'statusCode': 200,
            'headers': {
                'etag': '8faa04ee-beea-c9d7-c493-96ac2367636f',
                'last-modified': 'elided',
                'accept-ranges': 'bytes',
                'content-type': 'application/json',
                'content-md5': 'tMWfm2tOmvOX2tD6OG8H/g==',
                'content-length': '400',
                'durability-level': 2,
                'date': 'elided',
                'server': 'Manta'
            }
        },
        'latency': 12,
        '_auditData': true,
        'dataLatency': 12,
        'dataSize': 3313,
        'latencyToFirstByte': 12,
        'msg': 'handled: 200',
        'time': 'elided',
        'v': 0
    };

    if (override) {
        Object.keys(override).forEach(function (k) {
            if (k === 'req' || k === 'res') {
                Object.keys(override[k]).forEach(function (l) {
                    record[k][l] = override[k][l];
                });
            } else {
                record[k] = override[k];
            }
        });
    }

    return (record);
}

function emptyOutput(override) {
    var output = {
        'requests': {
            'type': {
                'PUT': 0,
                'LIST': 0,
                'GET': 0,
                'DELETE': 0,
                'POST': 0,
                'HEAD': 0,
                'OPTIONS': 0
            },
            'bandwidth': {
                'in': '0',
                'out': '0',
                'headerIn': '0',
                'headerOut': '0'
            }
        },
    };

    if (override) {
        if (override.type) {
            Object.keys(override.type).forEach(function (k) {
                output.requests.type[k] = override.type[k];
            });
        }
        if (override.bandwidth) {
            Object.keys(override.bandwidth).forEach(function (k) {
                output.requests.bandwidth[k] = override.bandwidth[k];
            });
        }
        output.owner = override.owner;
    }

    return (output);
}



test('GET', function (t) {
    var owner = 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a';
    var records = [
        generateRecord(),
        generateRecord()
    ];

    var input = records.map(JSON.stringify).join('\n');

    var stream = new RequestMapStream({
        admin: 'poseidon',
        billableOps: BILLABLE_OPS,
        includeAdmin: true,
        excludeUnapproved: false,
        log: log
    });

    var expected = emptyOutput({
        owner: owner,
        type: {
            GET: 2
        },
        bandwidth: {
            in: '0',
            out: '800',
            headerIn: '200',
            headerOut: '400'
        }
    });

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});


test('ignore bad lines', function (t) {
    var owner = 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a';
    var input = [
        '[ Nov 28 21:35:27 Disabled. ]',
        '[ Nov 28 21:35:27 Rereading configuration. ]',
        '[ Nov 28 21:35:27 Enabled. ]',
        JSON.stringify(generateRecord())
    ].join('\n');

    var stream = new RequestMapStream({
        admin: 'poseidon',
        billableOps: BILLABLE_OPS,
        includeAdmin: true,
        excludeUnapproved: false,
        log: log
    });

    var malformed = 0;
    stream.on('malformed', function () {
        malformed++;
    });

    var expected = emptyOutput({
        owner: owner,
        type: {
            GET: 1
        },
        bandwidth: {
            in: '0',
            out: '400',
            headerIn: '100',
            headerOut: '200'
        }
    });

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.equal(3, malformed, 'malformed line count');
        t.end();
    });
});


test('ignore 404', function (t) {
    var owner = 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a';
    var records = [
        generateRecord({
            res: {
                statusCode: 404
            }
        }),
        generateRecord()
    ];

    var input = records.map(JSON.stringify).join('\n');
    var stream = new RequestMapStream({
        admin: 'poseidon',
        billableOps: BILLABLE_OPS,
        includeAdmin: true,
        excludeUnapproved: false,
        log: log
    });
    var expected = emptyOutput({
        owner: owner,
        type: {
            GET: 1
        },
        bandwidth: {
            in: '0',
            out: '400',
            headerIn: '100',
            headerOut: '200'
        }
    });
    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});

test('count unapproved users', function (t) {
    var owner1 = 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a';
    var owner2 = '7cd763f2-094c-11e5-8c52-a7dc71d5f7c3';
    var records = [
        generateRecord(),
        generateRecord({
            req: {
                owner: owner2
            }
        })
    ];
    var lookup = {};
    lookup[owner1] = {
        approved: true,
        login: 'poseidon'
    };
    lookup[owner2] = {
        approved: false,
        login: 'bob_user'
    };

    var input = records.map(JSON.stringify).join('\n');

    var stream = new RequestMapStream({
        admin: 'poseidon',
        billableOps: BILLABLE_OPS,
        includeAdmin: true,
        excludeUnapproved: false,
        log: log,
        lookup: lookup
    });

    var expected = [
        emptyOutput({
            owner: owner1,
            type: {
                GET: 1
            },
            bandwidth: {
                in: '0',
                out: '400',
                headerIn: '100',
                headerOut: '200'
            }
        }),
        emptyOutput({
            owner: owner2,
            type: {
                GET: 1
            },
            bandwidth: {
                in: '0',
                out: '400',
                headerIn: '100',
                headerOut: '200'
            }
        })
    ];

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});

test('count unapproved users', function (t) {
    var owner1 = 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a';
    var owner2 = '7cd763f2-094c-11e5-8c52-a7dc71d5f7c3';
    var records = [
        generateRecord(),
        generateRecord({
            req: {
                owner: owner2
            }
        })
    ];
    var lookup = {};
    lookup[owner1] = {
        approved: true,
        login: 'poseidon'
    };
    lookup[owner2] = {
        approved: false,
        login: 'bob_user'
    };

    var input = records.map(JSON.stringify).join('\n');

    var stream = new RequestMapStream({
        admin: 'poseidon',
        billableOps: BILLABLE_OPS,
        includeAdmin: true,
        excludeUnapproved: true,
        log: log,
        lookup: lookup
    });

    var expected = [
        emptyOutput({
            owner: owner1,
            type: {
                GET: 1
            },
            bandwidth: {
                in: '0',
                out: '400',
                headerIn: '100',
                headerOut: '200'
            }
        })
    ];

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});

test('don\'t count unapproved users', function (t) {
    var owner1 = 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a';
    var owner2 = '7cd763f2-094c-11e5-8c52-a7dc71d5f7c3';
    var records = [
        generateRecord(),
        generateRecord({
            req: {
                owner: owner2
            }
        })
    ];
    var lookup = {};
    lookup[owner1] = {
        approved: true,
        login: 'poseidon'
    };
    lookup[owner2] = {
        approved: false,
        login: 'bob_user'
    };

    var input = records.map(JSON.stringify).join('\n');

    var stream = new RequestMapStream({
        admin: 'poseidon',
        billableOps: BILLABLE_OPS,
        includeAdmin: true,
        excludeUnapproved: false,
        log: log,
        lookup: lookup
    });

    var expected = [
        emptyOutput({
            owner: owner1,
            type: {
                GET: 1
            },
            bandwidth: {
                in: '0',
                out: '400',
                headerIn: '100',
                headerOut: '200'
            }
        }),
        emptyOutput({
            owner: owner2,
            type: {
                GET: 1
            },
            bandwidth: {
                in: '0',
                out: '400',
                headerIn: '100',
                headerOut: '200'
            }
        })
    ];

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});

test('drop admin requests', function (t) {
    var owner = 'cfcd7b0c-0571-11e5-8b6d-1fe27e2fd88a';
    var records = [
        generateRecord(),
        generateRecord()
    ];

    var input = records.map(JSON.stringify).join('\n');

    var stream = new RequestMapStream({
        admin: 'poseidon',
        billableOps: BILLABLE_OPS,
        includeAdmin: false,
        excludeUnapproved: false,
        log: log
    });

    var expected = emptyOutput({
        owner: owner,
        type: {
            GET: 2
        },
        bandwidth: {
            in: '0',
            out: '800',
            headerIn: '200',
            headerOut: '400'
        }
    });

    h.streamTest(t, stream, input, expected, function (err) {
        t.ifError(err, 'no error');
        t.end();
    });
});
