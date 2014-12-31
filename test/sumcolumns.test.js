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

var sumcolumns = mod_path.resolve(__dirname, '../assets/lib/sum-columns.js');

var log = new mod_bunyan({
    'name': 'sumcolumns.test.js',
    'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;

function runTest(opts, cb) {
    var spawn = mod_child_process.spawn(sumcolumns, opts.opts);

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
    var record1 = {
        'key': 'test',
        'a': 1,
        'b': 2,
        'c': 3
    };
    var record2 = {
        'key': 'test',
        'a': 3,
        'c': 5,
        'd': 6
    };
    var expected = {
        'key': 'test',
        'a': 4,
        'b': 2,
        'c': 8,
        'd': 6
    };
    runTest({
        stdin: JSON.stringify(record1) + '\n' + JSON.stringify(record2)
    }, function (result) {
        t.deepEqual(JSON.parse(result.stdout), expected);
        t.done();
    });
});

test('large integers', function (t) {
    var record1 = {
        'key': 'test',
        'a': '398409348255024958',
    };
    var record2 = {
        'key': 'test',
        'a': '87946397849793'
    };
    var expected = {
        'key': 'test',
        'a': '398497294652874751'
    };
    runTest({
        stdin: JSON.stringify(record1) + '\n' + JSON.stringify(record2)
    }, function (result) {
        t.deepEqual(JSON.parse(result.stdout), expected);
        t.done();
    });
});

test('recursive', function (t) {
    var record1 = {
        'key': 'test',
        'a': { '1': 1, '2': 2 },
        'b': {
            'c': { '3': 3, '4': 4 },
            'd': { '5': 5, '6': 6, '7': 7},
        }
    };

    var record2 = {
        'key': 'test',
        'a': { '1': 8, '2': 9 },
        'b': {
            'c': { '3': 10, '4': 11 },
            'd': { '5': 12, '6': 13, '8': 14},
        }

    };

    var expected = {
        'key': 'test',
        'a': { '1': 9, '2': 11 },
        'b': {
            'c': { '3': 13, '4': 15 },
            'd': { '5': 17, '6': 19, '7': 7, '8': 14},
        }
    };

    runTest({
        stdin: JSON.stringify(record1) + '\n' + JSON.stringify(record2)
    }, function (result) {
        t.deepEqual(JSON.parse(result.stdout), expected);
        t.done();
    });
});

test('aggregation key', function (t) {
    var record1a = {
        'key': 'test1',
        'a': 1,
        'b': 2,
        'c': 3
    };
    var record1b = {
        'key': 'test1',
        'a': 4,
        'b': 5,
        'c': 6
    };
    var record2a = {
        'key': 'test2',
        'a': 7,
        'c': 8,
        'd': 9
    };
    var record2b = {
        'key': 'test2',
        'a': 10,
        'c': 11,
        'd': 12
    };

    var expected1 = {
        'key': 'test1',
        'a': 5,
        'b': 7,
        'c': 9,
    };
    var expected2 = {
        'key': 'test2',
        'a': 17,
        'c': 19,
        'd': 21,
    };

    var input = JSON.stringify(record1a) + '\n' +
            JSON.stringify(record1b) + '\n' +
            JSON.stringify(record2a) + '\n' +
            JSON.stringify(record2b);

    runTest({
        stdin: input
    }, function (result) {
        var lines = result.stdout.split('\n');
        t.equal(lines.length, 3);
        t.equal(lines[lines.length-1], '');
        var actual1 = JSON.parse(lines[0]);
        var actual2 = JSON.parse(lines[1]);
        if (actual1.key === 'test1') {
            t.deepEqual(actual1, expected1);
            t.deepEqual(actual2, expected2);
        } else {
            t.deepEqual(actual1, expected2);
            t.deepEqual(actual2, expected1);
        }
        t.done();
    });
});

test('add fields', function (t) {
    var record1 = {
        "owner": "a792e2b4-ccde-4b9f-99ed-8e824643c07e",
        "computeGBSeconds": 96,
        "bandwidth":  {
            "in": "98223",
            "out": "19446"
        }
    };

    var record2 = {
        "owner": "a792e2b4-ccde-4b9f-99ed-8e824643c07e",
        "byteHrs": "230286312"
    };

    var record3 = {
        "owner": "a792e2b4-ccde-4b9f-99ed-8e824643c07e",
        "requests": {
            "DELETE": 1,
            "GET": 2,
            "HEAD": 3,
            "LIST": 4,
            "OPTIONS": 5,
            "POST": 6,
            "PUT": 7
        },
        "bandwidth": {
            "in": "6880413",
            "out": "23"
        }
    };

    var expected = {
        "owner": "a792e2b4-ccde-4b9f-99ed-8e824643c07e",
        "requests": {
            "DELETE": 1,
            "GET": 2,
            "HEAD": 3,
            "LIST": 4,
            "OPTIONS": 5,
            "POST": 6,
            "PUT": 7
        },
        "bandwidth": {
            "in": "6978636",
            "out": "19469"
        },
        "computeGBSeconds": 96,
        "byteHrs": "230286312"
    };

    var input = JSON.stringify(record1) + '\n' +
            JSON.stringify(record2) + '\n' +
            JSON.stringify(record3);

    runTest({
        stdin: input
    }, function (result) {
        var actual = JSON.parse(result.stdout);
        t.deepEqual(actual, expected);
        t.done();
    });
});
