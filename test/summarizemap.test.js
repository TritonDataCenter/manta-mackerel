// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_fs = require('fs');
var mod_child_process = require('child_process');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var helper = require('./helper.js');

var summarizemap =
        mod_path.resolve(__dirname, '../assets/lib/summarize-map.js');

var log = new mod_bunyan({
        'name': 'summarizemap.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;

function runTest(opts, cb) {
        var spawn = mod_child_process.spawn(summarizemap, opts.opts);

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

var STORAGE = {
        "owner": "cc56f978-00a7-4908-8d20-9580a3f60a6e",
        "storage": {
                "stor": {
                        "directories": "301",
                        "keys": "1523",
                        "objects": "1523",
                        "bytes": "230286312"
                },
                "public": {
                        "directories": "0",
                        "keys": "0",
                        "objects": "0",
                        "bytes": "0"
                },
                "jobs": {
                        "directories": "0",
                        "keys": "1",
                        "objects": "1",
                        "bytes": "34908"
                },
                "reports": {
                        "directories": "0",
                        "keys": "1",
                        "objects": "1",
                        "bytes": "5498"
                }
        }
};

var REQUEST = {
        'owner': '83081c10-1b9c-44b3-9c5c-36fc2a5218a0',
        'requests': {
                'type': {
                        'PUT': "1",
                        'LIST': "2",
                        'GET': "3",
                        'DELETE': "4",
                        'POST': "5",
                        'LIST': "6",
                        'HEAD': "7",
                        'OPTIONS': "8"
                },
                'bandwidth': {
                        'in': '2294',
                        'out': '458',
                        'headerIn': '806',
                        'headerOut': '200'
                }
        },
};

var COMPUTE = {
        "owner": "a792e2b4-ccde-4b9f-99ed-8e824643c07e",
        "jobs": {
                "c29fb939-c19e-4439-9692-6a0f14bd9728": {
                        "0": {
                                "memory": "1024",
                                "disk": "8",
                                "seconds": "2",
                                "ntasks": "2",
                                "bandwidth": {
                                        "in": "39356",
                                        "out": "1260"
                                }
                        }
                },
                "60812ed5-23aa-41cc-898c-36a5a1b5d223": {
                        "1": {
                                "memory": "2048",
                                "disk": "8",
                                "seconds": "1",
                                "ntasks": "1",
                                "bandwidth": {
                                        "in": "23004",
                                        "out": "756"
                                }
                        },
                        "2": {
                                "memory": "1024",
                                "disk": "8",
                                "seconds": "2",
                                "ntasks": "1",
                                "bandwidth": {
                                        "in": "553",
                                        "out": "336"
                                }
                        }
                },
                "9a8c4cec-2e6f-46cf-b290-87b16dd49c7b": {
                        "0": {
                                "memory": "1024",
                                "disk": "8",
                                "seconds": "2",
                                "ntasks": "1",
                                "bandwidth": {
                                        "in": "3032",
                                        "out": "420"
                                }
                        },
                        "1": {
                                "memory": "1024",
                                "disk": "8",
                                "seconds": "1",
                                "ntasks": "1",
                                "bandwidth": {
                                        "in": "553",
                                        "out": "420"
                                }
                        }
                }
        }
};

test('summarize storage', function (t) {
        runTest({
                stdin: JSON.stringify(STORAGE)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(result.stdout);
                var expected = {
                        owner: STORAGE.owner,
                        byteHrs: "230326718"
                };
                t.deepEqual(actual, expected);
                t.equal(typeof (actual.byteHrs), 'string');
                t.done();
        });
});

test('summarize request', function (t) {
        runTest({
                stdin: JSON.stringify(REQUEST)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(result.stdout);
                var expected = {
                        owner: REQUEST.owner,
                        requests: REQUEST.requests.type,
                        bandwidth: {
                                in: "2294",
                                out: "458"
                        }
                };
                t.deepEqual(actual, expected);
                t.equal(typeof (actual.requests['PUT']), 'string');
                t.equal(typeof (actual.bandwidth['in']), 'string');
                t.equal(typeof (actual.bandwidth['out']), 'string');
                t.done();
        });
});

test('summarize compute', function (t) {
        runTest({
                stdin: JSON.stringify(COMPUTE)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(result.stdout);
                var expected = {
                        owner: COMPUTE.owner,
                        computeGBSeconds: "9",
                        bandwidth: {
                                in: "66498",
                                out: "3192",
                        }
                };
                t.deepEqual(actual, expected);
                t.equal(typeof (actual.bandwidth['in']), 'string');
                t.equal(typeof (actual.bandwidth['out']), 'string');
                t.done();
        });
});

test('compute billing table', function (t) {
        var input = JSON.parse(JSON.stringify(COMPUTE));
        input.jobs['60812ed5-23aa-41cc-898c-36a5a1b5d223']['1'].disk = "512";
        input.jobs['60812ed5-23aa-41cc-898c-36a5a1b5d223']['2'].disk= "64";
        runTest({
                stdin: JSON.stringify(input)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(result.stdout);
                var expected = {
                        owner: COMPUTE.owner,
                        computeGBSeconds: "25",
                        bandwidth: {
                                in: "66498",
                                out: "3192",
                        }
                };
                t.deepEqual(actual, expected);
                t.equal(typeof (actual.bandwidth['in']), 'string');
                t.equal(typeof (actual.bandwidth['out']), 'string');
                t.done();
        });
});
