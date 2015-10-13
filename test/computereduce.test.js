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

var computereduce =
        mod_path.resolve(__dirname, '../assets/lib/compute-reduce.js');

var log = new mod_bunyan({
        'name': 'computereduce.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;

function runTest(opts, cb) {
        var spawn = mod_child_process.spawn(computereduce, opts.opts);

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

var RECORD = {
        'owner': 'a792e2b4-ccde-4b9f-99ed-8e824643c07e',
        'jobs': {
                'c29fb939-c19e-4439-9692-6a0f14bd9728': {
                        '0': {
                                'memory': 1024,
                                'disk': 8,
                                'seconds': 2,
                                'ntasks': 2,
                                'bandwidth': {
                                        'in': '39356',
                                        'out': '1260'
                                }
                        }
                },
                '60812ed5-23aa-41cc-898c-36a5a1b5d223': {
                        '1': {
                                'memory': 2048,
                                'disk': 8,
                                'seconds': 1,
                                'ntasks': 1,
                                'bandwidth': {
                                        'in': '23004',
                                        'out': '756'
                                }
                        },
                        '2': {
                                'memory': 1024,
                                'disk': 8,
                                'seconds': 2,
                                'ntasks': 1,
                                'bandwidth': {
                                        'in': '553',
                                        'out': '336'
                                }
                        }
                },
                '9a8c4cec-2e6f-46cf-b290-87b16dd49c7b': {
                        '0': {
                                'memory': 1024,
                                'disk': 8,
                                'seconds': 2,
                                'ntasks': 1,
                                'bandwidth': {
                                        'in': '3032',
                                        'out': '420'
                                }
                        },
                        '1': {
                                'memory': 1024,
                                'disk': 8,
                                'seconds': 1,
                                'ntasks': 1,
                                'bandwidth': {
                                        'in': '553',
                                        'out': '420'
                                }
                        }
                }
        }
};

var TWICE = {
        'owner': 'a792e2b4-ccde-4b9f-99ed-8e824643c07e',
        'jobs': {
                'c29fb939-c19e-4439-9692-6a0f14bd9728': {
                        '0': {
                                'memory': 1024,
                                'disk': 8,
                                'seconds': 4,
                                'ntasks': 4,
                                'bandwidth': {
                                        'in': '78712',
                                        'out': '2520'
                                }
                        }
                },
                '60812ed5-23aa-41cc-898c-36a5a1b5d223': {
                        '1': {
                                'memory': 2048,
                                'disk': 8,
                                'seconds': 2,
                                'ntasks': 2,
                                'bandwidth': {
                                        'in': '46008',
                                        'out': '1512'
                                }
                        },
                        '2': {
                                'memory': 1024,
                                'disk': 8,
                                'seconds': 4,
                                'ntasks': 2,
                                'bandwidth': {
                                        'in': '1106',
                                        'out': '672'
                                }
                        }
                },
                '9a8c4cec-2e6f-46cf-b290-87b16dd49c7b': {
                        '0': {
                                'memory': 1024,
                                'disk': 8,
                                'seconds': 4,
                                'ntasks': 2,
                                'bandwidth': {
                                        'in': '6064',
                                        'out': '840'
                                }
                        },
                        '1': {
                                'memory': 1024,
                                'disk': 8,
                                'seconds': 2,
                                'ntasks': 2,
                                'bandwidth': {
                                        'in': '1106',
                                        'out': '840'
                                }
                        }
                }
        }
};

test('basic', function (t) {
        runTest({
                stdin: JSON.stringify(RECORD) + '\n' + JSON.stringify(RECORD)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(result.stdout);
                t.deepEqual(actual, TWICE);
                t.done();
        });
});
