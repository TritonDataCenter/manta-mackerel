// Copyright (c) 2013, Joyent, Inc. All rights reserved.
var mod_fs = require('fs');
var mod_child_process = require('child_process');
var mod_path = require('path');
var mod_http = require('http');

var mod_bunyan = require('bunyan');
var helper = require('./helper.js');

var deliverusage =
    mod_path.resolve(__dirname, '../assets/lib/deliver-access.js');

var log = new mod_bunyan({
        'name': 'deliveraccess.test.js',
        'level': process.env['LOG_LEVEL'] || 'debug'
});

var after = helper.after;
var before = helper.before;
var test = helper.test;

var PORT= 5678;
var SERVER = null;
var MANTA_URL = 'http://localhost:' + PORT;
var LOOKUP_FILE = '../../test/test_data/lookup.json';
var LOOKUP = require('./test_data/lookup.json');

var EXPECTED = {
        'remoteAddress': '::ffff:a03:5bec',
        'req': {
                'method': 'PUT',
                'request-uri': '/poseidon/stor/manatee_backups',
                'headers': {
                        'accept': 'application/json',
                        'content-type': 'application/json; type=directory',
                        'date': 'Sun, 10 Mar 2013 10:00:02 GMT',
                        'x-request-id': '175cc9ce-6342-44be-877d-9a1eaaa25e6e',
                        'user-agent': 'restify/2.1.1 (ia32-sunos; ' +
                                'v8/3.11.10.25; OpenSSL/0.9.8w) node/0.8.14',
                        'accept-version': '~1.0',
                        'host': 'manta.beta.joyent.us',
                        'connection': 'keep-alive',
                        'transfer-encoding': 'chunked'
                },
                'httpVersion': '1.1',
                'caller': {
                        'login': 'poseidon'
                },
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
        }
};

var RECORD = {
        'name': 'muskie',
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

var PATH = '/var/tmp/' + RECORD.req.owner;

function runTest(opts, cb) {
        opts.env = opts.env || {};
        opts.env['MANTA_URL'] = MANTA_URL;
        opts.env['MANTA_NO_AUTH'] = 'true';
        opts.env['HEADER_CONTENT_TYPE'] = 'application/x-json-stream';
        opts.env['ACCESS_LINK'] = '/reports/access-logs/latest';
        opts.env['ACCESS_DEST'] = '/reports/access-logs/y/m/d/h/hhour.json';
        opts.env['LOOKUP_FILE'] = LOOKUP_FILE;

        var spawn = mod_child_process.spawn(deliverusage, opts.opts, opts);

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
                var body = '';
                req.on('data', function (data) {
                        body += data;
                });
                req.on('end', function () {
                        req.body = body;
                        SERVER.requests.push(req);
                        res.writeHead(204);
                        res.end();
                });
        }).listen(PORT, function (err) {
                cb(err);
        });
        SERVER.requests = [];
});

after(function (cb) {
        SERVER.close(function (err) {
                SERVER = null;
                if (err) {
                        cb(err);
                        return;
                }
                mod_fs.unlink(PATH, function (suberr) {
                        if (suberr && suberr.code === 'ENOENT') {
                                cb();
                                return;
                        }
                        cb(suberr);
                });
        });
});

test('whitelist', function (t) {
        runTest({
                stdin: JSON.stringify(RECORD)
        }, function (result) {
                t.equal(result.code, 0);
                t.ok(mod_fs.existsSync(PATH));

                var actual = JSON.parse(mod_fs.readFileSync(PATH, 'utf8'));
                t.deepEqual(actual, EXPECTED);
                t.done();
        });
});

test('remoteAddress ipv4->ipv6 conversion', function (t) {
        var ipv4 = '10.3.91.236';
        var input = JSON.parse(JSON.stringify(RECORD));
        input.req.headers['x-forwarded-for'] = ipv4;
        runTest({
                stdin: JSON.stringify(input)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(mod_fs.readFileSync(PATH, 'utf8'));
                t.deepEqual(actual, EXPECTED);
                t.done();
        });
});

test('empty remoteAddress', function (t) {
        var input = JSON.parse(JSON.stringify(RECORD));
        var expected = JSON.parse(JSON.stringify(EXPECTED));
        expected.remoteAddress = '::ffff:a9fe:1';
        delete input.req.headers['x-forwarded-for'];
        runTest({
                stdin: JSON.stringify(input)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(mod_fs.readFileSync(PATH, 'utf8'));
                t.deepEqual(actual, expected);
                t.done();
        });
});

test('file uploaded and link created', function (t) {
        runTest({
                stdin: JSON.stringify(RECORD)
        }, function (result) {
                t.equal(result.code, 0);
                t.equal(SERVER.requests.length, 2);
                var req = SERVER.requests[0];
                t.equal(req.headers['content-type'],
                        'application/x-json-stream');
                t.deepEqual(JSON.parse(req.body), EXPECTED);

                req = SERVER.requests[1];
                t.equal(req.headers['content-type'],
                        'application/json; type=link');
                t.done();
        });
});

test('multiple x-forwarded-for', function (t) {
        var input = JSON.parse(JSON.stringify(RECORD));
        input.req.headers['x-forwarded-for'] = '76.121.137.102, ' +
                '::ffff:66.249.85.112';
        var expected = JSON.parse(JSON.stringify(EXPECTED));
        expected.remoteAddress = '::ffff:4c79:8966';
        runTest({
                stdin: JSON.stringify(input)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(mod_fs.readFileSync(PATH, 'utf8'));
                t.deepEqual(actual, expected);
                t.done();
        });
});

test('"unknown" in x-forwarded-for', function (t) {
        var input = JSON.parse(JSON.stringify(RECORD));
        input.req.headers['x-forwarded-for'] = 'unknown, 76.121.137.102, ' +
                '::ffff:66.249.85.112';
        var expected = JSON.parse(JSON.stringify(EXPECTED));
        expected.remoteAddress = 'unknown';
        runTest({
                stdin: JSON.stringify(input)
        }, function (result) {
                t.equal(result.code, 0);
                var actual = JSON.parse(mod_fs.readFileSync(PATH, 'utf8'));
                t.deepEqual(actual, expected);
                t.done();
        });
});

test('drop poseidon requests', function (t) {
        runTest({
                stdin: JSON.stringify(RECORD),
                env: {
                        'DROP_POSEIDON_REQUESTS': 'true'
                }
        }, function (result) {
                t.equal(result.code, 0);
                t.equal(SERVER.requests.length, 0);
                t.done();
        });
});

test('do not deliver unapproved reports', function (t) {
        var input = JSON.parse(JSON.stringify(RECORD));
        input.req.owner = 'ed5fa8da-fd61-42bb-a24a-515b56c6d581';
        t.equal(LOOKUP[input.req.owner].approved, false);
        runTest({
                stdin: JSON.stringify(input),
                env: {
                        'DELIVER_UNAPPROVED_REPORTS': 'false'
                }
        }, function (result) {
                t.equal(result.code, 0);
                t.equal(SERVER.requests.length, 0);
                t.done();
        });
});

test('deliver unapproved reports', function (t) {
        var input = JSON.parse(JSON.stringify(RECORD));
        input.req.owner = 'ed5fa8da-fd61-42bb-a24a-515b56c6d581';
        t.equal(LOOKUP[input.req.owner].approved, false);
        var path = '/var/tmp/' + input.req.owner;
        runTest({
                stdin: JSON.stringify(input),
                env: {
                        'DELIVER_UNAPPROVED_REPORTS': 'true'
                }
        }, function (result) {
                t.equal(result.code, 0);
                t.equal(SERVER.requests.length, 2);
                var actual = JSON.parse(mod_fs.readFileSync(path, 'utf8'));
                t.deepEqual(actual, EXPECTED);
                t.done();
        });
});

test('malformed line', function (t) {
        runTest({
                stdin: JSON.stringify(RECORD) + '\n{"incomplete":{"record":',
        }, function (result) {
                t.equal(1, result.code);
                t.done();
        });
});

test('malformed line limit', function (t) {
        runTest({
                stdin: JSON.stringify(RECORD) + '\n{"incomplete":{"record":',
                env: {
                        "MALFORMED_LIMIT": "1",
                        'COUNT_UNAPPROVED_USERS': 'true'
                }
        }, function (result) {
                t.equal(0, result.code);
                t.equal(SERVER.requests.length, 2);
                var actual = JSON.parse(mod_fs.readFileSync(PATH, 'utf8'));
                t.deepEqual(actual, EXPECTED);
                t.done();
        });
});

