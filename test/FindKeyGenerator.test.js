/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

// To run, you need operator access or set MANTA_CONFIG to a config with
// poseidon credentials

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');
var exec = require('child_process').exec;
var fs = require('fs');
var manta = require('manta');
var mod_keygen = require('../lib/keygen/FindKeyGenerator.js');


///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;
before(function (cb) {
        var self = this;
        this.log = helper.createLogger();
        var f = process.env.SSH_KEY || process.env.HOME + '/.ssh/id_rsa';
        var cmd = 'ssh-keygen -l -f ' +
                f + ' ' +
                '| awk \'{print $2}\'';
        var url = process.env.MANTA_URL;
        var user = process.env.MANTA_USER;

        if (!process.env.MANTA_CONFIG) {
                var key = fs.readFileSync(f, 'utf8');
                this.testdir = '/' + user + '/stor/jobrunner-test';
                exec(cmd, function (err, stdout, stderr) {
                        if (err) {
                                cb(err);
                                return;
                        }

                        self.client = manta.createClient({
                                connectTimeout: 1000,
                                log: self.log,
                                retry: false,
                                sign: manta.privateKeySigner({
                                        key: key,
                                        keyId: stdout.replace('\n', ''),
                                        user: user
                                }),
                                url: url,
                                rejectUnauthorized:
                                    !process.env['MANTA_TLS_INSECURE'],
                                user: user
                        });
                        cb();
                });
        } else {
                this.client = manta.createClientFromFileSync(
                        process.env.MANTA_CONFIG, this.log);
                cb();
        }
});

after(function (cb) {
        if (this.client) {
                this.client.close();
        }
        cb();
});

test('single directory', function (t) {
        var self = this;
        var keys = [];
        var path = '/poseidon/stor/usage/storage/2015/10/10';
        var expected = [ path + '/00/h00.json' ];
        var keygen = mod_keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: path
                }
        });
        keygen.on('key', function (key) {
                keys.push(key);
        });
        keygen.on('end', function () {
                t.deepEqual(keys.sort(), expected.sort());
                t.end();
        });
        keygen.start();
});


test('single directory with regex', function (t) {
        var self = this;
        var keys = [];
        var expected = [];
        var path = '/poseidon/stor/usage/compute/2015/10/10';
        for (var i = 0; i < 24; i++) {
                expected.push(path + '/' + pad(i) + '/h' + pad(i) + '.json');
        }
        var keygen = mod_keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: path,
                        regex: 'h[0-9][0-9].json'
                }
        });
        keygen.on('key', function (key) {
                keys.push(key);
        });
        keygen.on('end', function () {
                t.deepEqual(keys.sort(), expected.sort());
                t.end();
        });
        keygen.start();
});


test('multiple directories', function (t) {
        var self = this;
        var keys = [];
        var expected = [];
        var path = [
                '/poseidon/stor/usage/compute/2015/10/10',
                '/poseidon/stor/usage/request/2015/10/10'
        ];
        var i;
        for (i = 0; i < 24; i++) {
                expected.push(path[0] + '/' + pad(i) + '/h' + pad(i) + '.json');
        }
        for (i = 0; i < 24; i++) {
                expected.push(path[1] + '/' + pad(i) + '/h' + pad(i) + '.json');
        }
        var keygen = mod_keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: path
                }
        });
        keygen.on('key', function (key) {
                keys.push(key);
        });
        keygen.on('end', function () {
                t.deepEqual(keys.sort(), expected.sort());
                t.end();
        });
        keygen.start();
});

test('multiple directories with regex', function (t) {
        var self = this;
        var keys = [];
        var expected = [];
        var path = [
                '/poseidon/stor/usage/compute/2015/10/10',
                '/poseidon/stor/usage/request/2015/10/10'
        ];
        var i;
        for (i = 0; i < 10; i++) {
                expected.push(path[0] + '/' + pad(i) + '/h' + pad(i) + '.json');
        }
        for (i = 0; i < 10; i++) {
                expected.push(path[1] + '/' + pad(i) + '/h' + pad(i) + '.json');
        }
        var keygen = mod_keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: path,
                        regex: 'h0[0-9].json'
                }
        });
        keygen.on('key', function (key) {
                keys.push(key);
        });
        keygen.on('end', function () {
                t.deepEqual(keys.sort(), expected.sort());
                t.end();
        });
        keygen.start();
});

test('minSize error', function (t) {
        var self = this;
        var errors = [];
        var keys = [];
        var path = [
                '/poseidon/stor/usage/request/2015/10/10'
        ];
        var keygen = mod_keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: path,
                        regex: 'h[0-9][0-9].json',
                        minSize: 7050000
                }
        });
        keygen.on('key', function (key) {
                keys.push(key);
        });
        keygen.on('error', function (err) {
                errors.push(err);
        });
        keygen.on('end', function () {
                t.equal(1, errors.length);
                t.equal(0, keys.length);
                t.end();
        });
        keygen.start();
});

function pad(x) {
        if (x < 10)
                return ('0' + x);
        else
                return (x);
}
