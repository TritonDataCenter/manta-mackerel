/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');
var exec = require('child_process').exec;
var fs = require('fs');
var manta = require('manta');
var keygen = require('../lib/keygen/StorageHourlyKeyGenerator.js');
var PassThrough = require('readable-stream/passthrough.js');
var vasync = require('vasync');
var path = require('path');

var after = helper.after;
var before = helper.before;
var test = helper.test;

function putEmpties(paths, cb) {
        function putEmpty(p, cb) {
                var self = this;
                var mstream = new PassThrough();
                mstream.pause();
                self.client.mkdirp(path.dirname(p), function (err) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        self.client.put(p, mstream, {size: 0}, function (err2) {
                                if (err2) {
                                        cb(err2);
                                        return;
                                }
                                cb();
                        });
                        mstream.write('');
                        mstream.end();
                });

        }

        vasync.forEachParallel({
                func: putEmpty.bind(this),
                inputs: paths
        }, function (err) {
                if (err) {
                        cb(err);
                        return;
                }
                cb();
        });
}

before(function (cb) {
        var client;
        var self = this;
        this.putEmpties = putEmpties.bind(this);
        this.log = helper.createLogger();
        var f = process.env.SSH_KEY || process.env.HOME + '/.ssh/id_rsa';
        var cmd = 'ssh-keygen -l -f ' +
                f + ' ' +
                '| awk \'{print $2}\'';
        var url = process.env.MANTA_URL;
        var user = process.env.MANTA_USER;

        if (!process.env.MANTA_CONFIG) {
                var key = fs.readFileSync(f, 'utf8');
                this.testdir = '/' + user + '/stor/mackerel-test';
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
                                user: user
                        });

                        self.client.mkdir(self.testdir, function (err) {
                                if (err) {
                                        cb(err);
                                        return;
                                }
                                cb();
                        });
                });
        } else {
                this.client = manta.createClientFromFileSync(
                        process.env.MANTA_CONFIG, this.log);
                this.testdir = '/' + this.client.user + '/stor/mackerel-test';
                cb();
        }
});

after(function (cb) {
        var self = this;
        if (self.client) {
                self.client.rmr(self.testdir, function (err) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        self.client.close();
                        cb();
                });
        }
});


test('basic', function (t) {
        var self = this;
        var actualKeys = [];
        var errors = [];

        var keys = [
                self.testdir + '/1.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/2.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/3.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/4.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/1.shard/2013/07/05/00/manta-2013.gz',
                self.testdir + '/1.shard/2013/07/05/22/manta_2013.gz'
        ];

        var expected = keys.slice(0, 4);

        var gen = keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: self.testdir,
                        date: '2013-07-05T22:00:00'
                }
        });

        gen.on('key', function (k) {
                actualKeys.push(k);
        });

        gen.on('error', function (e) {
                errors.push(e);
        });

        gen.once('end', function () {
                t.deepEqual(expected.sort(), actualKeys.sort());
                t.equal(0, errors.length);
                t.done();
        });

        self.putEmpties(keys, function (err) {
                t.ifError(err);
                gen.start();
        });
});

test('missing dump', function (t) {
        var self = this;
        var actualKeys = [];
        var errors = [];

        var keys = [
                self.testdir + '/1.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/2.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/3.shard/2013/07/05/22/nodump',
                self.testdir + '/4.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/5.shard/2013/07/05/22/nodump'
        ];

        var expected = [
                self.testdir + '/1.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/2.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/4.shard/2013/07/05/22/manta-2013.gz'
        ];

        var gen = keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: self.testdir,
                        date: '2013-07-05T22:00:00'
                }
        });

        gen.on('key', function (k) {
                actualKeys.push(k);
        });

        gen.on('error', function (e) {
                errors.push(e);
        });

        gen.once('end', function () {
                t.deepEqual(expected.sort(), actualKeys.sort());
                t.equal(1, errors.length);
                t.done();
        });

        self.putEmpties(keys, function (err) {
                t.ifError(err);
                gen.start();
        });
});

test('missing directory', function (t) {
        var self = this;
        var actualKeys = [];
        var errors = [];

        var keys = [
                self.testdir + '/1.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/2.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/3.shard/1999',
                self.testdir + '/4.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/5.shard/2013/10/10/10/manta-2013.gz'
        ];

        var expected = [
                self.testdir + '/1.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/2.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/4.shard/2013/07/05/22/manta-2013.gz'
        ];

        var gen = keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: self.testdir,
                        date: '2013-07-05T22:00:00'
                }
        });

        gen.on('key', function (k) {
                actualKeys.push(k);
        });

        gen.on('error', function (e) {
                errors.push(e);
        });

        gen.once('end', function () {
                t.deepEqual(expected.sort(), actualKeys.sort());
                t.equal(1, errors.length);
                t.done();
        });

        self.putEmpties(keys, function (err) {
                t.ifError(err);
                gen.start();
        });

});

test('blacklist', function (t) {
        var self = this;
        var actualKeys = [];
        var errors = [];

        var keys = [
                self.testdir + '/1.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/2.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/3.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/4.shard/2013/07/05/22/manta-2013.gz'
        ];

        var expected = [
                self.testdir + '/1.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/2.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/4.shard/2013/07/05/22/manta-2013.gz'
        ];

        var gen = keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: self.testdir,
                        date: '2013-07-05T22:00:00',
                        shardBlacklist: ['3']
                }
        });

        gen.on('key', function (k) {
                actualKeys.push(k);
        });

        gen.on('error', function (e) {
                errors.push(e);
        });

        gen.once('end', function () {
                t.deepEqual(expected.sort(), actualKeys.sort());
                t.equal(0, errors.length);
                t.done();
        });

        self.putEmpties(keys, function (err) {
                t.ifError(err);
                gen.start();
        });

});

test('minSize error', function (t) {
        var self = this;
        var actualKeys = [];
        var errors = [];

        var keys = [
                self.testdir + '/1.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/2.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/3.shard/2013/07/05/22/manta-2013.gz',
                self.testdir + '/4.shard/2013/07/05/22/manta-2013.gz'
        ];

        var expected = [];

        var gen = keygen.keygen({
                client: self.client,
                log: helper.createLogger(),
                args: {
                        source: self.testdir,
                        date: '2013-07-05T22:00:00',
                        minSize: 1,
                        shardBlacklist: ['3']
                }
        });

        gen.on('key', function (k) {
                actualKeys.push(k);
        });

        gen.on('error', function (e) {
                errors.push(e);
        });

        gen.once('end', function () {
                t.deepEqual(expected.sort(), actualKeys.sort());
                t.equal(1, errors.length);
                t.done();
        });

        self.putEmpties(keys, function (err) {
                t.ifError(err);
                gen.start();
        });


});
