// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var events = require('events');
var exec = require('child_process').exec;
var manta = require('manta');
var jobrunner = require('../lib/jobrunner');
var fs = require('fs');

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');


///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;


///--- Tests

before(function (cb) {
        var client;
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
                this.testdir = '/' + this.client.user + '/stor/jobrunner-test';
                this.client.mkdir(this.testdir, function (err) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        cb();
                });
        }
});


after(function (cb) {
        var self = this;
        if (this.client) {
                this.client.rmr(this.testdir, function (err) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        self.client.close();
                        cb();
                });
        } else {
                cb();
        }
});


test('uploadAssets', function (t) {
        t.expect(3);
        var self = this;

        var job = {
                name: 'test-jobrunner-uploadassets1',
                phases: [ {
                        assets: [ this.testdir + '/asset1' ],
                        exec: 'echo hello'
                } ]
        };

        var assets = {};
        assets[ this.testdir + '/asset1' ] = module.filename;

        jobrunner.uploadAssets({
                assets: assets,
                jobManifest: job,
                client: this.client,
                log: this.log
        }, function (err) {
                t.ifError(err);
                Object.keys(assets).forEach(function (k) {
                        self.client.info(k, function (err2, info) {
                                t.ifError(err2);
                                t.ok(info);
                                t.end();
                        });
                });
        });
});


test('uploadAssets: missing asset entry', function (t) {
        t.expect(4);
        var self = this;

        var asset1 = this.testdir + '/asset1';
        var asset2 = this.testdir + '/asset2';
        var job = {
                name: 'test-jobrunner-uploadassets2',
                phases: [ {
                        assets: [
                                asset1,
                                asset2
                        ],
                        exec: 'echo hello'
                } ]
        };

        var assets = {};
        assets[asset1] = module.filename;

        jobrunner.uploadAssets({
                assets: assets,
                jobManifest: job,
                client: this.client,
                log: this.log
        }, function (err) {
                t.ok(err);

                self.client.info(asset2, function (err2, info) {
                        t.ok(err2);
                });

                Object.keys(assets).forEach(function (k) {
                        self.client.info(k, function (err2, info) {
                                t.ifError(err2);
                                t.ok(info);
                                t.end();
                        });
                });
        });
});

test('uploadAssets: unmodified asset', function (t) {
        t.expect(8);
        var self = this;

        function putBoth(cb) {
                jobrunner.uploadAssets({
                        assets: assets,
                        jobManifest: job,
                        client: self.client,
                        log: self.log
                }, function (err) {
                        t.ifError(err);
                        cb();
                });
        }

        function infoBoth(cb) {
                self.client.info(asset1, function (err, info) {
                        t.ifError(err);
                        etag1 = info.etag;
                        self.client.info(asset2, function (err2, info2) {
                                t.ifError(err2);
                                etag2 = info2.etag;
                                cb();
                        });
                });
        }

        function modifyOne(cb) {
                self.client.put(asset2, stream, function (err) {
                        t.ifError(err);
                        cb();
                });
        }

        function compareTags() {
                self.client.info(asset1, function (err, info) {
                        t.ifError(err);
                        t.equals(info.etag, etag1);
                        self.client.info(asset2, function (err2, info2) {
                                t.ifError(err2);
                                t.notEqual(info2.etag, etag2);
                                t.end();
                        });
                });
        }

        var asset1 = this.testdir + '/asset1';
        var asset2 = this.testdir + '/asset2';
        var job = {
                name: 'test-jobrunner-uploadassetstest3',
                phases: [ {
                        assets: [
                                asset1,
                                asset2
                        ],
                        exec: 'echo hello'
                } ]
        };

        var assets = {};
        assets[asset1] = module.filename;
        assets[asset2] = __dirname + '/helper.js';

        var stream = fs.createReadStream(module.filename);
        stream.pause();
        var etag1, etag2;

        putBoth(function () {
                infoBoth(function () {
                        modifyOne(function () {
                                jobrunner.uploadAssets({
                                        assets: assets,
                                        jobManifest: job,
                                        client: self.client,
                                        log: self.log
                                }, compareTags);
                        });
                });
        });
});

test('createJob & endJobInput', function (t) {
        t.expect(4);
        var self = this;
        var job = {
                name: 'test-jobrunner-createjob',
                phases: [ {
                        exec: 'echo hello'
                } ]
        };

        jobrunner.createJob({
                jobManifest: job,
                log: this.log,
                client: this.client
        }, function (err, jobPath) {
                t.ifError(err);
                t.ok(jobPath);
                jobrunner.endJobInput({
                        jobPath: jobPath,
                        log: self.log,
                        client: self.client
                }, function (err2, jobPath2) {
                        t.ifError(err2);
                        t.equals(jobPath, jobPath2);
                        t.end();
                });
        });
});


test('addInputKeys', function (t) {
        t.expect(9);
        var self = this;
        var asset1 = this.testdir + '/asset1';
        var asset2 = this.testdir + '/asset2';

        var job = {
                name: 'test-jobrunner-addinputkeys',
                phases: [ {
                        assets: [
                                asset1,
                                asset2
                        ],
                        exec: 'echo hello'
                } ]
        };
        var keygen = new events.EventEmitter();
        keygen.start = (function (keys) {
                for (var i = 0; i < keys.length; i++) {
                        this.emit('key', keys[i]);
                }
                this.emit('end');
        }).bind(keygen, job.phases[0].assets);

        jobrunner.createJob({
                jobManifest: job,
                log: this.log,
                client: this.client
        }, function (err, jobPath) {
                t.ifError(err);
                t.ok(jobPath);
                jobrunner.addInputKeys({
                        keygen: keygen,
                        jobPath: jobPath,
                        log: self.log,
                        client: self.client
                }, function (err2, jobPath2, count) {
                        t.ifError(err2);
                        t.equals(jobPath, jobPath2);
                        t.equals(2, count);
                        self.client.jobInput(jobPath, function (err3, res) {
                                var c = 0;
                                res.on('key', function (key) {
                                        t.ok(key === asset1 || key === asset2);
                                        c++;
                                });

                                res.on('end', function () {
                                        t.equals(2, c);
                                        self.client.endJob(jobPath,
                                                function (err4) {
                                                        t.ifError(err4);
                                                        t.end();
                                        });
                                });
                        });
                });
        });
});

test('addInputKeys: malformed key', function (t) {
        t.expect(5);
        var self = this;
        var asset1 =  'asset1';
        var asset2 = this.testdir + '/asset2';

        var job = {
                name: 'test-jobrunner-addinputkeys',
                phases: [ {
                        assets: [
                                asset1,
                                asset2
                        ],
                        exec: 'echo hello'
                } ]
        };
        var keygen = new events.EventEmitter();
        keygen.start = (function (keys) {
                for (var i = 0; i < keys.length; i++) {
                        this.emit('key', keys[i]);
                }
                this.emit('end');
        }).bind(keygen, job.phases[0].assets);

        jobrunner.createJob({
                jobManifest: job,
                log: this.log,
                client: this.client
        }, function (err, jobPath) {
                t.ifError(err);
                t.ok(jobPath);
                try {
                        jobrunner.addInputKeys({
                                keygen: keygen,
                                jobPath: jobPath,
                                log: self.log,
                                client: self.client
                        }, function (err2, jobPath2, count) {
                                t.ok(false);
                                t.end();
                        });
                } catch (e) {
                        t.ok(e);
                        t.equals('InvalidPathError', e.name);
                        self.client.endJob(jobPath, function (err2) {
                                t.ifError(err2);
                                t.end();
                        });
                }
        });
});


test('monitorJob', function (t) {
        t.expect(5);
        var self = this;

        var asset1 = this.testdir + '/asset1';

        var job = {
                name: 'test-jobrunner-monitorjob',
                phases: [ {
                        assets: [
                                asset1
                        ],
                        exec: 'sleep 30'
                } ]
        };

        var keygen = new events.EventEmitter();
        keygen.start = (function (keys) {
                for (var i = 0; i < keys.length; i++) {
                        this.emit('key', keys[i]);
                }
                this.emit('end');
        }).bind(keygen, job.phases[0].assets);

        jobrunner.createJob({
                jobManifest: job,
                log: this.log,
                client: this.client
        }, function (err, jobPath) {
                t.ifError(err);
                jobrunner.addInputKeys({
                        keygen: keygen,
                        jobPath: jobPath,
                        log: self.log,
                        client: self.client
                }, function (err2, _, count) {
                        t.ifError(err2);
                        t.equals(1, count);
                        jobrunner.endJobInput({
                                jobPath: jobPath,
                                log: self.log,
                                client: self.client
                        }, function (err3) {
                                t.ifError(err3);
                                jobrunner.monitorJob({
                                        jobPath: jobPath,
                                        log: self.log,
                                        client: self.client
                                }, function (err4) {
                                        t.ifError(err4);
                                        t.end();
                                });
                        });
                });
        });
});

test('monitorJob: expire', function (t) {
        t.expect(6);
        var self = this;

        var asset1 = this.testdir + '/asset1';
        var assets = {};
        assets[asset1] = module.filename;

        var job = {
                name: 'test-jobrunner-monitorjob',
                phases: [ {
                        assets: [
                                asset1
                        ],
                        exec: 'sleep 60'
                } ]
        };

        var keygen = new events.EventEmitter();
        keygen.start = (function (keys) {
                for (var i = 0; i < keys.length; i++) {
                        this.emit('key', keys[i]);
                }
                this.emit('end');
        }).bind(keygen, job.phases[0].assets);

        jobrunner.uploadAssets({
                jobManifest: job,
                log: this.log,
                client: this.client,
                assets: assets
        }, function (e) {
                t.ifError(e);
                jobrunner.createJob({
                        jobManifest: job,
                        log: self.log,
                        client: self.client
                }, function (err, jobPath) {
                        t.ifError(err);
                        jobrunner.addInputKeys({
                                keygen: keygen,
                                jobPath: jobPath,
                                log: self.log,
                                client: self.client
                        }, function (err2, _, count) {
                                t.ifError(err2);
                                t.equals(1, count);
                                jobrunner.endJobInput({
                                        jobPath: jobPath,
                                        log: self.log,
                                        client: self.client
                                }, function (err3) {
                                        t.ifError(err3);
                                        jobrunner.monitorJob({
                                                jobPath: jobPath,
                                                log: self.log,
                                                client: self.client,
                                                monitorBackoff: {
                                                        initialDelay: 100,
                                                        maxDelay: 200,
                                                        failAfter: 3
                                                }
                                        }, function (err4) {
                                                t.ok(err4);
                                                t.end();
                                        });
                                });
                        });
                });
        });
});


test('doJob', function (t) {
        t.expect(6);
        var asset1 = this.testdir + '/asset1';
        var assets = {};
        assets[asset1] = module.filename;

        var job = {
                name: 'test-jobrunner-dojob',
                phases: [ {
                        assets: [
                                asset1
                        ],
                        exec: 'echo hello'
                } ]
        };

        var keygen = new events.EventEmitter();
        keygen.start = (function (keys) {
                for (var i = 0; i < keys.length; i++) {
                        this.emit('key', keys[i]);
                }
                this.emit('end');
        }).bind(keygen, job.phases[0].assets);

        jobrunner.doJob({
                assets: assets,
                jobManifest: job,
                keygen: keygen,
                log: this.log,
                client: this.client
        }, function (err, result) {
                t.ifError(err);
                t.ok(result);
                t.ok(result.jobPath);
                t.equals(0, result.errors.length);
                t.equals(0, result.failures.length);
                t.equals(1, result.outputs.length);
                t.end();
        });
});
