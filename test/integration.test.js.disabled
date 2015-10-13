/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var events = require('events');
var manta = require('manta');
var meter = require('../lib/meter');
var once = require('once');
var fs = require('fs');
var mod_path = require('path');
var exec = require('child_process').exec;
var vasync = require('vasync');

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');

///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;
var LOOKUP_FILE = '/var/tmp/lookup.json';
var CONFIG = process.env.CONFIG ? require(process.env.CONFIG) :
        require('../etc/test-config.js');
var JOBS = require('../etc/jobs.json').jobs;


///--- Helpers

function upload(path, stream, cb) {
        var self = this;
        self.client.mkdirp(mod_path.dirname(path), function (err) {
                if (err) {
                        cb(err);
                        return;
                }
                self.client.put(path, stream, function (err2) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        cb();
                });
        });
}

// hack for testing:
// prepends the test user's login to each lookup user's login to sandbox tests
// to the test user's namespace, so usage reports that regularly get delivered
// to /bob/reports/report.json will go to
// /fred/stor/mackerel-test/users/bob/reports/report.json (with sponge as
// the test user)
var processLookups = once(function () {
        // get the original lookup file
        var lookup = require(mod_path.resolve(__dirname,
                '..', CONFIG.lookupFile));

        // write to temporary lookup file
        Object.keys(lookup).forEach(function (k) {
                lookup[k].login = CONFIG.mantaBaseDirectory + '/users/' +
                        lookup[k].login;
        });
        fs.writeFileSync(LOOKUP_FILE, JSON.stringify(lookup));

        CONFIG.assetOverrides = {}
        var lookupAsset = CONFIG.mantaBaseDirectory + '/assets/etc/lookup.json';
        CONFIG.assetOverrides[lookupAsset] = LOOKUP_FILE;

        // update where we look for the lookup file
        CONFIG.lookupFile = LOOKUP_FILE;
});


before(function (cb) {
        this.log = helper.createLogger();

        CONFIG.manta.log = this.log;
        this.client = manta.createClient(CONFIG.manta);

        processLookups();
        this.upload = upload.bind(this);
        cb();
});


after(function (cb) {
        var self = this;
        self.client.rmr(CONFIG.mantaBaseDirectory, function (err) {
                self.client.rmr(CONFIG.mantaBaseDirectory, function (err2) {
                        self.client.close();
                        cb();
                });
        });
});


test('storage', function (t) {
        var self = this;
        var job = JOBS['storage'];

        // some paths for records are hardcoded, so change them here
        job.keygenArgs.source = CONFIG.mantaBaseDirectory + '/dumps';

        var path = job.keygenArgs.source +
                '/2.shard/2013/02/25/23/manta-2013-02-25-23-00-03.gz';

        var stream = fs.createReadStream(__dirname +
                '/test_data/storage-raw.json.gz');
        stream.pause();
        self.upload(path, stream, function (err) {
                t.ifError(err);
                meter.meter({
                        config: CONFIG,
                        date: new Date(2013, 1, 25, 23),
                        jobConfig: job,
                        log: self.log
                }, function (err2, res) {
                        t.ifError(err2);
                        t.equal(1, res.outputs.length);
                        t.end();
                });
        });
});

test('request', function (t) {
        var self = this;
        var job = JOBS['request'];

        // some paths for records hardcoded, so change them here
        job.keygenArgs.source = CONFIG.mantaBaseDirectory + '/muskie' +
                '/$year/$month/$day/$hour';

        var path = CONFIG.mantaBaseDirectory +
                '/muskie/2013/02/25/23/fe590134.log';

        var stream = fs.createReadStream(__dirname +
                '/test_data/request-raw.json');
        stream.pause();
        this.upload(path, stream, function (err) {
                t.ifError(err);
                meter.meter({
                        config: CONFIG,
                        date: new Date(2013, 1, 25, 23),
                        jobConfig: job,
                        log: self.log
                }, function (err2, res) {
                        t.ifError(err2);
                        t.equal(1, res.outputs.length);
                        t.end();
                });
        });
});

test('compute', function (t) {
        var self = this;
        var job = JOBS['compute'];

        // some paths for records hardcoded, so change them here
        job.keygenArgs.source = CONFIG.mantaBaseDirectory + '/marlin-agent' +
                '/$year/$month/$day/$hour';

        var path = CONFIG.mantaBaseDirectory +
                '/marlin-agent/2013/02/25/23/RM391.log';

        var stream = fs.createReadStream(__dirname +
                '/test_data/compute-raw.json');
        stream.pause();
        this.upload(path, stream, function (err) {
                t.ifError(err);
                meter.meter({
                        config: CONFIG,
                        date: new Date(2013, 1, 25, 23),
                        jobConfig: job,
                        log: self.log
                }, function (err2, res) {
                        t.ifError(err2);
                        t.equal(1, res.outputs.length);
                        t.end();
                });
        });
});

test('summarizeDaily', function (t) {
        var self = this;
        var job = JOBS['summarizeDaily'];

        // some paths for records hardcoded, so change them here
        job.keygenArgs.source = [
                CONFIG.mantaBaseDirectory + '/storage/$year/$month/$day',
                CONFIG.mantaBaseDirectory + '/compute/$year/$month/$day',
                CONFIG.mantaBaseDirectory + '/request/$year/$month/$day'
        ];


        vasync.pipeline({
                funcs: [
                        function (_, cb) {
                                var path = CONFIG.mantaBaseDirectory +
                                        '/storage/2013/02/25/23/h23.json';
                                var stream = fs.createReadStream(__dirname +
                                        '/storage_sample/expected.stdout');
                                stream.pause();
                                self.upload(path, stream, function (err) {
                                        cb(err);
                                });
                        },
                        function (_, cb) {
                                var path = CONFIG.mantaBaseDirectory +
                                        '/compute/2013/02/25/23/h23.json';
                                var stream = fs.createReadStream(__dirname +
                                        '/compute_sample/expected.stdout');
                                stream.pause();
                                self.upload(path, stream, function (err) {
                                        cb(err);
                                });
                        },
                        function (_, cb) {
                                var path = CONFIG.mantaBaseDirectory +
                                        '/request/2013/02/25/23/h23.json';
                                var stream = fs.createReadStream(__dirname +
                                        '/request_sample/expected.stdout');
                                stream.pause();
                                self.upload(path, stream, function (err) {
                                        cb(err);
                                });
                        }
                ]
        }, function (err, results) {
                t.ifError(err);
                meter.meter({
                        config: CONFIG,
                        date: new Date(2013, 1, 25, 23),
                        jobConfig: job,
                        log: self.log
                }, function (err2, res) {
                        t.ifError(err2);
                        t.equal(1, res.outputs.length);
                        t.end();
                });
        });
});

test('accessLogs', function (t) {
        var self = this;
        var job = JOBS['accessLogs'];

        // some paths for records hardcoded, so change them here
        job.keygenArgs.source = CONFIG.mantaBaseDirectory + '/muskie' +
                '/$year/$month/$day/$hour';

        var path = CONFIG.mantaBaseDirectory +
                '/muskie/2013/02/25/23/fe590134.log';

        var stream = fs.createReadStream(__dirname +
                '/test_data/request-raw.json');
        stream.pause();
        this.upload(path, stream, function (err) {
                t.ifError(err);
                meter.meter({
                        config: CONFIG,
                        date: new Date(2013, 1, 25, 23),
                        jobConfig: job,
                        log: self.log
                }, function (err2, res) {
                        t.ifError(err2);
                        t.ok(res.outputs.length > 1);
                        t.end();
                });
        });
});

/*
test('storage - workflow', function (t) {
        // TODO
        t.end();
});
*/
