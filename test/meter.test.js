// Copyright (c) 2013, Joyent, Inc. All rights reserved.

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
var config = process.env.CONFIG ? require(process.env.CONFIG) :
        require('../etc/test-config.js');
var jobs = require('../etc/jobs.json');
var lookupFile = mod_path.resolve(__dirname, '..', jobs.lookupFile);

function upload(path, stream, cb) {
        var self = this;
        this.client.mkdirp(mod_path.dirname(path), function (err) {
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
// /sponge/stor/mackerel-test/userdirs/bob/reports/report.json (with sponge as
// the test user)
var processLookups = once(function (user) {
        var lookup = require('./test_data/lookup.json');
        Object.keys(lookup).forEach(function (k) {
                lookup[k] = user + '/stor/mackerel-test/userdirs/' + lookup[k];
        });
        fs.writeFileSync(lookupFile, JSON.stringify(lookup));
});


///--- Tests
test('generate lookup', function (t) {
        meter.generateLookup({
                host: config.mahi.host,
                port: config.mahi.port,
                log: this.log
        }, function (err, result) {
                t.ifError(err);
                t.ok(result);
                t.done();
        });
});


test('floorDate', function (t) {
        //                   yyyy  MM  dd  HH  mm  ss
        var time  = new Date(2000,  0, 15, 14, 13, 12);
        var hour  = new Date(2000,  0, 15, 14,  0,  0);
        var day   = new Date(2000,  0, 15,  0,  0,  0);
        var month = new Date(2000,  0,  1,  0,  0,  0);
        t.equal(hour.valueOf(), meter.floorDate(time, 'hourly').valueOf());
        t.equal(day.valueOf(), meter.floorDate(time, 'daily').valueOf());
        t.equal(month.valueOf(), meter.floorDate(time, 'monthly').valueOf());
        t.end();
});

before(function (cb) {
        this.log = helper.createLogger();
        var self = this;

        config.manta.log = this.log;
        this.client = manta.createClient(config.manta);

        this.dir = '/' + this.client.user + '/stor/mackerel-test';
        processLookups(this.client.user);
        this.upload = upload.bind(this);
        cb();
});


after(function (cb) {
        var self = this;
        this.client.rmr(this.dir, function (err) {
                self.client.rmr(jobs.mantaBaseDirectory, function (err2) {
                        self.client.close();
                        cb();
                });
        });
});


test('storage hourly', function (t) {
        console.warn('This next one takes a while...');
        var self = this;
        var job = jobs.jobs.storage.hourly;

        // source paths for records are hardcoded, so change them here
        job.keygenArgs.source = this.dir + '/storage-hourly';

        var path = job.keygenArgs.source +
                '/shard1/2013/02/25/23/manta-2013-02-25-23-00-03.gz';

        var stream = fs.createReadStream(__dirname +
                '/test_data/storage-raw.json.gz');
        stream.pause();
        this.upload(path, stream, function (err) {
                t.ifError(err);
                meter.meter({
                        date: new Date(2013, 1, 25, 23),
                        category: 'storage',
                        period: 'hourly',
                        config: config,
                        jobs: jobs,
                        log: self.log
                }, function (err2, res) {
                        t.ifError(err2);
                        t.ok(res);
                        t.equals(0, res.errors.length);
                        t.equals(0, res.failures.length);
                        t.equals(1, res.outputs.length);
                        t.end();
                });
        });
});


test('storage daily', function (t) {
        var self = this;
        var job = jobs.jobs.storage.daily;

        // source paths for records are hardcoded, so change them here
        job.keygenArgs.source = this.dir + '/storage-daily/$year/$month/$day';

        var path1 = this.dir + '/storage-daily/2013/02/25/23/h23.json';
        var path2 = this.dir + '/storage-daily/2013/02/25/24/h24.json';

        var stream1 = fs.createReadStream(__dirname +
                '/test_data/storage-hour.json');
        var stream2 = fs.createReadStream(__dirname +
                '/test_data/storage-hour.json');
        stream1.pause();
        stream2.pause();
        this.upload(path1, stream1, function (err) {
                t.ifError(err);
                self.upload(path2, stream2, function (err2) {
                        t.ifError(err2);
                        meter.meter({
                                date: new Date(2013, 1, 25, 23),
                                category: 'storage',
                                period: 'daily',
                                config: config,
                                jobs: jobs,
                                log: self.log
                        }, function (err3, res) {
                                t.ifError(err3);
                                t.ok(res);
                                t.equals(0, res.errors.length);
                                t.equals(0, res.failures.length);
                                t.equals(1, res.outputs.length);
                                t.end();
                        });
                });
        });
});



test('storage monthly', function (t) {
        var self = this;
        var job = jobs.jobs.storage.monthly;

        // source paths for records are hardcoded, so change them here
        job.keygenArgs.source = this.dir + '/storage-monthly/$year/$month';

        var path1 = this.dir + '/storage-monthly/2013/02/25/d25.json';
        var path2 = this.dir + '/storage-monthly/2013/02/26/d26.json';

        var stream1 = fs.createReadStream(__dirname +
                '/test_data/storage-day.json');
        var stream2 = fs.createReadStream(__dirname +
                '/test_data/storage-day.json');
        stream1.pause();
        stream2.pause();
        this.upload(path1, stream1, function (err) {
                t.ifError(err);
                self.upload(path2, stream2, function (err2) {
                        t.ifError(err2);
                        meter.meter({
                                date: new Date(2013, 1, 25, 23),
                                category: 'storage',
                                period: 'monthly',
                                config: config,
                                jobs: jobs,
                                log: self.log
                        }, function (err3, res) {
                                t.ifError(err3);
                                t.ok(res);
                                t.equals(0, res.errors.length);
                                t.equals(0, res.failures.length);
                                t.equals(1, res.outputs.length);
                                t.end();
                        });
                });
        });
});

test('request hourly', function (t) {
        var self = this;
        var job = jobs.jobs.request.hourly;

        // source paths for records are hardcoded, so change them here
        job.keygenArgs.source = this.dir +
                '/request-hourly/$year/$month/$day/$hour';

        var path = this.dir + '/request-hourly/2013/02/25/23/fe590134.log';

        var stream = fs.createReadStream(__dirname +
                '/test_data/request-raw.json');
        stream.pause();
        this.upload(path, stream, function (err) {
                t.ifError(err);
                meter.meter({
                        date: new Date(2013, 1, 25, 23),
                        category: 'request',
                        period: 'hourly',
                        config: config,
                        jobs: jobs,
                        log: self.log
                }, function (err3, res) {
                        t.ifError(err3);
                        t.ok(res);
                        t.equals(0, res.errors.length);
                        t.equals(0, res.failures.length);
                        t.equals(1, res.outputs.length);
                        t.end();
                });
        });
});

test('request daily', function (t) {
        var self = this;
        var job = jobs.jobs.request.daily;

        // source paths for records are hardcoded, so change them here
        job.keygenArgs.source = this.dir + '/request-daily/$year/$month/$day';

        var path1 = this.dir + '/request-daily/2013/02/25/23/h23.json';
        var path2 = this.dir + '/request-daily/2013/02/25/24/h24.json';

        var stream1 = fs.createReadStream(__dirname +
                '/test_data/request-hour.json');
        var stream2 = fs.createReadStream(__dirname +
                '/test_data/request-hour.json');
        stream1.pause();
        stream2.pause();
        this.upload(path1, stream1, function (err) {
                t.ifError(err);
                self.upload(path2, stream2, function (err2) {
                        t.ifError(err2);
                        meter.meter({
                                date: new Date(2013, 1, 25, 23),
                                category: 'request',
                                period: 'daily',
                                config: config,
                                jobs: jobs,
                                log: self.log
                        }, function (err3, res) {
                                t.ifError(err3);
                                t.ok(res);
                                t.equals(0, res.errors.length);
                                t.equals(0, res.failures.length);
                                t.equals(1, res.outputs.length);
                                t.end();
                        });
                });
        });
});

test('request monthly', function (t) {
        var self = this;
        var job = jobs.jobs.request.monthly;

        // source paths for records are hardcoded, so change them here
        job.keygenArgs.source = this.dir + '/request-monthly/$year/$month';

        var path1 = this.dir + '/request-monthly/2013/02/25/d25.json';
        var path2 = this.dir + '/request-monthly/2013/02/26/d26.json';

        var stream1 = fs.createReadStream(__dirname +
                '/test_data/request-day.json');
        var stream2 = fs.createReadStream(__dirname +
                '/test_data/request-day.json');
        stream1.pause();
        stream2.pause();
        this.upload(path1, stream1, function (err) {
                t.ifError(err);
                self.upload(path2, stream2, function (err2) {
                        t.ifError(err2);
                        meter.meter({
                                date: new Date(2013, 1, 25, 23),
                                category: 'request',
                                period: 'monthly',
                                config: config,
                                jobs: jobs,
                                log: self.log
                        }, function (err3, res) {
                                t.ifError(err3);
                                t.ok(res);
                                t.equals(0, res.errors.length);
                                t.equals(0, res.failures.length);
                                t.equals(1, res.outputs.length);
                                t.end();
                        });
                });
        });
});

test('compute hourly', function (t) {
        // TODO get raw data
        t.end();
        /*
        var self = this;
        var job = jobs.jobs.compute.hourly;

        // source paths for records are hardcoded, so change them here
        job.keygenArgs.source = this.dir +
                '/compute-hourly/$year/$month/$day/$hour';

        var path = this.dir + '/compute-hourly/2013/02/25/23/fe590134.log';

        var stream = fs.createReadStream(__dirname +
                '/test_data/compute-raw.json');
        stream.pause();
        this.upload(path, stream, function (err) {
                t.ifError(err);
                meter.meter({
                        date: new Date(2013, 1, 25, 23),
                        category: 'compute',
                        period: 'hourly',
                        config: config,
                        log: self.log
                }, function (err3, res) {
                        t.ifError(err3);
                        t.ok(res);
                        t.equals(0, res.errors.length);
                        t.equals(0, res.failures.length);
                        t.equals(1, res.outputs.length);
                        t.end();
                });
        });
        */
});

test('compute daily', function (t) {
        // TODO get raw data
        t.end();
});

test('compute monthly', function (t) {
        // TODO get raw data
        t.end();
});

test('workflow (using storage daily)', function (t) {
        // TODO
        t.end();
});
