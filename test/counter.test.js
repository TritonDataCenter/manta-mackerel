// Copyright 2012 Joyent, Inc.  All rights reserved.

var fs = require('fs');

var carrier = require('carrier');
var deepEqual = require('deep-equal');

var mackerel = require('../lib');

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var test = helper.test;

var FILE_OPTS = {
        encoding: 'utf8'
};
var LOG = helper.createLogger();
var TEST_FILE1 = __dirname + '/sampledump1';
var TEST_FILE2 = __dirname + '/sampledump2';
var TEST_FILE3 = __dirname + '/sampledump3';
var TEST_FILE4 = __dirname + '/sampledump4';




///--- Tests

test('single customer', function (t) {
        var actual = {};
        var expect = {
                'fred': {
                        numKb: 40,
                        numKeys: 5
                }
        };

        var carry = carrier.carry(fs.createReadStream(TEST_FILE1, FILE_OPTS));

        carry.on('line', function onLine(line) {
                var record;

                try {
                        record = JSON.parse(line);
                } catch (e) {
                        LOG.fatal(e, 'invalid line');
                        t.notOk(e);
                        t.end();
                }

                t.ok(mackerel.processRecord({
                        aggregation: actual,
                        line: line,
                        log: LOG,
                        record: record
                }));
        });

        carry.once('end', function printResults() {
                t.ok(deepEqual(expect, actual));
                t.end();
        });
});
test('single customer with links', function (t) {
        var actual = {};
        var expect = {
                'fred': {
                        numKb: 24,
                        numKeys: 4
                }
        };

        var carry = carrier.carry(fs.createReadStream(TEST_FILE2, FILE_OPTS));

        carry.on('line', function onLine(line) {
                var record;

                try {
                        record = JSON.parse(line);
                } catch (e) {
                        LOG.fatal(e, 'invalid line');
                        t.notOk(e);
                        t.end();
                }

                t.ok(mackerel.processRecord({
                        aggregation: actual,
                        line: line,
                        log: LOG,
                        record: record
                }));
        });

        carry.once('end', function printResults() {
                t.ok(deepEqual(expect, actual));
                t.end();
        });
});
test('single customer with links and larger files', function (t) {
        var actual = {};
        var expect = {
                'fred': {
                        numKb: 40,
                        numKeys: 4
                }
        };

        var carry = carrier.carry(fs.createReadStream(TEST_FILE3, FILE_OPTS));

        carry.on('line', function onLine(line) {
                var record;

                try {
                        record = JSON.parse(line);
                } catch (e) {
                        LOG.fatal(e, 'invalid line');
                        t.notOk(e);
                        t.end();
                }

                t.ok(mackerel.processRecord({
                        aggregation: actual,
                        line: line,
                        log: LOG,
                        record: record
                }));
        });

        carry.once('end', function printResults() {
                t.ok(deepEqual(expect, actual));
                t.end();
        });
});
test('multiple customers', function (t) {
        var actual = {};
        var expect = {
                'fred1': {
                        numKb: 32,
                        numKeys: 3
                },
                'fred2': {
                        numKb: 16,
                        numKeys: 2
                }
        };

        var carry = carrier.carry(fs.createReadStream(TEST_FILE4, FILE_OPTS));

        carry.on('line', function onLine(line) {
                var record;

                try {
                        record = JSON.parse(line);
                } catch (e) {
                        LOG.fatal(e, 'invalid line');
                        t.notOk(e);
                        t.end();
                }

                t.ok(mackerel.processRecord({
                        aggregation: actual,
                        line: line,
                        log: LOG,
                        record: record
                }));
        });

        carry.once('end', function printResults() {
                t.ok(deepEqual(expect, actual));
                t.end();
        });
});
