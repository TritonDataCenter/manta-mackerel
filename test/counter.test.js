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
var TEST_FILE = __dirname + '/sampledump';



///--- Tests

test('single line', function (t) {
        var actual = {};
        var expect = {
                'fred': {
                        numKb: 20,
                        numKeys: 5
                }
        };

        var carry = carrier.carry(fs.createReadStream(TEST_FILE, FILE_OPTS));

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

        carry.on('end', function printResults() {
                t.ok(deepEqual(expect, actual));
                t.end();
        });
});
