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
var TEST_FILE_SINGLE    = __dirname + '/sampledump-single';
var TEST_FILE_LINKS     = __dirname + '/sampledump-links';
var TEST_FILE_BIG_FILES = __dirname + '/sampledump-big';
var TEST_FILE_MULTIPLE  = __dirname + '/sampledump-multiple';



///--- Internal helper functions

function compare(expect, file, t) {
        var actual = {};

        mackerel.aggregate({
                stream: fs.createReadStream(file, FILE_OPTS),
                aggregation: actual,
                log: LOG,
                callback: function printResults() {
                        t.ok(deepEqual(expect, actual));
                        t.end();
                }
        });
}



///--- Tests

test('single customer', function (t) {
        var expect = {
                fred: {
                        numKb: 40,
                        numKeys: 5
                }
        };

        compare(expect, TEST_FILE_SINGLE, t);
});


test('single customer with links', function (t) {
        var expect = {
                fred: {
                        numKb: 24,
                        numKeys: 4
                }
        };

        compare(expect, TEST_FILE_LINKS, t);
});


test('single customer with links and larger files', function (t) {
        var expect = {
                fred: {
                        numKb: 40,
                        numKeys: 4
                }
        };

        compare(expect, TEST_FILE_BIG_FILES, t);
});


test('multiple customers', function (t) {
        var expect = {
                fred1: {
                        numKb: 32,
                        numKeys: 3
                },
                fred2: {
                        numKb: 16,
                        numKeys: 2
                }
        };

        compare(expect, TEST_FILE_MULTIPLE, t);
});
