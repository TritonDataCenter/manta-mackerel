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
var TEST_FILE_MAP_OUT   = __dirname + '/mapout-all';



///--- Internal helper functions

function compare(expect, file, fun, t) {
        var actual = {};

        mackerel.aggregate({
                stream: fs.createReadStream(file, FILE_OPTS),
                aggregation: actual,
                aggregationFunction: fun,
                log: LOG,
                callback: function printResults() {
                        t.ok(deepEqual(expect, actual));
                        t.end();
                }
        });
}

///--- Tests

test('map: single customer', function (t) {
        var expect = {
                fred: {
                        numKb: 40,
                        numKeys: 5
                }
        };

        compare(expect, TEST_FILE_SINGLE, mackerel.mapFunction, t);
});


test('map: single customer with links', function (t) {
        var expect = {
                fred: {
                        numKb: 24,
                        numKeys: 4
                }
        };

        compare(expect, TEST_FILE_LINKS, mackerel.mapFunction, t);
});


test('map: single customer with links and larger files', function (t) {
        var expect = {
                fred: {
                        numKb: 40,
                        numKeys: 4
                }
        };

        compare(expect, TEST_FILE_BIG_FILES, mackerel.mapFunction, t);
});


test('map: multiple customers', function (t) {
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

        compare(expect, TEST_FILE_MULTIPLE, mackerel.mapFunction, t);
});

test('reduce', function (t) {
        var expect = {
                fred: {
                        numKb: 104,
                        numKeys: 13
                },
                fred1: {
                        numKb: 32,
                        numKeys: 3
                },
                fred2: {
                        numKb: 16,
                        numKeys: 2
                }
        };

        compare(expect, TEST_FILE_MAP_OUT, mackerel.reduceFunction, t);
});
