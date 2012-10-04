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

        var imack = fun({
                input: fs.createReadStream(file, FILE_OPTS),
                log: LOG
        });

        t.expect(1);

        imack.on('error', function () { t.ok(false); });

        imack.on('end', function verify(actual) {
                t.deepEqual(expect, actual, 'actual: ' + actual);
                t.done();
        });

}

///--- Tests

test('map: single customer', function (t) {
        var expect = [ {
                owner: 'fred',
                numKb: 40,
                numKeys: 5
        } ];

        compare(expect, TEST_FILE_SINGLE, mackerel.createMapReader, t);
});


test('map: single customer with links', function (t) {
        var expect = [ {
                owner: 'fred',
                numKb: 24,
                numKeys: 4
        } ];

        compare(expect, TEST_FILE_LINKS, mackerel.createMapReader, t);
});


test('map: single customer with links and larger files', function (t) {
        var expect = [ {
                owner: 'fred',
                numKb: 40,
                numKeys: 4
        } ];

        compare(expect, TEST_FILE_BIG_FILES, mackerel.createMapReader, t);
});


test('map: multiple customers', function (t) {
        var expect = [
                {
                        owner: 'fred1',
                        numKb: 32,
                        numKeys: 3
                },
                {
                        owner: 'fred2',
                        numKb: 16,
                        numKeys: 2
                }
        ];

        compare(expect, TEST_FILE_MULTIPLE, mackerel.createMapReader, t);
});

test('reduce', function (t) {
        var expect = [
                {
                        owner: 'fred',
                        numKb: 104,
                        numKeys: 13
                },
                {
                        owner: 'fred1',
                        numKb: 32,
                        numKeys: 3
                },
                {
                        owner: 'fred2',
                        numKb: 16,
                        numKeys: 2
                }
        ];

        compare(expect, TEST_FILE_MAP_OUT, mackerel.createReduceReader, t);
});
