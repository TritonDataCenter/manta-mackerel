// Copyright 2012 Joyent, Inc.  All rights reserved.
var counter = require('../lib/counter');
var helper = require('./helper.js');
var deepEqual = require('deep-equal');
var fs = require('fs');
var MemoryStream = require('memorystream');

var after = helper.after;
var before = helper.before;
var test = helper.test;

test('single line', function(t) {
        var expected = {
                customer: "fred",
                numKB: 20,
                numKeys: 5
        };

        var output = new MemoryStream();
        var outputString = '';
        var input = fs.ReadStream('./test/sampledump');
        input.setEncoding('utf8');

        output.on('data', function append(data) {
                outputString += data;
        });

        output.on('end', function compare() {
                var actual = JSON.parse(outputString);
                t.ok(deepEqual(expected, actual));
                t.end();
        });
        counter.main(input, output);

});
