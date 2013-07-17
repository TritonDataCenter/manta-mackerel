// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_path = require('path');
if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');

var generateLookup = require('../lib/generateLookup');

///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var config = process.env.CONFIG ? require(process.env.CONFIG) :
        require('../etc/test-config.js');

var lookupFile = mod_path.resolve(__dirname, '..', config.lookupFile);

test('generateLookup', function (t) {
    generateLookup({
        host: config.mahi.host,
        port: config.mahi.port,
        log: helper.createLogger()
    }, function (err, result) {
        t.ifError(err);
        t.ok(result);
        var firstKey = Object.keys(result)[0];
        t.equal(typeof (result[firstKey].login), 'string');
        t.equal(typeof (result[firstKey].approved), 'boolean');
        t.end()
    });
});
