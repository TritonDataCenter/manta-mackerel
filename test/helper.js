/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var lstream = require('lstream');

function streamTest(t, stream, input, expected, cb) {
    var actual = [];
    var lines = new lstream();

    stream.on('readable', function () {
        var chunk;
        var line = '';
        while (null !== (chunk = stream.read())) {
            line += chunk;
        }
        actual.push(JSON.parse(line));
    });
    stream.once('error', function (err) {
        cb(err);
    });
    stream.once('end', function () {
        if (!Array.isArray(expected)) {
            expected = [ expected ];
        }
        t.deepEqual(actual, expected, 'output matches');
        cb();
    });

    lines.pipe(stream);
    lines.end(input, 'utf8');
}

module.exports = {
    streamTest: streamTest
};
