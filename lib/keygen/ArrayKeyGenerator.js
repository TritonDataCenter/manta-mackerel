/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_events = require('events');
var mod_manta = require('manta');
var mod_util = require('util');
var mod_vasync = require('vasync');

function ArrayKeyGenerator(opts) {
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'log');
    mod_assert.object(opts.args, 'args');
    mod_assert.arrayOfString(opts.args.array, 'args.array');

    this.log = opts.log;
    this.array = opts.args.array;
}

mod_util.inherits(ArrayKeyGenerator, mod_events.EventEmitter);

ArrayKeyGenerator.prototype.start = function start() {
    var self = this;

    for (var i = 0; i < self.array.length; i++) {
        self.emit('key', self.array[i]);
    }

    self.emit('end');
};

module.exports.keygen = function (opts) {
    return (new ArrayKeyGenerator(opts));
};


if (require.main === module) {
    var config = require('../../etc/config.js');
    var log = mod_bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'mackerel',
        stream: process.stdout
    });

    var keygen = new ArrayKeyGenerator({
        log: log,
        args: {
            array: process.argv.slice(2)
        }
    });

    keygen.on('key', console.log.bind(null));
    keygen.start();
}
