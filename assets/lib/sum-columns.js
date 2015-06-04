#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var Big = require('big.js');
var Transform = require('stream').Transform;
var bunyan = require('bunyan');
var lstream = require('lstream');
var util = require('util');

function getAggKey(obj) {
    var key = '';
    Object.keys(obj).forEach(function (k) {
        if (typeof (obj[k]) === 'string') {
            key += obj[k];
        }
    });
    return (key);
}


function copyProperties(from, to) {
    Object.keys(from).forEach(function (k) {
        var i;
        if (typeof (to[k]) === 'undefined') {
            if (Array.isArray(from[k])) {
                to[k] = new Array(from[k].length);
                for (i = 0; i < from[k].length; i++) {
                    copyProperties(from[k], to[k]);
                }
            } else if (from[k] instanceof Big) {
                to[k] = new Big(0);
            } else if (typeof (from[k]) === 'object') {
                to[k] = to[k] || {};
                copyProperties(from[k], to[k]);
            } else if (typeof (from[k]) === 'number') {
                to[k] = 0;
            } else {
                to[k] = from[k];
            }
        } else if (typeof (to[k]) === 'object') {
            copyProperties(from[k], to[k]);
        }
    });
}


// assumes oldvalue and newvalue have the same structure
// oldvalue += newvalue;
function plusEquals(oldvalue, newvalue) {
    Object.keys(newvalue).forEach(function (k) {
        if (newvalue[k] instanceof Big) {
            oldvalue[k] = oldvalue[k].plus(newvalue[k]);
        } else if (typeof (newvalue[k]) === 'object') {
            plusEquals(oldvalue[k], newvalue[k]);
        } else if (typeof (newvalue[k]) === 'number') {
            oldvalue[k] += newvalue[k];
        }
    });
}


function parse(key, value) {
    if (key === '') {
        return (value);
    }

    if (typeof (value) === 'string') {
        try {
            return (new Big(value));
        } catch (e) {
            return (value);
        }
    }
    return (value);
}

function stringify(key, value) {
    if (value instanceof Big || typeof (value) === 'number') {

        return (value.toString());
    }
    return (value);
}

function SumColumnStream(opts) {
    this.log = opts.log;
    this.aggr = {};
    this.lineNumber = 0;
    opts.decodeStrings = false;
    Transform.call(this.opts);
}
util.inherits(SumColumnStream, Transform);

SumColumnStream.prototype._transform = function _transform(line, enc, cb) {
    this.lineNumber++;

    var parsed;
    try {
        parsed = JSON.parse(line, parse);
    } catch (e) {
        this.log.error({
            error: e.message,
            line: line,
            lineNumber: this.lineNumber
        }, 'error parsing line');
        cb(e);
        return;
    }

    var aggKey = getAggKey(parsed);
    if (!this.aggr[aggKey]) {
        this.aggr[aggKey] = parsed;
    } else {
        copyProperties(parsed, this.aggr[aggKey]);
        plusEquals(this.aggr[aggKey], parsed);
    }
};

SumColumnStream.prototype._flush = function _flush(cb) {
    var self = this;
    Object.keys(this.aggr).forEach(function (k) {
        self.push(JSON.stringify(self.aggr[k], stringify) + '\n');
    });
    cb();
};


function main() {
    var log = bunyan.createLogger({
        name: 'sum-columns.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var sumStream = new SumColumnStream({
        log: log
    });

    sumStream.once('error', function (error) {
        log.error({error: error}, 'sum-columns error');
        process.abort();
    });

    process.stdin.pipe(new lstream()).pipe(sumStream).pipe(process.stdout);
}


if (require.main === module) {
    main();
}

module.exports = SumColumnStream;
