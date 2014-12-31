#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_carrier = require('carrier');
var Big = require('big.js');
var lineCount = 0;
var ERROR = false;
var LOG = require('bunyan').createLogger({
    name: 'sum-columns.js',
    stream: process.stderr,
    level: process.env['LOG_LEVEL'] || 'info'
});

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


function onLine(aggr, line) {
    lineCount++;

    var parsed;
    try {
        parsed = JSON.parse(line, function (key, value) {
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
        });
    } catch (e) {
        LOG.error(e, 'Error on line ' + lineCount);
        ERROR = true;
        return;
    }

    var aggKey = getAggKey(parsed);

    if (!aggr[aggKey]) {
        aggr[aggKey] = parsed;
    } else {
        copyProperties(parsed, aggr[aggKey]);
        plusEquals(aggr[aggKey], parsed);
    }
}


function onEnd(aggr) {
    Object.keys(aggr).forEach(function (k) {
        console.log(JSON.stringify(aggr[k], function (key, value) {
            if (value instanceof Big ||
                typeof (value) === 'number') {

                return (value.toString());
            }
            return (value);
        }));
    });
}


function main() {
    var carry = mod_carrier.carry(process.openStdin());

    var aggr = {};

    carry.on('line', onLine.bind(null, aggr));
    carry.once('end', onEnd.bind(null, aggr));
}


if (require.main === module) {
    process.on('exit', function onExit() {
        process.exit(ERROR);
    });

    main();
}
