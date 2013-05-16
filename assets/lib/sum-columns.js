#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('carrier');
var lineCount = 0;

function hrtimePlusEquals(oldvalue, newvalue) {
        oldvalue[0] += newvalue[0];
        oldvalue[1] += newvalue[1];
        if (oldvalue[1] > 1e9) {
                oldvalue[0]++;
                oldvalue[1] -= 1e9;
        }
        return (oldvalue);
}


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
                        } else if (typeof (from[k]) === 'object') {
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
        Object.keys(oldvalue).forEach(function (k) {
                // consider any array of length 2 to be a hrtime
                if (Array.isArray(oldvalue[k]) && oldvalue[k].length === 2) {
                        hrtimePlusEquals(oldvalue[k], newvalue[k]);
                } else if (typeof (oldvalue[k]) === 'object') {
                        plusEquals(oldvalue[k], newvalue[k]);
                } else if (typeof (oldvalue[k]) === 'number') {
                        oldvalue[k] += newvalue[k];
                }
        });
}


function onLine(aggr, line) {
        lineCount++;

        var parsed;
        try {
                parsed = JSON.parse(line);
        } catch (e) {
                console.warn('Error on line ' + lineCount + ': ' + e.message);
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
                console.log(JSON.stringify(aggr[k]));
        });
}


function main() {
        var carry = mod_carrier.carry(process.openStdin());

        var aggr = {};

        carry.on('line', onLine.bind(null, aggr));
        carry.once('end', onEnd.bind(null, aggr));
}


if (require.main === module) {
        main();
}
