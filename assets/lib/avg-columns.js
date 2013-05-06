#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('carrier');

function parseLine(line) {
        return (JSON.parse(line));
}

function getAggKey(obj) {
        var key = '';
        Object.keys(obj).forEach(function (k) {
                if (typeof (obj[k]) !== 'number') {
                        key += obj[k];
                }
        });
        return (key);
}


// assumes arg1 and arg2 have the same structure, and adds each respective
// number recursively
function plusEquals(arg1, arg2) {
        Object.keys(arg1).forEach(function (k) {
                if (typeof (arg1[k]) === 'object') {
                        plusEquals(arg1[k], arg2[k]);
                } else if (typeof (arg1[k]) === 'number') {
                        arg1[k] += arg2[k];
                }
        });
}


// divides each number in obj by the divisor recursively
function divide(obj, divisor) {
        Object.keys(obj).forEach(function (k) {
                if (typeof (obj[k]) === 'object') {
                        divide(obj[k], divisor);
                } else if (typeof (obj[k]) === 'number') {
                        obj[k] = Math.round(obj[k] / divisor);
                }
        });
}


function onLine(aggr, line) {
        var parsed = parseLine(line);
        parsed.__count = 1;

        var aggKey = getAggKey(parsed);

        if (!aggr[aggKey]) {
                aggr[aggKey] = parsed;
        } else {
                plusEquals(aggr[aggKey], parsed);
        }
}


function onEnd(aggr) {
        Object.keys(aggr).forEach(function (j) {
                var count = aggr[j].__count;
                delete aggr[j].__count;
                divide(aggr[j], count);
                console.log(JSON.stringify(aggr[j]));
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
