#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('./carrier');

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

// assumes arg1 and arg2 have the same structure
// arg1 += arg2;
function plusEquals(arg1, arg2) {
        Object.keys(arg1).forEach(function (k) {
                if (typeof (arg1[k]) === 'object') {
                        plusEquals(arg1[k], arg2[k]);
                } else if (typeof (arg1[k]) === 'number') {
                        arg1[k] += arg2[k];
                }
        });
}

function onLine(aggr, line) {
        var parsed = parseLine(line);

        var aggKey = getAggKey(parsed);

        if (!aggr[aggKey]) {
                aggr[aggKey] = parsed;
        } else {
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
