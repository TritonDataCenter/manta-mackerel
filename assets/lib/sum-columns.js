#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('./carrier');

function isNum(n) {
        return (!isNaN(parseFloat(n)) && isFinite(n));
}


function parseLine(line) {
        return (JSON.parse(line));
}


function getAggKey(obj) {
        var key = '';
        Object.keys(obj).forEach(function (k) {
                if (!isNum(obj[k])) {
                        key += obj[k];
                }
        });
        return (key);
}


function onLine(aggr, line) {
        var parsed = parseLine(line);

        var aggKey = getAggKey(parsed);

        if (!aggr[aggKey]) {
                aggr[aggKey] = {};
                Object.keys(parsed).forEach(function (k) {
                        if (!isNum(parsed[k])) {
                                aggr[aggKey][k] = parsed[k];
                                return;
                        }
                        aggr[aggKey][k] = 0;
                });
        }
        Object.keys(parsed).forEach(function (k) {
                if (!isNum(parsed[k])) {
                        return;
                }
                aggr[aggKey][k] += parsed[k];
        });
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
