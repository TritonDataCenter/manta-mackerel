#!/usr/bin/env node
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

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
                aggr[aggKey] = {__count: 0};
                Object.keys(parsed).forEach(function (k) {
                        if (!isNum(parsed[k])) {
                                aggr[aggKey][k] = parsed[k];
                                return;
                        }
                        aggr[aggKey][k] = 0;
                });
        }

        aggr[aggKey].__count += 1;
        Object.keys(parsed).forEach(function (k) {
                if (!isNum(parsed[k])) {
                        return;
                }
                aggr[aggKey][k] += parsed[k];
        });
}


function onEnd(aggr) {
        Object.keys(aggr).forEach(function (j) {
                var count = aggr[j].__count;
                aggr[j].__count = undefined;
                Object.keys(aggr[j]).forEach(function (k) {
                        if (isNum(aggr[j][k])) {
                                aggr[j][k] = Math.round(aggr[j][k] / count);
                        }
                });
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
