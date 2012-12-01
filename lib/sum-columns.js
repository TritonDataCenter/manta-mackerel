#!/usr/bin/env node
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var carrier = require('carrier');


function parseLine(line) {
        return (JSON.parse(line));
}

function onLine(aggr, line) {
        var parsed = parseLine(line);

        var owner = parsed.owner;

        if (!aggr[owner]) {
                aggr[owner] = {owner: owner};
                Object.keys(parsed).forEach(function (k) {
                        if (k === 'owner') {
                                return;
                        }
                        aggr[owner][k] = 0;
                });
        }
        Object.keys(parsed).forEach(function (k) {
                if (k === 'owner') {
                        return;
                }
                aggr[owner][k] += parsed[k];
        });
}

function onEnd(aggr) {
        Object.keys(aggr).forEach(function (k) {
                console.log(JSON.stringify(aggr[k]));
        });
}

function main() {
        var carry = carrier.carry(process.openStdin());

        var aggr = {};

        carry.on('line', onLine.bind(null, aggr));
        carry.once('end', onEnd.bind(null, aggr));
}

if (require.main === module) {
        main();
}
