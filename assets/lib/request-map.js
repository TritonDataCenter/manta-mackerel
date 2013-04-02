#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('./carrier');
var mod_range_check = require('./range_check');
var ranges = require('../etc/ranges.json');

function shouldProcess(record) {
        return (record.audit &&
                record.req.url !== '/ping' &&
                typeof (record.req.owner) !== 'undefined');
}

function okStatus(code) {
        return (code >= 200 && code <= 204);
}


function getNetwork(record) {
        var ip = record.req.headers['x-forwarded-for'];
        for (var r in ranges) {
                if (mod_range_check.in_range(ip, ranges[r].ranges)) {
                        return (ranges[r].name);
                }
        }
        return ('external');
}

function count(record, aggr) {
        var owner = record.req.owner;
        var method = record.req.method;
        var resHeaderLength = record.resHeaderLength;
        var reqHeaderLength = record.reqHeaderLength;
        var statusCode = record.res.statusCode;
        var network = getNetwork(record);
        var r;

        // get the content-length if it exists
        var contentLength = record.res.headers['content-length'] ||
                +record.req.headers['content-length'] || 0;
        // _____^ content-length is a string when it's in the req header

        if (!aggr[owner]) {
                aggr[owner] = {
                        owner: owner
                };
                for (r in ranges) {
                        aggr[owner][ranges[r].name] = {
                                requests: {
                                        OPTION: 0,
                                        GET: 0,
                                        HEAD: 0,
                                        POST: 0,
                                        PUT: 0,
                                        DELETE: 0
                                },
                                bandwidth: {
                                        in: 0,
                                        out: 0,
                                        headerIn: 0,
                                        headerOut: 0
                                }
                        };
                }
        }

        aggr[owner][network].requests[method]++;
        aggr[owner][network].bandwidth.headerIn += reqHeaderLength;
        aggr[owner][network].bandwidth.headerOut += resHeaderLength;

        // only count bandwidth for successful GET & PUT
        if (method === 'GET' && okStatus(statusCode)) {
                aggr[owner][network].bandwidth.out += contentLength;
        }

        if (method === 'PUT' && okStatus(statusCode)) {
                aggr[owner][network].bandwidth.in += contentLength;
        }
}

function printResults(aggr) {
        Object.keys(aggr).forEach(function (owner) {
                console.log(JSON.stringify(aggr[owner]));
        });
}

function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var aggr = {};
        carry.on('line', function onLine(line) {
                var record;

                // since bunyan logs may contain lines such as
                // [ Nov 28 21:35:27 Enabled. ]
                // we need to ignore them
                try {
                        record = JSON.parse(line);
                } catch (e) {
                        console.warn('Line not json: ' + line);
                        return;
                }

                if (!shouldProcess(record)) {
                        return;
                }

                count(record, aggr);
        });

        carry.once('end', function onEnd() {
                printResults(aggr);
        });
}
main();
