#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('./carrier');

function shouldProcess(record) {
        return (record.audit &&
                record.req.url !== '/ping' &&
                typeof (record.req.owner) !== 'undefined');
}

function okStatus(code) {
        return (code >= 200 && code <= 204);
}


function count(record, aggr) {
        var owner = record.req.owner;
        var method = record.req.method;
        var resHeaderLength = record.resHeaderLength;
        var reqHeaderLength = record.reqHeaderLength;
        var statusCode = record.res.statusCode;
        var i;

        // get the content-length if it exists
        var contentLength = record.res.headers['content-length'] ||
                +record.req.headers['content-length'] || 0;
        // _____^ content-length is a string when it's in the req header

        aggr[owner] = aggr[owner] || {
                owner: owner,
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

        aggr[owner].requests[method]++;
        aggr[owner].bandwidth.headerIn += reqHeaderLength;
        aggr[owner].bandwidth.headerOut += resHeaderLength;

        // only count bandwidth for successful GET & PUT
        if (method === 'GET' && okStatus(statusCode)) {
                aggr[owner].bandwidth.out += contentLength;
        }

        if (method === 'PUT' && okStatus(statusCode)) {
                aggr[owner].bandwidth.in += contentLength;
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

                // only process audit records, and ignore pings
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
