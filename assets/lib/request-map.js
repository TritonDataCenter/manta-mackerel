#!/usr/node/bin/node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('carrier');
var Big = require('big.js');
var ERROR = false;
var lookupPath = process.env['LOOKUP_FILE'] || '../etc/lookup.json';
var lookup = require(lookupPath); // maps uuid->approved_for_provisioning

var COUNT_UNAPPROVED_USERS = process.env['COUNT_UNAPPROVED_USERS'] === 'true';
var DROP_POSEIDON = process.env['DROP_POSEIDON_REQUESTS'] === 'true';
var MALFORMED_LIMIT = process.env['MALFORMED_LIMIT'] || '0';

var LOG = require('bunyan').createLogger({
        name: 'request-map.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

function shouldProcess(record) {
        return (record.audit &&
                record.req.url !== '/ping' &&
                typeof (record.req.owner) !== 'undefined' &&
                (!record.req.caller ||
                        !DROP_POSEIDON ||
                        record.req.caller.login !== 'poseidon'));
}

function okStatus(code) {
        return (code >= 200 && code <= 204);
}


function count(record, aggr) {
        var owner = record.req.owner;
        var operation = record.billable_operation;
        var method = record.req.method;
        var resHeaderLength = record.resHeaderLength;
        var reqHeaderLength = record.reqHeaderLength;
        var statusCode = record.res.statusCode;

        // get the content-length if it exists
        var contentLength = 0;
        if (record.res.headers && record.res.headers['content-length']) {
                contentLength = +record.res.headers['content-length'];
        } else if (record.req.headers && record.req.headers['content-length']) {
                contentLength = +record.req.headers['content-length'];
        }

        aggr[owner] = aggr[owner] || {
                owner: owner,
                requests: {
                        type: {
                                // if these change, update summarize-reduce.js
                                DELETE: 0,
                                GET: 0,
                                HEAD: 0,
                                LIST: 0,
                                OPTIONS: 0,
                                POST: 0,
                                PUT: 0
                        },
                        bandwidth: {
                                in: new Big(0),
                                out: new Big(0),
                                headerIn: new Big(0),
                                headerOut: new Big(0)
                        }
                }
        };

        if (operation) {
                aggr[owner].requests.type[operation]++;
        }

        var bw = aggr[owner].requests.bandwidth;
        bw.headerIn = bw.headerIn.plus(reqHeaderLength);
        bw.headerOut = bw.headerOut.plus(resHeaderLength);

        // only count bandwidth for successful GET & PUT
        if (method === 'GET' && okStatus(statusCode)) {
                bw.out = bw.out.plus(contentLength);
        }

        if (method === 'PUT' && okStatus(statusCode)) {
                bw.in = bw.in.plus(contentLength);
        }
}

function printResults(aggr) {
        Object.keys(aggr).forEach(function (owner) {
                console.log(JSON.stringify(aggr[owner], function (key, value) {
                        if (value instanceof Big) {
                                return (value.toString());
                        }
                        return (value);
                }));
        });
}

function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var aggr = {};
        var lineCount = 0;
        var malformed = 0;
        carry.on('line', function onLine(line) {
                lineCount++;
                var record;

                // since bunyan logs may contain lines such as
                // [ Nov 28 21:35:27 Enabled. ]
                // we need to ignore them
                if (line[0] != '{') {
                        return;
                }

                try {
                        record = JSON.parse(line);
                } catch (e) {
                        malformed++;
                        LOG.error(e, 'Error on line ' + lineCount);
                        return;
                }

                // only process audit records, and ignore pings
                if (!shouldProcess(record)) {
                        return;
                }

                if (!COUNT_UNAPPROVED_USERS) {
                        if (!lookup[record.req.owner]) {
                                LOG.error(record, 'No login found for UUID ' +
                                        record.req.owner);
                                ERROR = true;
                                return;
                        }

                        if (!lookup[record.req.owner].approved) {
                                LOG.warn(record, record.req.owner +
                                        ' not approved for provisioning. ' +
                                        'Skipping...');
                                return;
                        }
                }

                count(record, aggr);
        });

        carry.once('end', function onEnd() {
                var len = MALFORMED_LIMIT.length;
                var threshold;

                if (MALFORMED_LIMIT[len - 1] === '%') {
                        var pct = +(MALFORMED_LIMIT.substr(0, len-1));
                        threshold = pct * lineCount;
                } else {
                        threshold = +MALFORMED_LIMIT;
                }

                if (isNaN(threshold)) {
                        LOG.error('MALFORMED_LIMIT not a number');
                        ERROR = true;
                        return;
                }

                if (malformed > threshold) {
                        LOG.fatal('Too many malformed lines');
                        ERROR = true;
                        return;
                }

                printResults(aggr);
        });
}

if (require.main === module) {

        process.on('exit', function onExit() {
                process.exit(ERROR);
        });

        main();
}
