#!/usr/node/bin/node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

/* BEGIN JSSTYLED */
/*
 * sample muskie audit record
 *
 * {
 *   "name": "audit",
 *   "hostname": "fb07e9ec-5137-418e-aff2-01d00aff1a49",
 *   "pid": 4400,
 *   "audit": true,
 *   "level": 30,
 *   "remoteAddress": "10.2.201.70",
 *   "remotePort": 36387,
 *   "reqHeaderLength": 818,
 *   "req": {
 *     "method": "PUT",
 *     "url": "/poseidon/stor/graphs/assets/manowar.tar.gz",
 *     "headers": {
 *       "accept": "application/json",
 *       "content-length": "6880413",
 *       "content-type": "application/octet-stream",
 *       "date": "Wed, 28 Nov 2012 21:46:00 GMT",
 *       "expect": "100-continue",
 *       "x-request-id": "cbc3e1ce-f863-4942-839b-1d30542ad31d",
 *       "x-durability-level": "2",
 *       "authorization": ELIDED FOR READABILITY
 *       "user-agent": "restify/1.0 (ia32-sunos; v8/3.11.10.22; OpenSSL/0.9.8w) node/0.8.12",
 *       "accept-version": "~1.0",
 *       "host": "manta.joyent.us",
 *       "connection": "keep-alive",
 *       "x-forwarded-for": "10.2.201.57"
 *     },
 *     "httpVersion": "1.1",
 *     "trailers": {},
 *     "owner": "eba7f07c-d57c-48f6-8072-f75db963e9d6"
 *   },
 *   "resHeaderLength": 241,
 *   "res": {
 *     "statusCode": 204,
 *     "headers": {
 *       "etag": "d5c7ee35-e232-4bb9-b239-1ef93daffcaf",
 *       "last-modified": "Wed, 28 Nov 2012 21:46:00 GMT",
 *       "date": "Wed, 28 Nov 2012 21:46:00 GMT",
 *       "server": "Manta",
 *       "x-request-id": "cbc3e1ce-f863-4942-839b-1d30542ad31d",
 *       "x-response-time": 212,
 *       "x-server-name": "fb07e9ec-5137-418e-aff2-01d00aff1a49"
 *     },
 *     "trailer": false
 *   },
 *   "latency": 212,
 *   "_audit": true,
 *   "msg": "handled: 204",
 *   "time": "2012-11-28T21:46:00.933Z",
 *   "v": 0
 * }
 */


/*
 * sample GET record req and res
 *
 * "req": {
 *   "method": "GET",
 *   "url": "/poseidon/stor/manta_gc/moray?limit=1024",
 *   "headers": {
 *     "accept": "application/x-json-stream",
 *     "date": "Wed, 28 Nov 2012 21:49:01 GMT",
 *     "x-request-id": "2e878928-eaf3-4ce2-8922-62d604d04c9c",
 *     "authorization": ELIDED FOR READABILITY
 *     "user-agent": "restify/1.0 (ia32-sunos; v8/3.11.10.22; OpenSSL/0.9.8w) node/0.8.12",
 *     "accept-version": "~1.0",
 *     "host": "manta.joyent.us",
 *     "connection": "close",
 *     "x-forwarded-for": "10.2.201.57"
 *   },
 *   "httpVersion": "1.1",
 *   "trailers": {},
 *   "owner": "eba7f07c-d57c-48f6-8072-f75db963e9d6"
 * },
 * "res": {
 *   "statusCode": 404,
 *   "headers": {
 *     "content-type": "application/json",
 *     "content-length": 83,
 *     "content-md5": "fP8EF/9kmhUAFV1y+WkEEA==",
 *     "date": "Wed, 28 Nov 2012 21:49:01 GMT",
 *     "server": "Manta",
 *     "x-request-id": "2e878928-eaf3-4ce2-8922-62d604d04c9c",
 *     "x-response-time": 8,
 *     "x-server-name": "fb07e9ec-5137-418e-aff2-01d00aff1a49"
 *   },
 *   "trailer": false
 * },
 */
/* END JSSTYLED */

var mod_carrier = require('carrier');
var Big = require('big.js');
var ERROR = false;

var LOG = require('bunyan').createLogger({
        name: 'request-map.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

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
        var operation = record.billable_operation;
        var method = record.req.method;
        var resHeaderLength = record.resHeaderLength;
        var reqHeaderLength = record.reqHeaderLength;
        var statusCode = record.res.statusCode;

        // get the content-length if it exists
        var contentLength = +record.res.headers['content-length'] ||
                +record.req.headers['content-length'] || 0;

        aggr[owner] = aggr[owner] || {
                owner: owner,
                requests: {
                        type: {
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
                        LOG.error(e, 'Error on line ' + lineCount);
                        ERROR = true;
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
