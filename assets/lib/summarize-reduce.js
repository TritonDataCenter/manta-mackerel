#!/usr/node/bin/node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('carrier');
var Big = require('big.js');
var ERROR = false;
var LOG = require('bunyan').createLogger({
        name: 'summarize-reduce.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

var BPERGB = 1073741824;

function ceil(x) {
        return (x.round(0, 0).eq(x) ? x : x.plus(1).round(0, 0));
}

function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var lineCount = 0;

        carry.on('line', function onLine(line) {
                lineCount++;
                try {
                        var record = JSON.parse(line, function (key, value) {
                                if (key === '') {
                                        return (value);
                                }
                                if (key === 'byteHrs' ||
                                    key === 'in' ||
                                    key === 'out') {

                                        return (new Big(value));
                                }
                                return (value);
                        });
                } catch (e) {
                        LOG.error(e, 'Error on line ' + lineCount);
                        ERROR = true;
                        return;
                }
                record['byteHrs'] = record['byteHrs'] || new Big(0);
                record['bandwidth'] = record['bandwidth'] || {
                        'in': new Big(0),
                        'out': new Big(0)
                };
                record['requests'] = record['requests'] || {
                        // if these change, update request-map.js
                        'DELETE': '0',
                        'GET': '0',
                        'HEAD': '0',
                        'LIST': '0',
                        'OPTIONS': '0',
                        'POST': '0',
                        'PUT': '0'
                };
                record['computeBandwidth'] = record['computeBandwidth'] || {
                        'in': new Big(0),
                        'out': new Big(0)
                };
                record['computeGBSeconds'] = record['computeGBSeconds'] || '0';
                var storageGBHours = ceil(record['byteHrs'].div(BPERGB));
                var bandwidthBytes = {
                        in: record['bandwidth']['in'],
                        out: record['bandwidth']['out']
                };
                var computeBandwidthBytes = {
                        in: record['computeBandwidth']['in'],
                        out: record['computeBandwidth']['out']
                };

                var output = {
                        owner: record.owner,
                        date: process.env['DATE'],
                        storageGBHours: storageGBHours,
                        bandwidthBytes: bandwidthBytes,
                        requests: record.requests,
                        computeGBSeconds: record.computeGBSeconds,
                        computeBandwidthBytes: computeBandwidthBytes
                };

                console.log(JSON.stringify(output, function (key, value) {
                        if (value instanceof Big) {
                                return (value.toString());
                        }
                        return (value);
                }));
        });
}

if (require.main === module) {
        process.on('exit', function onExit() {
                process.exit(ERROR);
        });

        main();
}
