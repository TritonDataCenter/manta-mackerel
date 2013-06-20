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
                var storageGBHours = ceil(record['byteHrs'].div(BPERGB));
                var bandwidthGB = {
                        in: ceil(record['bandwidth']['in'].div(BPERGB)),
                        out: ceil(record['bandwidth']['out'].div(BPERGB))
                };
                var computeBandwidthGB = {
                        in: ceil(record['computeBandwidth']['in'].div(BPERGB)),
                        out: ceil(record['computeBandwidth']['out'].div(BPERGB))
                };

                var output = {
                        owner: record.owner,
                        date: process.env['DATE'],
                        storageGBHours: storageGBHours,
                        bandwidthGB: bandwidthGB,
                        requests: record.requests,
                        computeGBSeconds: record.computeGBSeconds,
                        computeBandwidthGB: computeBandwidthGB
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
