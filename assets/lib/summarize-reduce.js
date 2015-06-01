#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var Big = require('big.js');
var Transform = require('stream').Transform;
var dashdash = require('dashdash');
var lstream = require('lstream');
var util = require('util');

var BPERGB = 1073741824;

function stringify(key, value) {
    if (value instanceof Big || typeof (value) === 'number') {
        return (value.toString());
    }
    return (value);
}


function ceil(x) {
    return (x.round(0, 0).eq(x) ? x : x.plus(1).round(0, 0));
}


function SummarizeReduceStream(opts) {
    this.log = opts.log;
    this.billableOps = opts.billableOps;

    this.lineNumber = 0;
    opts.decodeStrings = false;
    Transform.call(this, opts);
}
util.inherits(SummarizeReduceStream, Transform);


SummarizeReduceStream.prototype._transform = function transform(line, enc, cb) {
    this.lineNumber++;

    var record;
    try {
        record = JSON.parse(line, function (key, value) {
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
        this.log.error({
            error: e,
            lineNumber: this.lineNumber,
            line: line
        }, 'error parsing line ' + this.lineNumber);

        cb(e);
        return;
    }

    // MANTA-2430 only one hour of metering is generated per day so
    // mulitply by 24 to get byte-hours
    record.byteHrs = (record.byteHrs || new Big(0)).times(24);

    record.bandwidth = record.bandwidth || {
        'in': new Big(0),
        'out': new Big(0)
    };
    if (!record.requests) {
        this.billableOps.forEach(function (op) {
            record.requests[op] = '0';
        });
    }
    record.computeBandwidth = record.computeBandwidth || {
        'in': new Big(0),
        'out': new Big(0)
    };
    record.computeGBSeconds = record.computeGBSeconds || '0';
    var storageGBHours = ceil(record.byteHrs.div(BPERGB));
    var bandwidthBytes = {
        in: record.bandwidth.in,
        out: record.bandwidth.out
    };
    var computeBandwidthBytes = {
        in: record.computeBandwidth.in,
        out: record.computeBandwidth.out
    };

    var output = {
        owner: record.owner,
        date: process.env.DATE,
        storageGBHours: storageGBHours,
        bandwidthBytes: bandwidthBytes,
        requests: record.requests,
        computeGBSeconds: record.computeGBSeconds,
        computeBandwidthBytes: computeBandwidthBytes
    };

    this.push(JSON.stringify(output, stringify) + '\n');
};

function main() {
    var log = require('bunyan').createLogger({
        name: 'summarize-reduce.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var options = [
        {
            name: 'billableOps',
            type: 'string',
            env: 'BILLABLE_OPS',
            default: 'DELETE,GET,HEAD,LIST,OPTIONS,POST,PUT',
            help: 'Comma-separated list of operations to meter'
        },
        {
            names: ['help', 'h'],
            type: 'bool',
            help: 'Print help'
        }
    ];

    var parser = dashdash.createParser({options: options});
    var opts;
    try {
        opts = parser.parse(process.argv);
    } catch (e) {
        console.error('summarize-reduce: error: %s', e.message);
        process.exit(1);
    }

    if (opts.help) {
        var help = parser.help({includeEnv: true}).trimRight();
        console.log('usage: node summarize-reduce.js [OPTIONS]\n' +
                    'options:\n' +
                    help);
        process.exit(0);
    }

    var billableOps = opts.billableOps.split(',');
    var reduceStream = new SummarizeReduceStream({
        billableOps: billableOps,
        log: log
    });

    reduceStream.once('error', function (error) {
        log.error({error: error}, 'summarize reduce error');
        process.abort();
    });

    process.stdin.pipe(new lstream()).pipe(reduceStream).pipe(process.stdout);

}


if (require.main === module) {
    main();
}

module.exports = SummarizeReduceStream;
