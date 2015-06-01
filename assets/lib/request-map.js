#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * RequestMapStream
 *
 * Extracts relevant usage fields from muskie logs and aggregates them on a
 * per-owner basis. The next phase combines usage from the separate logs
 * belonging to the same owner.
 */

var Big = require('big.js');
var Transform = require('stream').Transform;
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var lstream = require('lstream');
var util = require('util');


function RequestMapStream(opts) {
    this.adminUser = opts.adminUser;
    this.billableOps = opts.billableOps;
    this.includeAdmin = opts.includeAdmin;
    this.excludeUnapproved = opts.excludeUnapproved;
    this.log = opts.log;
    this.lookup = opts.lookup;

    this.lineNumber = 0;
    this.aggr = {};
    opts.decodeStrings = false;
    Transform.call(this, opts);
}
util.inherits(RequestMapStream, Transform);


RequestMapStream.prototype._transform = function _transform(line, enc, cb) {
    var self = this;
    this.lineNumber++;

    var record;
    try {
        record = JSON.parse(line);
    } catch (e) {
        // Sometimes a log will be rotated in the middle of a bunyan entry,
        // which will cause JSON.parse to fail. We'll ignore those, but we want
        // to keep track of them in case there's a more widespread issue with
        // record parsing.
        this.emit('malformed', line);
        this.log.warn({
            error: e,
            line: line,
            lineNumber: this.lineNumber
        }, 'error parsing line ' + this.lineNumber);
        cb();
        return;
    }

    if (!this._shouldProcess(record)) {
        cb();
        return;
    }

    // initialize the user
    var owner = record.req.owner;
    var method = record.req.method;
    if (!this.aggr[owner]) {
        this.aggr[owner] = {
            owner: owner,
            requests: {
                type: {},
                bandwidth: {
                    in: new Big(0),
                    out: new Big(0),
                    headerIn: new Big(0),
                    headerOut: new Big(0)
                }
            },
        };
        this.billableOps.forEach(function (op) {
            self.aggr[owner].requests.type[op] = 0;
        });
    }

    var operation = record.billable_operation;
    if (operation) {
        this.aggr[owner].requests.type[operation]++;
    }

    // count header bandwidth
    var bw = this.aggr[owner].requests.bandwidth;
    var resHeaderLength = record.resHeaderLength;
    var reqHeaderLength = record.reqHeaderLength;
    bw.headerIn = bw.headerIn.plus(reqHeaderLength);
    bw.headerOut = bw.headerOut.plus(resHeaderLength);

    // get the content-length if it exists
    var contentLength = 0;
    if (record.res.headers && record.res.headers['content-length']) {
        contentLength = parseInt(record.res.headers['content-length'], 10);
    } else if (record.req.headers && record.req.headers['content-length']) {
        contentLength = parseInt(record.req.headers['content-length'], 10);
    } else if (record.bytesTranferred) {
        // bytesTransferred will exist if the request is streaming
        // i.e. transfer-encoding: chunked
        contentLength = parseInt(record.bytesTransferred, 10);
    }
    // only count bandwidth for GET & PUT
    if (method === 'GET') {
        bw.out = bw.out.plus(contentLength);
    }
    if (method === 'PUT') {
        bw.in = bw.in.plus(contentLength);
    }
    cb();
};


RequestMapStream.prototype._shouldProcess = function _shouldProcess(record) {
    var isAudit = record.audit;
    if (!isAudit) {
        return (false);
    }

    var isPing = record.req.url === '/ping';
    var hasOwner = typeof (record.req.owner) !== 'undefined';
    var okStatus = record.res.statusCode >= 200 && record.res.statusCode <= 299;
    var isAdmin = record.req.caller && record.req.caller.login === this.adminUser;

    var isApproved;
    if (this.excludeUnapproved && hasOwner) {
        if (!this.lookup[record.req.owner]) {
            this.log.warn({
                record: record
            }, 'No login found for %s', record.req.owner);
            isApproved = true;
        } else {
            isApproved = this.lookup[record.req.owner].approved;
        }
    } else {
        isApproved = true;
    }

    return (!isPing &&
            hasOwner &&
            okStatus &&
            isApproved &&
            (this.includeAdmin || !isAdmin));
};


RequestMapStream.prototype._flush = function _flush(cb) {
    var self = this;

    function bigToString(key, value) {
        if (value instanceof Big) {
            return (value.toString());
        }
        return (value);
    }

    Object.keys(this.aggr).forEach(function (owner) {
        self.push(JSON.stringify(self.aggr[owner], bigToString) + '\n');
    });
    cb();
};



function main() {
    var log = bunyan.createLogger({
        name: 'storage-reduce1.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var options = [
        {
            name: 'adminUser',
            type: 'string',
            env: 'ADMIN_USER',
            default: 'poseidon',
            help: 'Manta admin user login'
        },
        {
            name: 'billableOps',
            type: 'string',
            env: 'BILLABLE_OPS',
            default: 'DELETE,GET,HEAD,LIST,OPTIONS,POST,PUT',
            help: 'Comma-separated list of operations to meter'
        },
        {
            name: 'excludeUnapproved',
            type: 'bool',
            env: 'EXCLUDE_UNAPPROVED_USERS',
            help: 'Exclude usage for users that have ' +
                    'approved_for_provisioning = false'
        },
        {
            name: 'includeAdmin',
            type: 'bool',
            env: 'INCLUDE_ADMIN_REQUESTS',
            help: 'Include requests by the Manta admin user (i.e. poseidon)'
        },
        {
            name: 'lookupPath',
            type: 'string',
            env: 'LOOKUP_PATH',
            default: '../etc/lookup.json',
            help: 'Path to lookup file'
        },
        {
            name: 'malformedLimit',
            type: 'integer',
            env: 'MALFORMED_LIMIT',
            help: 'Number of malformed lines that will be ' +
                    'ignored before raising an error'
        },
        {
            name: 'malformedLimitPct',
            type: 'number',
            env: 'MALFORMED_LIMIT_PCT',
            help: 'Percentage of malformed lines that will be ' +
                    'ignored before raising an error'
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
        console.error('request-map: error: %s', e.message);
        process.exit(1);
    }

    if (opts.help) {
        var help = parser.help({includeEnv: true}).trimRight();
        console.log('usage: node request-map.js [OPTIONS]\n' +
                    'options:\n' +
                    help);
        process.exit(0);
    }

    if (opts.hasOwnProperty('excludeUnapproved') &&
        !opts.hasOwnProperty('lookupPath')) {
        console.error('storage-map: error: missing lookup file');
        process.exit(1);
    }

    var lookup;
    if (opts.excludeUnapproved) {
        lookup = require(opts.lookupPath);
    }

    var billableOps = opts.billableOps.split(',');
    var mapStream = new RequestMapStream({
        admin: opts.adminUser,
        billableOps: billableOps,
        excludeUnapproved: opts.excludeUnapproved,
        includeAdmin: opts.includeAdmin,
        log: log,
        lookup: lookup
    });

    var malformed = 0;

    mapStream.once('error', function (error) {
        log.error({error: error}, 'request map phase error');
        process.abort();
    });

    mapStream.on('malformed', function () {
        malformed++;
        if (opts.hasOwnProperty('malformedLimit') &&
                malformed > opts.malformedLimit) {
            log.error({count: malformed}, 'Too many malformed lines.');
            process.exit(1);
            return;
        }
    });

    mapStream.once('end', function () {
        if (opts.hasOwnProperty('malformedLimitPct') &&
                malformed > opts.malformedLimitPct * mapStream.lineNumber) {
            log.error({count: malformed}, 'Too many malformed lines.');
            process.exit(1);
            return;
        }

    });

    process.stdin.pipe(new lstream()).pipe(mapStream).pipe(process.stdout);
}

if (require.main === module) {
    main();
}

module.exports = RequestMapStream;
