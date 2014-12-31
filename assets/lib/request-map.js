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
var lstream = require('lstream');
var util = require('util');

var ADMIN_USER = process.env.ADMIN_USER || 'poseidon';
var BILLABLE_OPS = (process.env.BILLABLE_OPS).split(' ');
var EXCLUDE_UNAPPROVED_USERS = process.env.EXCLUDE_UNAPPROVED_USERS === 'true';
var INCLUDE_ADMIN_REQUESTS = process.env.INCLUDE_ADMIN_REQUESTS === 'true';
var LOOKUP_PATH = process.env.LOOKUP_FILE || '../etc/lookup.json';
var MALFORMED_LIMIT = process.env.MALFORMED_LIMIT || '0';


function RequestMapStream(opts) {
    this.admin = opts.admin;
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

    // since bunyan logs may contain lines such as
    // [ Nov 28 21:35:27 Enabled. ]
    // we need to ignore them
    if (line[0] !== '{') {
        cb();
        return;
    }

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
            line: line
        }, 'error parsing line ' + this.lineNumber);
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
            requests: {},
            bandwidth: {
                in: new Big(0),
                out: new Big(0),
                headerIn: new Big(0),
                headerOut: new Big(0)
            }
        };
        this.billableOps.forEach(function (op) {
            self.aggr[owner].requests[op] = 0;
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
    var isPing = record.req.url === '/ping';
    var hasOwner = typeof (record.req.owner) !== 'undefined';
    var okStatus = record.res.statusCode >= 200 && record.res.statusCode <= 299;
    var isAdmin = record.req.caller && record.req.caller.login === this.admin;

    var isApproved;
    if (this.excludeUnapproved) {
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

    return (isAudit &&
            !isPing &&
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
    var log = require('bunyan').createLogger({
        name: 'storage-reduce1.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var lookup;

    if (EXCLUDE_UNAPPROVED_USERS) {
        lookup = require(LOOKUP_PATH);
    }

    var mapStream = new RequestMapStream({
        admin: ADMIN_USER,
        billableOps: BILLABLE_OPS,
        excludeUnapproved: EXCLUDE_UNAPPROVED_USERS,
        includeAdmin: INCLUDE_ADMIN_REQUESTS,
        log: log,
        lookup: lookup
    });

    var malformed = 0;

    mapStream.once('error', function (error) {
        log.fatal({error: error}, 'request map phase error');
        process.exit(1);
    });

    mapStream.on('malformed', function () {
        malformed++;
    });

    mapStream.once('end', function () {
        var len = MALFORMED_LIMIT.length;
        var threshold;

        if (MALFORMED_LIMIT[len - 1] === '%') {
            var pct = +(MALFORMED_LIMIT.substr(0, len-1));
            threshold = pct * mapStream.lineNumber;
        } else {
            threshold = parseInt(MALFORMED_LIMIT, 10);
        }

        if (isNaN(threshold)) {
            return;
        }

        if (malformed > threshold) {
            log.fatal({count: malformed}, 'Too many malformed lines.');
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
