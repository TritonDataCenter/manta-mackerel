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
 * Extracts the _value field from JSON-ified moray dumps.
 * The rows are then msplit using the tuple (owner, type, objectId) to
 * dedupe links.
 */

var Transform = require('stream').Transform;
var lstream = require('lstream');
var util = require('util');

var LOOKUP_PATH = process.env.LOOKUP_FILE || '../etc/lookup.json';
var EXCLUDE_UNAPPROVED_USERS = process.env.EXCLUDE_UNAPPROVED_USERS === 'true';

/*
 * check for the fields we know we need
 */
function validSchema(obj) {
    var fields = ['key', 'owner', 'type'];
    var valid = fields.every(function (field) {
        return (typeof (obj[field]) !== 'undefined');
    });
    return (valid);
}

/*
 * parse the JSON value from the moray record
 */
function parseLine(line, index) {
    var record = JSON.parse(line);
    if (!record.entry || !record.entry[index]) {
        throw (new Error('unrecognized line'));
    }

    var value = JSON.parse(record.entry[index]);
    if (!validSchema(value)) {
        throw (new Error('missing key, owner or type'));
    }

    return (value);
}


function StorageMapStream(opts) {
    this.log = opts.log;
    this.lookup = opts.lookup;
    this.excludeUnapproved = opts.excludeUnapproved;

    this.lineNumber = 0;
    opts.decodeStrings = false;
    Transform.call(this, opts);
}
util.inherits(StorageMapStream, Transform);

StorageMapStream.prototype._transform = function _transform(line, enc, cb) {
    this.lineNumber++;

    if (typeof (this.index) === 'undefined') {
        try {
            this.index = JSON.parse(line).keys.indexOf('_value');
            cb();
            return;
        } catch (e) {
            this.log.fatal({error: e, line: line}, 'error parsing schema');
            cb(e);
            return;
        }
    }


    var obj;
    try {
        obj = parseLine(line, this.index);
    } catch (e) {
        this.log.error({
            error: e.message,
            lineNumber: this.lineNumber,
            line: line
        }, 'error parsing line');

        cb(e);
        return;
    }

    if (this.excludeUnapproved) {
        if (!this.lookup[obj.owner]) {
            this.log.warn({obj: obj}, 'No login found for %s', obj.owner);
        }

        if (!this.lookup[obj.owner].approved) {
            this.log.warn({obj: obj},
                '%s not approved for provisioning. Skipping...', obj.owner);
            cb();
            return;
        }
    }

    this.push(JSON.stringify(obj) + '\n');
    cb();
    return;
};



function main() {
    var log = require('bunyan').createLogger({
        name: 'storage-map.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var lookup;

    if (EXCLUDE_UNAPPROVED_USERS) {
        lookup = require(LOOKUP_PATH);
    }

    var mapStream = new StorageMapStream({
        log: log,
        lookup: lookup,
        excludeUnapproved: EXCLUDE_UNAPPROVED_USERS
    });

    mapStream.once('error', function (error) {
        log.fatal({error: error}, 'storage map error');
        process.exit(1);
    });

    process.stdin.pipe(new lstream()).pipe(mapStream).pipe(process.stdout);
}

if (require.main === module) {
    main();
}

module.exports = StorageMapStream;
