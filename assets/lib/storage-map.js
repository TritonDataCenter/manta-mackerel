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
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var lstream = require('lstream');
var util = require('util');


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
            this.log.error({error: e, line: line}, 'error parsing schema');
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
            cb();
            return;
        }

        if (!this.lookup[obj.owner].approved) {
            this.log.debug({obj: obj},
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
    var log = bunyan.createLogger({
        name: 'storage-map.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var options = [
        {
            name: 'excludeUnapproved',
            type: 'bool',
            env: 'EXCLUDE_UNAPPROVED_USERS',
            help: 'Exclude usage for users that have ' +
                    'approved_for_provisioning = false'
        },
        {
            name: 'lookupPath',
            type: 'string',
            env: 'LOOKUP_PATH',
            default: '../etc/lookup.json',
            help: 'Path to lookup file'
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
        console.error('storage-map: error: %s', e.message);
        process.exit(1);
    }

    if (opts.help) {
        var help = parser.help({includeEnv: true}).trimRight();
        console.log('usage: node storage-map.js [OPTIONS]\n' +
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

    var mapStream = new StorageMapStream({
        excludeUnapproved: opts.excludeUnapproved,
        log: log,
        lookup: lookup
    });

    mapStream.once('error', function (error) {
        log.error({error: error}, 'storage map error');
        process.abort();
    });

    process.stdin.pipe(new lstream()).pipe(mapStream).pipe(process.stdout);
}

if (require.main === module) {
    main();
}

module.exports = StorageMapStream;
