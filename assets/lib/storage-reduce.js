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
 * Input is rows that have been msplit using (owner, type, objectId).
 * This stream calculates the size of each unique object, dedupes links and
 * counts directories. The output is msplit using the
 * `owner` field to be summed in the next phase.
 */

/* BEGIN JSSTYLED */
/*
 * sample object record:
 * {
 *   "dirname": "/cc56f978-00a7-4908-8d20-9580a3f60a6e/stor/logs/postgresql/2012/11/12/18",
 *   "key": "/cc56f978-00a7-4908-8d20-9580a3f60a6e/stor/logs/postgresql/2012/11/12/18/49366a2c.log.bz2",
 *   "headers": {},
 *   "mtime": 1352746869592,
 *   "owner": "cc56f978-00a7-4908-8d20-9580a3f60a6e",
 *   "type": "object",
 *   "contentLength": 84939,
 *   "contentMD5": "iSdRMW7Irsw1UwYoRDFmIA==",
 *   "contentType": "application/x-bzip2",
 *   "etag": "5fcc0345-1044-4b67-b7e8-98ee692001bc",
 *   "objectId": "5fcc0345-1044-4b67-b7e8-98ee692001bc",
 *   "sharks": [
 *   {
 *     "availableMB": 20477,
 *     "percentUsed": 1,
 *     "datacenter": "bh1-kvm1",
 *     "server_uuid": "44454c4c-4700-1034-804a-c7c04f354d31",
 *     "zone_uuid": "ef8b166a-ac3e-4d59-bb73-a65e2b17ba44",
 *     "url": "http://ef8b166a-ac3e-4d59-bb73-a65e2b17ba44.stor.bh1-kvm1.joyent.us"
 *   },
 *   {
 *     "availableMB": 20477,
 *     "percentUsed": 1,
 *     "datacenter": "bh1-kvm1",
 *     "server_uuid": "44454c4c-4700-1034-804a-c7c04f354d31",
 *     "zone_uuid": "59fb8bd3-67a7-4da2-bb68-287e2db01ec1",
 *     "url": "http://59fb8bd3-67a7-4da2-bb68-287e2db01ec1.stor.bh1-kvm1.joyent.us"
 *   }
 *   ]
 * }
 */

/*
 * sample directory record:
 * {
 *   "dirname": "/cc56f978-00a7-4908-8d20-9580a3f60a6e/stor/manatee_backups/1.moray.bh1-kvm1.joyent.us",
 *   "key": "/cc56f978-00a7-4908-8d20-9580a3f60a6e/stor/manatee_backups/1.moray.bh1-kvm1.joyent.us/2012-11-13-02-00-03",
 *   "headers": {},
 *   "mtime": 1352772004269,
 *   "owner": "cc56f978-00a7-4908-8d20-9580a3f60a6e",
 *   "type": "directory"
 * }
 */

/*
 * aggr format:
 * {
 *    owner: {
 *        "dirs": {
 *            namespace: 0,
 *            ...
 *        },
 *        "objects": {
 *            objectId: {
 *                size: 0,
 *                counts: {
 *                  namespace: 0, // count of # keys in this
 *                                // namespace for this objectId
 *                  ...
 *                }
 *            },
 *            ...
 *        }
 *    },
 *    ...
 * }
 */

/*
 * output format:
 *
 *  {
 *       "owner": "cc56f978-00a7-4908-8d20-9580a3f60a6",
 *       "stor": {
 *           "directories": 111,
 *           "keys": 234,
 *           "objects": 184,
 *           "bytes": "348582"
 *       },
 *       "public": {
 *           "directories": 2,
 *           "keys": 5,
 *           "objects": 5,
 *           "bytes": "2235"
 *       },
 *       "jobs": {
 *           "directories": 0,
 *           "keys": 0,
 *           "objects": 0,
 *           "bytes": "0"
 *       },
 *       "reports": {
 *           "directories": 6,
 *           "keys": 7,
 *           "objects": 9,
 *           "bytes": "12592"
 *       }
 *  }
 */

/* END JSSTYLED */

var Big = require('big.js');
var Transform = require('stream').Transform;
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var lstream = require('lstream');
var util = require('util');


function StorageReduce1Stream(opts) {
    this.log = opts.log;
    this.namespaces = opts.namespaces;
    this.lineNumber = 0;
    this.aggr = {};
    if (typeof(opts.minSize) !== 'number') {
        this.minSize = 0;
    } else {
        this.minSize = opts.minSize;
    }
    opts.decodeStrings = false;
    Transform.call(this, opts);
}
util.inherits(StorageReduce1Stream, Transform);


StorageReduce1Stream.prototype._transform = function _transform(line, enc, cb) {
    this.lineNumber++;

    var record;
    try {
        record = JSON.parse(line);
    } catch (e) {
        this.log.error({
            error: e.message,
            line: line,
            lineNumber: this.lineNumber
        }, 'error parsing line');
        cb(e);
        return;
    }

    if (!record.owner || !record.type) {
        this.log.error({
            line: line,
            linenumber: this.lineNumber
        }, 'Missing owner or type field on line ' + this.lineNumber);
        cb(new Error('missing owner or type'));
        return;
    }

    var owner = record.owner;
    var type = record.type;
    var namespace = record.key.split('/')[2]; // /:uuid/:namespace/...
    record.namespace = namespace;

    this.aggr[owner] = this.aggr[owner] || {
        dirs: {},
        objects: {}
    };
    this.aggr[owner].dirs[namespace] = this.aggr[owner].dirs[namespace] || 0;

    if (type === 'directory') {
        this._incrDirectory(record);
    } else if (type === 'object') {
        this._incrObject(record);
    } else {
        this.log.error({
            line: line,
            lineNumber: this.lineNumber
        }, 'unrecognized object type: ' + type);
        cb(new Error('unrecognized object'));
        return;
    }

    cb();
    return;
};


StorageReduce1Stream.prototype._incrDirectory = function _incrDir(record) {
    var owner = record.owner;
    var namespace = record.namespace;

    this.aggr[owner].dirs[namespace]++;
};


StorageReduce1Stream.prototype._incrObject = function _incrObject(record) {
    var owner = record.owner;
    var namespace = record.namespace;
    var objectId = record.objectId;
    var size = Math.max(record.contentLength, this.minSize);
    var total = size * record.sharks.length;

    var objectList = this.aggr[owner].objects;
    if (!objectList[objectId]) {
        objectList[objectId] = {
            size: total,
            counts: {}
        };
    }

    if (objectList[objectId].counts[namespace]) {
        objectList[objectId].counts[namespace]++;
    } else {
        objectList[objectId].counts[namespace] = 1;
    }
};


StorageReduce1Stream.prototype._flush = function _flush(cb) {
    function bigToString(key, value) {
        if (value instanceof Big || typeof (value) === 'number') {
            return (value.toString());
        }
        return (value);
    }

    var self = this;
    Object.keys(this.aggr).forEach(function (owner) {
        var out = {
            owner: owner
        };

        self.namespaces.forEach(function (namespace) {
            out[namespace] = {
                directories: self.aggr[owner].dirs[namespace] || 0,
                keys: 0,
                objects: 0,
                bytes: new Big(0)
            };
        });

        Object.keys(self.aggr[owner].objects).forEach(function (objectId) {
            var size = self.aggr[owner].objects[objectId].size;
            var counts = self.aggr[owner].objects[objectId].counts;
            var counted = false;
            self.namespaces.forEach(function (namespace) {
                if (counts[namespace]) {
                    if (!counted) {
                        out[namespace].objects++;
                        out[namespace].bytes = out[namespace].bytes.plus(size);
                        counted = true;
                    }
                    out[namespace].keys += counts[namespace];
                }
            });
        });

        self.push(JSON.stringify(out, bigToString) + '\n');
    });
    cb();
};


function main() {
    var log = bunyan.createLogger({
        name: 'storage-reduce.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var options = [
        {
            name: 'namespaces',
            type: 'string',
            env: 'NAMESPACES',
            help: 'A list of comma-separated namespaces to include ' +
                'in storage reports (even if usage is empty)',
            default: 'stor public jobs reports'
        },
        {
            name: 'minSize',
            type: 'integer',
            env: 'MIN_SIZE',
            help: 'Minimum object size. If an object is less than ' +
                'this size, it is metered as being minSize.',
            default: 0
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
        console.error('storage-reduce: error: %s', e.message);
        process.exit(1);
    }

    var namespaces = opts.namespaces.split(' ');

    var reduceStream = new StorageReduce1Stream({
        namespaces: namespaces,
        minSize: opts.minSize,
        log: log
    });

    reduceStream.once('error', function (error) {
        log.error({error: error}, 'storage reduce phase 1 error');
        process.abort();
    });

    process.stdin.pipe(new lstream()).pipe(reduceStream).pipe(process.stdout);
}

if (require.main === module) {
    main();
}

module.exports = StorageReduce1Stream;
