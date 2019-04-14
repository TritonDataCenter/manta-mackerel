#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * This is the phase that does the real accounting of customers' usage in manta.
 * It receives the a stream of JSON objects representing objects and directories
 * stored in manta, and produces a summary usage record for each user.
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
 *     {
 *       "availableMB": 20477,
 *       "percentUsed": 1,
 *       "datacenter": "bh1-kvm1",
 *       "server_uuid": "44454c4c-4700-1034-804a-c7c04f354d31",
 *       "zone_uuid": "ef8b166a-ac3e-4d59-bb73-a65e2b17ba44",
 *       "url": "http://ef8b166a-ac3e-4d59-bb73-a65e2b17ba44.stor.bh1-kvm1.joyent.us"
 *     },
 *     {
 *       "availableMB": 20477,
 *       "percentUsed": 1,
 *       "datacenter": "bh1-kvm1",
 *       "server_uuid": "44454c4c-4700-1034-804a-c7c04f354d31",
 *       "zone_uuid": "59fb8bd3-67a7-4da2-bb68-287e2db01ec1",
 *       "url": "http://59fb8bd3-67a7-4da2-bb68-287e2db01ec1.stor.bh1-kvm1.joyent.us"
 *     }
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
 *      owner: {
 *              "dirs": {
 *                      namespace: 0,
 *                      ...
 *              },
 *              "objs": {
 *                      namespace: 0,
 *                      ...
 *              },
 *              "keys": {
 *                      namespace: 0,
 *                      ...
 *              },
 *              "bytes": {
 *                      namespace: 0,
 *                      ...
 *              },
 *      },
 *      ...
 * }
 *
 * SQLite table structure
 *
 * ------------------------------------
 * |  OWNER_UUID   |    OBJECT_UUID   |
 * ------------------------------------
 * |               |                  |
 * ------------------------------------
 * |               |                  |
 * ------------------------------------
 */

/*
 * output format:
 * {
 *      "owner": owner,
 *      "namespace": namespace,
 *      "directories": directories,
 *      "keys": keys,
 *      "objects": objects,
 *      "bytes": bytes
 * }
 */

/* END JSSTYLED */

var lstream = require('lstream');
var stream = require('stream');
var fs = require('fs');
var sqlite3 = require('sqlite3');
var Big = require('big.js');


var sstmt = null;	// This is a prepared select statement
var istmt = null;	// A prepared insert statement

var MIN_SIZE = +process.env['MIN_SIZE'] || 131072;


var LOG = require('bunyan').createLogger({
        name: 'storage-reduce1.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

var NAMESPACES = (process.env.NAMESPACES).split(' ');
var DBFILE = +process.env['DBFILE'] || '/var/tmp/objects.db';

function fatal(message)
{
        LOG.fatal(message);
        process.exit(1);
}

function processLine(record, aggr, db, done) {
        var owner = record.owner;
        var type = record.type;
        var namespace;
        var n;

        try {
                namespace = record.key.split('/')[2]; // /:uuid/:namespace/...
        } catch (_e) {
                fatal('Error getting namespace: ' + record.key);
                return;
        }

        // Create an owner record if we haven't see this account before
        if (!aggr[owner]) {
                aggr[owner] = {
                    dirs: {},
                    objs: {},
                    keys: {},
                    bytes: {}
                };

                for (n in NAMESPACES) {
                        aggr[owner].dirs[NAMESPACES[n]] = 0;
                        aggr[owner].objs[NAMESPACES[n]] = 0;
                        aggr[owner].keys[NAMESPACES[n]] = 0;
                        aggr[owner].bytes[NAMESPACES[n]] = new Big(0);
                }
        }

         // Ignore objects and directories in other namespaces
        if (!(aggr[owner].bytes[namespace])) {
                done();
                return;
        }

        if (type === 'directory') {
                aggr[owner].dirs[namespace]++;
                done();
                return;
        } else if (type === 'object') {
                var objectId, size;
                try {
                        objectId = record.objectId;
                        size = Math.max(record.contentLength, MIN_SIZE) *
                            record.sharks.length;
                } catch (e) {
                        fatal('Error processing object record\n' +
                            record + '\n' + e);
                        return;
                }

                // Add the owner, objectId to sqlite table
                istmt.run([owner, objectId], function (er) {
                        if (er) {
                                /*
                                 * It is ok if we failed to insert the
                                 * object record because it already
                                 * exists. In this case, we increment
                                 * the number of keys and return.
                                 */
                                if (er.code === 'SQLITE_CONSTRAINT') {
                                        aggr[owner].keys[namespace]++;
                                        done();
                                } else {
                                        fatal('sqlite3 error: ' + er);
                                }
                                return;
                        }

                        aggr[owner].keys[namespace]++;
                        aggr[owner].objs[namespace]++;
                        aggr[owner].bytes[namespace] =
                            aggr[owner].bytes[namespace].plus(size);
                        done();
                });

        } else {
                fatal('unrecognized object type: ' + type + '\n' + record);
        }
}

function printResults(aggr) {
        var n, bytes;
        Object.keys(aggr).forEach(function (owner) {
                for (n in NAMESPACES) {
                        bytes = aggr[owner].bytes[NAMESPACES[n]].toString();
                        console.log(JSON.stringify({
                                owner: owner,
                                namespace: NAMESPACES[n],
                                directories: aggr[owner].dirs[NAMESPACES[n]],
                                keys: aggr[owner].keys[NAMESPACES[n]],
                                objects: aggr[owner].objs[NAMESPACES[n]],
                                bytes: bytes
                        }));
                }
        });
}


function main() {

        var aggr = {};
        var lineCount = 0;

        try {
                fs.unlinkSync(DBFILE);
        } catch (e) {
        }

        function sqlite3_execute(sqdb, cmd, args) {
                sqdb.run(cmd, args, function (error) {
                        if (error) {
                                fatal('sqlite3: error executing '
                                    + '"' + cmd + '"\n' + error);
                        }
                });
        }

        function sqlite3_prepare_stmt(sqdb, stmt, args) {
                return (sqdb.prepare(stmt, args, function (error) {
                        if (error) {
                                fatal('sqlite3: error executing ' +
                                    '"' + stmt + '"\n' + error);
                        }
                }));
        }

        var db = new sqlite3.cached.Database(DBFILE);
        db.serialize(function () {
                // We are trading safety for performance
                sqlite3_execute(db, 'PRAGMA synchronous = OFF', []);
                sqlite3_execute(db, 'PRAGMA journal_mode = OFF', []);
                sqlite3_execute(db, 'PRAGMA locking_mode = EXCLUSIVE', []);
                // 1.5 GB of cache.
                sqlite3_execute(db, 'PRAGMA cache_size = 393216', []);
                // This will create an index on (owner_uuid, object_uuid)
                sqlite3_execute(db, 'CREATE TABLE objects ' +
                    '(owner_uuid TEXT, object_uuid TEXT, ' +
                    'PRIMARY KEY (owner_uuid, object_uuid))', []);

                sstmt = sqlite3_prepare_stmt(db,
                    'SELECT * FROM objects ' +
                    'WHERE owner_uuid = ? and object_uuid = ?', []);
                istmt = sqlite3_prepare_stmt(db,
                    'INSERT INTO objects VALUES (?, ?)', []);

                sqlite3_execute(db, 'BEGIN TRANSACTION', []);
        });


        var transform = new stream.Transform({ objectMode: true });

        transform._transform = function (line, _encoding, done) {
                lineCount++;
                try {
                        var record = JSON.parse(line);
                } catch (e) {
                        fatal('Error parsing line: ' + lineCount +
                            '\n"' + line + '"\n' + e);
                        return;
                }

                if (!record.owner || !record.type) {
                        fatal('Missing owner or type field on line: ' +
                                lineCount + '\n' + record);
                }
                processLine(record, aggr, db, done);
        };

        // Print the output when done
        transform._flush =  printResults.bind(null, aggr);

        process.stdin.pipe(new lstream()).pipe(transform);

}

if (require.main === module) {
        main();
}
