#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('./carrier');

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
 *              "counts": {
 *                      namespace: {
 *                              "directories": directories,
 *                              "keys": keys,
 *                              "objects": objects,
 *                              "bytes": bytes
 *                      }
 *                      ...
 *              },
 *              "objectIds": {
 *                      objectId: null,
 *                      ...
 *              }
 *      },
 *      ...
 * }
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

var _error = false;

/*
 * exit with exit code if needed to let marlin know something wrong happened
 *
 * _error is only set when we encounter an error where processing can continue
 * e.g. malformed line, unrecognized type
 */
process.on('exit', function () {
        process.exit(_error ? 12 : 0); // 12 chosen arbitrarily here
});

function addOwner(owner, aggr) {
        aggr[owner] = {
                counts: {},
                objectIds: {}
        };
}

function addNamespace(namespace, aggr) {
        aggr.counts[namespace] = {
                directories: 0,
                keys: 0,
                objects: 0,
                bytes: 0
        };
}

function count(record, aggr) {
        var owner = record.owner;
        var type = record.type;
        var namespace;
        try {
                namespace = record.key.split('/')[2]; // /:uuid/:namespace/...
        } catch (e) {
                console.warn(e);
                _error = true;
                return;
        }

        if (typeof (aggr[owner]) === 'undefined') {
                addOwner(owner, aggr);
        }

        if (typeof (aggr[owner].counts[namespace]) === 'undefined') {
                addNamespace(namespace, aggr[owner]);
        }

        if (type === 'directory') {
                aggr[owner].counts[namespace].directories += 1;
        } else if (type === 'object') {
                try {
                        var objectId = record.objectId;
                        var size = record.contentLength * record.sharks.length;
                } catch (e) {
                        console.warn(e);
                        _error = true;
                        return;
                }


                aggr[owner].counts[namespace].keys += 1;

                // check for unique object
                if (typeof (aggr[owner].objectIds[objectId]) === 'undefined') {
                        aggr[owner].objectIds[objectId] = null; // add to index
                        aggr[owner].counts[namespace].objects += 1;
                        aggr[owner].counts[namespace].bytes += size;
                }
        } else {
                console.warn('Unrecognized object type: ' + record);
                _error = true;
        }

}


function printResults(aggr) {
        Object.keys(aggr).forEach(function (owner) {
                Object.keys(aggr[owner].counts).forEach(function (namespace) {
                        var counts = aggr[owner].counts[namespace];
                        var output = {
                                owner: owner,
                                namespace: namespace,
                                directories: counts.directories,
                                keys: counts.keys,
                                objects: counts.objects,
                                bytes: counts.bytes
                        };
                        console.log(JSON.stringify(output));
                });
        });
}


function main() {
        var carry = mod_carrier.carry(process.openStdin());

        var aggr = {};
        carry.on('line', function onLine(line) {
                try {
                        var record = JSON.parse(line);
                } catch (e) {
                        _error = true;
                        console.warn(e);
                        return;
                }

                if (!record.owner || !record.type) {
                        _error = true;
                        console.warn('Missing owner or type field: ' + line);
                        return;
                }

                count(record, aggr);
        });

        carry.on('end', function onEnd() {
                printResults(aggr);
        });
}

if (require.main === module) {
        main();
}
