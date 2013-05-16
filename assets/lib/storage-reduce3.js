#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.


/* BEGIN JSSTYLED */
/*
 * input record format:
 * {
 *      "owner": owner,
 *      "namespace": namespace,
 *      "directories": directories,
 *      "keys": keys,
 *      "objects": objects,
 *      "bytes": bytes
 * }
 */

/*
 * output format:
 * {
 *      "owner": owner,
 *      "stor": {
 *              "namespace": "stor",
 *              "directories": directories,
 *              "keys": keys,
 *              "objects": objects,
 *              "bytes": bytes
 *      },
 *      "public": {
 *              "namespace": "public",
 *              "directories": directories,
 *              "keys": keys,
 *              "objects": objects,
 *              "bytes": bytes
 *      },
 *      "reports": {
 *              "namespace": "reports",
 *              "directories": directories,
 *              "keys": keys,
 *              "objects": objects,
 *              "bytes": bytes
 *      },
 *      "jobs": {
 *              "namespace": "jobs",
 *              "directories": directories,
 *              "keys": keys,
 *              "objects": objects,
 *              "bytes": bytes
 *      }
 * }
 */

/*
 * aggr format:
 * {
 *      owner: {
 *              "owner": owner,
 *              namepace: {
 *                      "namespace": namespace,
 *                      "directories": directories,
 *                      "keys": keys,
 *                      "objects": objects,
 *                      "bytes: bytes
 *              },
 *              ...
 *      ],
 *      ...
 * }
 *
 */

/* END JSSTYLED */
var mod_carrier = require('carrier');

function emptyUsage(namespace) {
        return ({
                namespace: namespace,
                directories: 0,
                keys: 0,
                objects: 0,
                bytes: 0
        });
}

function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var aggr = {};
        var namespaces = (process.env.NAMESPACES).split(' ');
        var lineCount = 0;

        carry.on('line', function onLine(line) {
                try {
                        var record = JSON.parse(line);
                        var owner = record.owner;
                        var namespace = record.namespace;
                } catch (e) {
                        console.warn('Error on line ' + lineCount + ':' + e);
                        return;
                }
                delete record.owner;
                delete record.namespace;
                aggr[owner] = aggr[owner] || {owner: owner};
                aggr[owner][namespace] = record;
        });

        carry.once('end', function onEnd(line) {
                Object.keys(aggr).forEach(function (o) {
                        for (var n = 0; n < namespaces.length; ++n) {
                                aggr[o][namespaces[n]] =
                                        aggr[o][namespaces[n]] ||
                                        emptyUsage(namespaces[n]);
                        }
                        console.log(JSON.stringify(aggr[o]));
                });
        });
}

if (require.main === module) {
        main();
}
