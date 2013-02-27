#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('./carrier');

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

var _error = false;

/*
 * exit with exit code if needed to let marlin know something wrong happened
 *
 * _error is only set when we encounter an error where processing can continue
 * e.g. missing fields, etc
 */
process.on('exit', function () {
        process.exit(_error ? 12 : 0); // 12 chosen arbitrarily here
});

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

        carry.on('line', function onLine(line) {
                try {
                        var record = JSON.parse(line);
                        var owner = record.owner;
                        var namespace = record.namespace;
                } catch (e) {
                        console.log(e);
                        return;
                }
                record.owner = undefined;
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
