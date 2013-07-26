#!/usr/node/bin/node
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
 *              "directories": directories,
 *              "keys": keys,
 *              "objects": objects,
 *              "bytes": bytes
 *      },
 *      "public": {
 *              "directories": directories,
 *              "keys": keys,
 *              "objects": objects,
 *              "bytes": bytes
 *      },
 *      "reports": {
 *              "directories": directories,
 *              "keys": keys,
 *              "objects": objects,
 *              "bytes": bytes
 *      },
 *      "jobs": {
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
var ERROR = false;

var LOG = require('bunyan').createLogger({
        name: 'storage-reduce3.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

var EMPTY_USAGE = {
        directories: '0',
        keys: '0',
        objects: '0',
        bytes: '0'
};

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
                        LOG.error(e, 'Error on line ' + lineCount);
                        ERROR = true;
                        return;
                }
                delete record.owner;
                delete record.namespace;
                aggr[owner] = aggr[owner] || {
                        owner: owner,
                        storage: {}
                };
                aggr[owner].storage[namespace] = record;
        });

        carry.once('end', function onEnd(line) {
                Object.keys(aggr).forEach(function (o) {
                        for (var n = 0; n < namespaces.length; ++n) {
                                aggr[o].storage[namespaces[n]] =
                                        aggr[o].storage[namespaces[n]] ||
                                        EMPTY_USAGE;
                        }
                        console.log(JSON.stringify(aggr[o]));
                });
        });
}

if (require.main === module) {
        process.on('exit', function onExit() {
                process.exit(ERROR);
        });

        main();
}
