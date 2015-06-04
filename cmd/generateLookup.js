#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var bunyan = require('bunyan');
var mahi = require('mahi');
var manta = require('manta');
var PassThrough = require('stream').PassThrough;

function fetchAndUpload(opts, cb) {
    var log = opts.log;
    var key = opts.key;
    var mantaClient = opts.mantaClient;
    var mahiClient = opts.mahiClient;

    log.info('requesting lookup table');
    mahiClient.getLookup(function (err, lookup) {
        if (err) {
            log.error({err: err}, 'error fetching lookup table');
            cb(err);
            return;
        }

        log.info('lookup table fetched, uploading');

        var string = JSON.stringify(lookup);
        var size = Buffer.byteLength(string);

        var options = {
            type: 'application/json',
            size: size,
            copies: 1
        };

        var stream = new PassThrough();
        mantaClient.put(key, stream, options, function (err) {
            if (err) {
                log.error({err: err}, 'error uploading table');
                cb(err);
                return;
            }
            log.info('upload complete');
            cb();
        });
        stream.end(string);
    });
}

if (require.main === module) {
    (function main() {
        var log = bunyan.createLogger({
            name: 'generateLookup',
            level: process.env.LOG_LEVEL || 'info'
        });
        var config = require('../etc/config.json');
        var mahiClient = mahi.createClient(config.mahi);
        var mantaClient = manta.createClient(config.manta);
        var key = config.lookupPath;
        fetchAndUpload({
            key: key,
            log: log,
            mantaClient: mantaClient,
            mahiClient: mahiClient
        }, function (err) {
            if (err) {
                process.exit(1);
            }
            mantaClient.close();
            mahiClient.close();
        });
    })();
}
