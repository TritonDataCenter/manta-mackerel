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
 * Uploads each user's usage record to their respective /reports directory and
 * creates a link to that record.
 */

var Transform = require('stream').Transform;
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var lstream = require('lstream');
var manta = require('manta');
var path = require('path');
var util = require('util');
var vasync = require('vasync');

function filter(key, value) {
    if (typeof (value) === 'number') {
        return (value.toString());
    }
    return (value);
}


function DeliverUsageStream(opts) {
    this.backfill = opts.backfill;
    this.dryRun = opts.dryRun;
    this.deliverUnapproved = opts.deliverUnapproved;
    this.log = opts.log;
    this.lookup = opts.lookup;
    this.mantaUrl = opts.mantaUrl;
    this.tmpdir = opts.tmpdir || '/var/tmp/';
    this.userDestination = opts.userDestination;
    this.userLink = opts.userLink;

    var uploadQueue = vasync.queuev({
        worker: this._upload.bind(this),
        concurrency: 25
    });
    var client = manta.createClient({
        sign: null,
        url: this.mantaUrl
    });

    this.uploadQueue = uploadQueue;
    this.mantaClient = client;
    this.lineNumber = 0;

    this.once('finish', function () {
        uploadQueue.close();
    });

    opts.decodeStrings = false;
    Transform.call(this, opts);
}
util.inherits(DeliverUsageStream, Transform);


DeliverUsageStream.prototype._transform = function transform(line, enc, cb) {
    this.lineNumber++;

    // re-emit input as output
    this.push(line + '\n');

    var record;
    try {
        record = JSON.parse(line);
    } catch (e) {
        this.emit('malformed', line);
        this.log.warn({
            error: e,
            line: line,
            lineNumber: this.lineNumber
        }, 'error parsing line ' + this.lineNumber);
        cb();
        return;
    }

    if (!this.lookup[record.owner]) {
        this.log.warn({
            record: record
        }, 'No login found for %s', record.owner);
        cb();
        return;
    }

    if (!this.deliverUnapproved && !this.lookup[record.owner].approved) {
        this.log.debug({record: record},
            '%s not approved for provisioning. Skipping...', record.owner);
        cb();
        return;
    }

    this.uploadQueue.push(record, function (err) {
        if (err) {
            this.log.error({
                err: err,
                record: record
            }, 'error uploading usage for record');
        }
    });
    cb();
};

DeliverUsageStream.prototype._upload = function _upload(record, cb) {
    var self = this;
    var login = this.lookup[record.owner].login;
    var client = this.mantaClient;

    var linkPath = '/' + login + this.userLink;
    var key;
    if (this.dryRun) {
        key = process.env.MANTA_OUTPUT_BASE + login;
    } else {
        key = '/' + login + this.userDestination;
    }

    vasync.pipeline({
        funcs: [
            function mkdir(_, pipelinecb) {
                var dir = path.dirname(key);
                client.mkdirp(dir, function (err) {
                    if (err) {
                        pipelinecb(err);
                        return;
                    }
                    pipelinecb();
                });
            },
            function put(_, pipelinecb) {

                // remove owner field from the user's personal report
                delete record.owner;

                // remove header bandwidth from request reports
                if (record.bandwidth) {
                    delete record.bandwidth.headerIn;
                    delete record.bandwidth.headerOut;
                }

                var line = JSON.stringify(record, filter) + '\n';
                var size = Buffer.byteLength(line);
                var options = {
                    size: size,
                    type: 'application/x-json-stream',
                    headers: {
                        'x-marlin-stream': 'stdout'
                    }
                };
                var writeStream = client.createWriteStream(key, options);
                writeStream.end(line, pipelinecb);
            },
            function link(_, pipelinecb) {
                if (self.backfill || self.dryRun) {
                    pipelinecb();
                    return;
                }
                client.ln(key, linkPath, function (err) {
                    if (err) {
                        pipelinecb(err);
                        return;
                    }
                    pipelinecb();
                });
            }
        ]
    }, function (err) {
        if (err) {
            cb(err);
            return;
        }
        cb();
    });
};



function main() {
    var log = bunyan.createLogger({
        name: 'deliver-usage.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var options = [
        {
            name: 'backfill',
            type: 'bool',
            env: 'BACKFILL',
            help: 'Don\'t create the "latest" link'
        },
        {
            name: 'deliverUnapproved',
            type: 'bool',
            env: 'DELIVER_UNAPPROVED_REPORTS',
            help: 'Do not deliver personal usage reports for users that have ' +
                    'approved_for_provisioning = false'
        },
        {
            name: 'dryRun',
            type: 'bool',
            env: 'DRY_RUN',
            help: 'Write in job directory instead of user directories'
        },
        {
            name: 'lookupPath',
            type: 'string',
            env: 'LOOKUP_PATH',
            default: '../etc/lookup.json',
            help: 'Path to lookup file'
        },
        {
            name: 'mantaUrl',
            type: 'string',
            env: 'MANTA_URL',
            help: 'Manta URL'
        },
        {
            name: 'userDestination',
            type: 'string',
            env: 'USER_DEST',
            help: 'Manta path to write usage reports to relative ' +
                    'to the user\'s top-level directory ' +
                    'e.g. /reports/usage/storage/report.json'
        },
        {
            name: 'userLink',
            type: 'string',
            env: 'USER_LINK',
            help: 'Manta path to create a link to relative ' +
                    'to the user\'s top-level directory ' +
                    'e.g. /reports/usage/storage/latest'
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
        console.error('deliver-usage: error: %s', e.message);
        process.exit(1);
    }

    if (opts.help) {
        var help = parser.help({includeEnv: true}).trimRight();
        console.log('usage: node deliver-usage.js [OPTIONS]\n' +
                    'options:\n' +
                    help);
        process.exit(0);
    }

    if (!opts.hasOwnProperty('lookupPath')) {
        console.error('deliver-usage: error: missing lookup file');
        process.exit(1);
    }

    var lookup = require(opts.lookupPath);

    var deliver = new DeliverUsageStream({
        backfill: opts.backfill,
        dryRun: opts.dryRun,
        deliverUnapproved: opts.deliverUnapproved,
        log: log,
        lookup: lookup,
        mantaUrl: opts.mantaUrl,
        userDestination: opts.userDestination,
        userLink: opts.userLink
    });

    deliver.once('error', function (error) {
        log.error(error, 'deliver usage error');
        process.abort();
    });

    process.stdin.pipe(new lstream()).pipe(deliver).pipe(process.stdout);
}

if (require.main === module) {
    main();
}
