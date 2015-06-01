#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */


var Writable = require('stream').Writable;
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var fs = require('fs');
var ipaddr = require('ipaddr.js');
var lstream = require('lstream');
var manta = require('manta');
var mantaFileSave = require('manta-compute-bin').mantaFileSave;
var screen = require('screener');
var util = require('util');
var vasync = require('vasync');


/*
 * Allow all headers, then auth and x-forwarded-for headers will be removed
 * after applying this whitelist. The IP in the x-forwarded-for header will
 * be used for the top-level remoteAddress IP.
 */
var whitelist = {
    'billable_operation': 'string',
    'req': {
        'method': 'string',
        'url': 'string', // renamed 'request-uri' see RFC 2616
        'headers': true, // XXX see above
        'httpVersion': 'string',
        'caller': {
            'login': 'string'
        }
    },
    'res': {
        'statusCode': 'number',
        'headers': true
    }
};


/*
 * Emits 'done' after all files have been uploaded to user directories.
 * The 'done' event fires after the Writable stream 'finish' event.
 */
function DeliverAccessStream(opts) {
    this.adminUser = opts.adminUser;
    this.backfill = opts.backfill;
    this.dryRun = opts.dryRun;
    this.deliverUnapproved = opts.deliverUnapproved;
    this.includeAdmin = opts.includeAdmin;
    this.log = opts.log;
    this.lookup = opts.lookup;
    this.mantaUrl = opts.mantaUrl;
    this.tmpdir = opts.tmpdir || '/var/tmp/';
    this.userDestination = opts.userDestination;
    this.userLink = opts.userLink;

    this.lineNumber = 0;
    this.files = {};

    this.once('finish', this._uploadFiles.bind(this));

    opts.decodeStrings = false;
    Writable.call(this, opts);
}
util.inherits(DeliverAccessStream, Writable);


DeliverAccessStream.prototype._write = function write(line, enc, cb) {
    var self = this;
    this.lineNumber++;

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

    if (!this._shouldProcess(record)) {
        cb();
        return;
    }

    var owner = record.req.owner;
    var output;
    try {
        output = JSON.stringify(this._sanitize(record)) + '\n';
    } catch (e) {
        cb(e);
        return;
    }

    if (!this.files[owner]) {
        this.files[owner] = fs.createWriteStream(this.tmpdir + '/' + owner);
        this.files.once('open', function () {
            var ok = self.files[owner].write(output);
            if (!ok) {
                self.files[owner].once('drain', cb);
            } else {
                setImmediate(cb);
            }
        });
    } else {
        var ok = self.files[owner].write(output);
        if (!ok) {
            this.files[owner].once('drain', cb);
        } else {
            setImmediate(cb);
        }
    }
};


DeliverAccessStream.prototype._shouldProcess = function _shouldProcess(record) {
    var isAudit = record.audit;
    if (!isAudit) {
        return (false);
    }

    var isPing = record.req.url === '/ping';
    var hasOwner = typeof (record.req.owner) !== 'undefined';
    var isAdmin = record.req.caller && record.req.caller.login === this.adminUser;

    var isApproved;
    if (!this.deliverUnapproved && hasOwner) {
        if (!this.lookup[record.req.owner]) {
            this.log.warn({
                record: record
            }, 'No login found for %s', record.req.owner);
            isApproved = true;
        } else {
            isApproved = this.lookup[record.req.owner].approved || isAdmin;
        }
    } else {
        isApproved = true;
    }

    return (isAudit &&
            !isPing &&
            hasOwner &&
            isApproved &&
            (this.includeAdmin || !isAdmin));
};


DeliverAccessStream.prototype._sanitize = function _sanitize(record) {
    var output = screen.screen(record, whitelist);
    if (output.req && output.req.headers) {
        var ip = output.req.headers['x-forwarded-for'] || '169.254.0.1';
        // MANTA-1918 if first ip is empty, use 'unknown'
        ip = ip.split(',')[0].trim() || 'unknown';

        // MANTA-1886 check for 'unknown'
        if (ip === 'unknown') {
            output.remoteAddress = 'unknown';
        } else {
            var addr = ipaddr.parse(ip);
            if (addr.kind() === 'ipv4') {
                output.remoteAddress = addr.toIPv4MappedAddress().toString();
            } else {
                output.remoteAddress = addr.toString();
            }
        }
        output.req['request-uri'] = output.req.url;
        delete output.req.headers.authorization;
        delete output.req.headers['x-forwarded-for'];
        delete output.req.url;
    }
    return (output);
};


DeliverAccessStream.prototype._uploadFiles = function _uploadFiles() {
    var self = this;
    var client = manta.createClient({
        sign: null,
        url: this.mantaUrl
    });

    var uploadQueue = vasync.queuev({
        worker: function saveFile(owner, cb) {
            var login = self.lookup[owner].login;

            if (!login) {
                cb(new Error('No login found for UUID ' + owner));
                return;
            }

            var filename = self.tmpdir + '/' + owner;
            var headers = {
                'content-type': 'application/x-json-stream'
            };
            var linkPath = '/' + login + self.userLink;

            var key;
            if (self.dryRun) {
                key = process.env.MANTA_OUTPUT_BASE + login;
            } else {
                key = '/' + login + self.userDestination;
            }

            mantaFileSave({
                client: client,
                filename: filename,
                headers: headers,
                iostream: 'stdout',
                key: key,
                log: self.log
            }, function (err) {
                if (err) {
                    cb(err);
                    return;
                }

                if (self.backfill || self.dryRun) {
                    cb();
                    return;
                }

                client.ln(key, linkPath, function (linkErr) {
                    if (linkErr) {
                        cb(linkErr);
                        return;
                    }
                    cb();
                });
            });
        },
        concurrency: 25
    });

    uploadQueue.push(Object.keys(this.files));
    uploadQueue.once('end', function () {
        self.emit('done');
    });
    uploadQueue.close();
};


function main() {
    var log = bunyan.createLogger({
        name: 'deliver-access.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var options = [
        {
            name: 'adminUser',
            type: 'string',
            env: 'ADMIN_USER',
            help: 'Manta admin user login'
        },
        {
            name: 'backfill',
            type: 'bool',
            env: 'BACKFILL',
            help: 'Don\'t create the "latest" link'
        },
        {
            name: 'dryRun',
            type: 'bool',
            env: 'DRY_RUN',
            help: 'Write in job directory instead of user directories'
        },
        {
            name: 'deliverUnapproved',
            type: 'bool',
            env: 'DELIVER_UNAPPROVED_REPORTS',
            help: 'Deliver personal access logs for users that have ' +
                    'approved_for_provisioning = false'
        },
        {
            name: 'includeAdmin',
            type: 'bool',
            env: 'INCLUDE_ADMIN_REQUESTS',
            help: 'Include requests by the the Manta admin user (i.e. poseidon)'
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
            help: 'Manta path to write access logs to relative ' +
                    'to the user\'s top-level directory ' +
                    'e.g. /reports/usage/access-logs/report.json'
        },
        {
            name: 'userLink',
            type: 'string',
            env: 'USER_LINK',
            help: 'Manta path to create a link to relative ' +
                    'to the user\'s top-level directory ' +
                    'e.g. /reports/usage/access-logs/latest'
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
        console.error('deliver-access: error: %s', e.message);
        process.exit(1);
    }

    if (opts.help) {
        var help = parser.help({includeEnv: true}).trimRight();
        console.log('usage: node deliver-access.js [OPTIONS]\n' +
                    'options:\n' +
                    help);
        process.exit(0);
    }

    if (!opts.hasOwnProperty('lookupPath')) {
        console.error('deliver-access: error: missing lookup file');
        process.exit(1);
    }

    var lookup = require(opts.lookupPath);

    var deliver = new DeliverAccessStream({
        adminUser: opts.adminUser,
        backfill: opts.backfill,
        dryRun: opts.dryRun,
        deliverUnapproved: opts.deliverUnapproved,
        includeAdmin: opts.includeAdmin,
        log: log,
        lookup: lookup,
        mantaUrl: opts.mantaUrl,
        userDestination: opts.userDestination,
        userLink: opts.userLink
    });

    deliver.once('error', function (error) {
        log.error(error, 'deliver access logs error');
        process.abort();
    });

    process.stdin.pipe(new lstream()).pipe(deliver).pipe(process.stdout);
}

if (require.main === module) {
    main();
}

module.exports = DeliverAccessStream;
