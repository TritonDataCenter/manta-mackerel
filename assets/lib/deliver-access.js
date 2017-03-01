#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var lookupPath = process.env['LOOKUP_FILE'] || '../etc/lookup.json';
var lookup = require(lookupPath); // maps uuid->login
var mantaFileSave = require('manta-compute-bin').mantaFileSave;
var mod_ipaddr = require('ipaddr.js');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_manta = require('manta');
var mod_screen = require('screener');
var mod_libmanta = require('libmanta');
var mod_util = require('util');
var mod_stream = require('stream');
var mod_assert = require('assert-plus');
var mod_lstream = require('lstream');

var ERROR = false;
var tmpdir = '/var/tmp';
var DELIVER_UNAPPROVED_REPORTS =
        process.env['DELIVER_UNAPPROVED_REPORTS'] === 'true';
var DROP_POSEIDON_REQUESTS = process.env['DROP_POSEIDON_REQUESTS'] === 'true';
var MALFORMED_LIMIT = process.env['MALFORMED_LIMIT'] || '0';
var ERROR_RE_BAD_IP = /the address has neither IPv6 nor IPv4 format/;

var LOG = require('bunyan').createLogger({
        name: 'deliver-access.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

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


function shouldProcess(record) {
        return (record.audit &&
                record.req.url !== '/ping' &&
                typeof (record.req.owner) !== 'undefined' &&
                (!record.req.caller ||
                        !DROP_POSEIDON_REQUESTS ||
                        record.req.caller.login !== 'poseidon'));
}


function sanitize(record) {
        var output = mod_screen.screen(record, whitelist);
        if (output.req && output.req.headers) {
                var ip = output.req.headers['x-forwarded-for'] || '169.254.0.1';
                var ip_parsed = null;

                // MANTA-1918 if first ip is empty, use 'unknown'
                ip = ip.split(',')[0].trim() || 'unknown';

                // MANTA-1886 check for 'unknown'
                if (ip !== 'unknown') {
                        try {
                                ip_parsed = mod_ipaddr.parse(ip);
                        } catch (ex) {
                                if (!ERROR_RE_BAD_IP.test(ex.message))
                                        throw (ex);
                        }
                }

                if (ip_parsed === null) {
                        output['remoteAddress'] = 'unknown';
                } else {
                        if (ip_parsed.kind() === 'ipv4') {
                                output['remoteAddress'] =
                                    ip_parsed.toIPv4MappedAddress().toString();
                        } else {
                                output['remoteAddress'] = ip_parsed.toString();
                        }
                }

                output.req['request-uri'] = output.req['url'];
                delete output.req.headers['x-forwarded-for'];
                delete output.req['url'];
                delete output.req.headers['authorization'];
        }
        LOG.debug({record: record, output: output}, 'record sanitized');
        return (output);
}


function saveAll(filenameList, cb) {
        mod_assert.arrayOfString(filenameList, 'filenameList');
        mod_assert.func(cb, 'cb');

        function save(owner, callback) {
                LOG.debug(owner, 'save start');
                var login = lookup[owner].login;
                var key = '/' + login + process.env['ACCESS_DEST'];
                var linkPath = '/' + login + process.env['ACCESS_LINK'];
                var headers = {
                        'content-type': process.env['HEADER_CONTENT_TYPE']
                };
                var filename = tmpdir + '/' + owner;

                if (!login) {
                        callback(new Error('No login found for UUID ' + owner));
                        return;
                }

                mantaFileSave({
                        client: client,
                        filename: filename,
                        key: key,
                        headers: headers,
                        log: LOG,
                        iostream: 'stderr'
                }, function saveCB(err) {
                        if (err) {
                                callback(err);
                                return;
                        }

                        LOG.info({
                                filename: filename,
                                key: key,
                                headers: headers
                        }, 'upload successful');

                        if (!process.env['ACCESS_LINK']) {
                                callback();
                                return;
                        }

                        LOG.debug({
                                key: key,
                                linkPath: linkPath
                        }, 'creating link');

                        client.ln(key, linkPath, function (err2) {
                                if (err2) {
                                        LOG.warn(err2, 'error ln ' + linkPath);
                                        return;
                                }

                                LOG.info({
                                        key: key,
                                        linkPath: linkPath
                                }, 'link created');

                                callback();
                                return;
                        });
                });
        }

        var client = mod_manta.createClient({
                sign: null,
                url: process.env['MANTA_URL'],
                rejectUnauthorized: !process.env['MANTA_TLS_INSECURE']
        });

        var uploadQueue = mod_libmanta.createQueue({
                worker: save,
                limit: 25
        });

        uploadQueue.on('error', function (err) {
                LOG.error(err, 'error saving');
                ERROR = true;
        });

        uploadQueue.once('end', function () {
                client.close();
                cb();
        });

        LOG.info(filenameList, 'files to upload');
        filenameList.forEach(function (k) {
                uploadQueue.push(k);
        });
        uploadQueue.close();
}


function DeliverAccessStream(options) {
        var self = this;

        mod_assert.object(options, 'options');
        mod_assert.func(options.processFunc, 'options.processFunc');
        mod_assert.string(options.outputDir, 'options.outputDir');

        mod_stream.Writable.call(this, {
                objectMode: true,
                highWaterMark: 0
        });

        self.das_processFunc = options.processFunc;
        self.das_outputDir = options.outputDir;

        self.das_lineCount = 0;
        self.das_malformedCount = 0;

        self.das_files = {};
        self.das_nfiles = 0;

        self.das_finished = false;
        self.on('finish', function onFinish() {
                self.das_finished = true;

                /*
                 * End the write stream for all of the files we opened.
                 * Push this to the next tick so that consumer "finish"
                 * events can run first.
                 */
                setImmediate(function endAllFiles() {
                        for (var fn in self.das_files) {
                                if (!self.das_files.hasOwnProperty(fn)) {
                                        continue;
                                }

                                self.das_files[fn].end();
                        }
                });
        });
}
mod_util.inherits(DeliverAccessStream, mod_stream.Writable);

DeliverAccessStream.prototype._commit = function dasCommit(action, done) {
        var self = this;

        mod_assert.string(action.filename, 'action.filename');
        mod_assert.object(action.record, 'action.record');
        mod_assert.func(done, 'done');

        var output = JSON.stringify(action.record) + '\n';

        var file = self.das_files[action.filename];
        mod_assert.object(file, 'file: ' + action.filename);

        if (!file.write(output)) {
                /*
                 * This file is blocked for writes.  To avoid exhausting
                 * available memory with buffered records, hold processing
                 * until the file stream has drained.
                 */
                file.once('drain', function fileOnDrain() {
                        done();
                });
                return;
        }

        setImmediate(done);
};

DeliverAccessStream.prototype._write = function dasWrite(line, _, done) {
        var self = this;

        mod_assert.string(line, 'line');

        self.das_lineCount++;

        var action;
        if ((action = self.das_processFunc(line)) === null) {
                setImmediate(done);
                return;
        }

        mod_assert.string(action.filename, 'action.filename');
        mod_assert.object(action.record, 'action.record');

        /*
         * Check to see if we've already opened this file.
         */
        if (!self.das_files[action.filename]) {
                /*
                 * Open the file, holding processing until the open
                 * completes.
                 */
                var path = mod_path.join(self.das_outputDir, action.filename);
                var fstr = mod_fs.createWriteStream(path);

                fstr.once('open', function fstrOnOpen() {
                        self._commit(action, done);
                });

                fstr.once('finish', function fstrFinish() {
                        mod_assert.strictEqual(self.das_finished, true,
                                'file "' + action.filename + '" finished ' +
                                'before input processing was done');
                        mod_assert.ok(self.das_nfiles > 0, 'nfiles > 0');

                        /*
                         * Once all files are finished streaming out, we
                         * emit a final event.
                         */
                        if (--self.das_nfiles === 0) {
                                self.emit('filesDone', Object.keys(
                                        self.das_files));
                        }
                });

                self.das_files[action.filename] = fstr;
                self.das_nfiles++;
                return;
        }

        /*
         * The file is already open.  Write the record immediately.
         */
        self._commit(action, done);
};


function main() {
        var lineCount = 0;
        var malformed = 0;

        var das = new DeliverAccessStream({
                processFunc: onLine,
                outputDir: tmpdir
        });

        das.once('filesDone', function (filenameList) {
                saveAll(filenameList, function (err) {
                        if (err) {
                                LOG.error(err, 'Error saving access logs');
                                ERROR = true;
                        }
                });
        });

        function onLine(line) {
                lineCount++;
                var record;
                //console.log(line); // re-emit each line for aggregation

                // since bunyan logs may contain lines such as
                // [ Nov 28 21:35:27 Enabled. ]
                // we need to ignore them
                if (line[0] != '{') {
                        return (null);
                }

                try {
                        record = JSON.parse(line);
                } catch (e) {
                        malformed++;
                        LOG.error(e, line, 'Error on line ' + lineCount);
                        return (null);
                }

                if (!shouldProcess(record)) {
                        return (null);
                }

                var login = lookup[record.req.owner];

                if (!login) {
                        LOG.error(record,
                                'No login found for UUID ' + record.req.owner);
                        ERROR = true;
                        return (null);
                }

                if (!DELIVER_UNAPPROVED_REPORTS && !login.approved) {
                        LOG.warn(record, record.req.owner +
                                ' not approved for provisioning. Skipping...');
                        return (null);
                }

                var output;
                try {
                        output = sanitize(record);
                } catch (e) {
                        LOG.error(e, 'Error sanitizing record');
                        ERROR = true;
                        return (null);
                }

                return ({
                        filename: record.req.owner,
                        record: output
                });
        }

        das.once('finish', function onFinish() {
                var len = MALFORMED_LIMIT.length;
                var threshold;

                if (MALFORMED_LIMIT[len - 1] === '%') {
                        var pct = +(MALFORMED_LIMIT.substr(0, len-1));
                        threshold = pct * lineCount;
                } else {
                        threshold = +MALFORMED_LIMIT;
                }

                if (isNaN(threshold)) {
                        LOG.error('MALFORMED_LIMIT not a number');
                        ERROR = true;
                        return;
                }

                if (malformed > threshold) {
                        LOG.fatal('Too many malformed lines');
                        ERROR = true;
                        return;
                }
        });

        process.stdin.pipe(new mod_lstream()).pipe(das);
}

if (require.main === module) {
        process.on('exit', function onExit(code) {
                if (code === 0) {
                        process.exit(ERROR ? 1 : 0);
                }
        });

        main();
}
