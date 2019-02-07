#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/* BEGIN JSSTYLED */
/*
 * The map phase extracts the JSON object of manta objects from the manta
 * moray dumps. The dumps structure matches the structure of a postgres
 * table. The first row has the list of the columns in "keys" attributs.
 *
 * {
 *   "name": "manta",
 *   "keys": [
 *    "_id",
 *    "_txn_snap",
 *    "_key",
 *    "_value",
 *    "_etag",
 *    "_mtime",
 *    "_vnode",
 *    "dirname",
 *    "name",
 *    "owner",
 *    "objectid",
 *    "type"
 *  ]
 * }
 *
 * The rest of the lines have the below structure.
 *
 * {
 *  "entry": [
 *    "481402737",
 *    "\\N",
 *    "/4ebc3ff1-dd83-40a6-fc38-9ca15a0cfbac/stor/amber/6252/865",
 *    "{\"dirname\":\"/4ebc3ff1-dd83-40a6-fc38-9ca15a0cfbac/stor/amber/6252\",\"key\":\"/4ebc3ff1-dd83-40a6-fc38-9ca15a0cfbac/stor/amber/6252/865\",\"headers\":{},\"mtime\":1499377315253,\"name\":\"865\",\"creator\":\"4ebc3ff1-dd83-40a6-fc38-9ca15a0cfbac\",\"owner\":\"4ebc3ff1-dd83-40a6-fc38-9ca15a0cfbac\",\"roles\":[],\"type\":\"object\",\"contentLength\":5196,\"contentMD5\":\"Kt5aSUjem6RMw20VIjh2XQ==\",\"contentType\":\"image/jpeg\",\"etag\":\"3fbf3c97-02ba-4bee-da1f-f809bda9b4cc\",\"objectId\":\"3fbf3c97-02ba-4bee-da1f-f809bda9b4cc\",\"sharks\":[{\"datacenter\":\"us-east-2\",\"manta_storage_id\":\"4.stor.us-east.joyent.us\"},{\"datacenter\":\"us-east-3\",\"manta_storage_id\":\"7.stor.us-east.joyent.us\"}],\"vnode\":8104251}",
 *    "832E628F",
 *    "1499377315267",
 *    "8104251",
 *    "/4ebc3ff1-dd83-40a6-fc38-9ca15a0cfbac/stor/amber/6252",
 *    "865",
 *    "4ebc3ff1-dd83-40a6-fc38-9ca15a0cfbac",
 *    "3fbf3c97-02ba-4bee-da1f-f809bda9b4cc",
 *    "object"
 *  ]
 * }
 *
 * The storage-map performs the below tasks:
 *   - Validates the dump schema.
 *   - Extracts the object JSON from every entry.
 *   - Dispatches the json object to one of the reducers.
 */

var mod_stream = require('stream');
var mod_getopt = require('posix-getopt');
var mod_uuid = require('uuid');
var mod_lstream = require('lstream');

var ZSplitter = require('./zsplitter');
var MUploader = require('./muploader');

var lookupPath = process.env['LOOKUP_FILE'] || '../etc/lookup.json';
var lookup = require(lookupPath); // maps uuid->login
var COUNT_UNAPPROVED_USERS = process.env['COUNT_UNAPPROVED_USERS'] === 'true';

var log = require('bunyan').createLogger({
        name: 'storage-map.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});


var index;
var lineCount = 0;
var process_line;

function validSchema(obj) {
        var fields = ['key', 'owner', 'type'];

        for (var i = 0; i < fields.length; i++) {
                if (!obj[fields[i]]) {
                        return (false);
                }
        }
        return (true);
}

function fatal(message)
{
        log.fatal(message);
        process.exit(1);
}

function processLine(zs, line, opts, cb) {
        lineCount++;
        try {
                var record = JSON.parse(line);
        } catch (e) {
                fatal('Error on line: ' + lineCount);
        }

        if (record.name === 'manta' && Array.isArray(record.keys)) {
                // This is a header record. It is safe to skip it.
                cb();
                return;
        }

        if (!record.entry || !record.entry[index]) {
                fatal('Unrecognized line: ' + lineCount);
        }

        try {
                var value = JSON.parse(record.entry[index]);
                if (!validSchema(value)) {
                        fatal('Invalid line: ' + lineCount);
                }
        } catch (e) {
                fatal('Error on line: ' + lineCount);
        }

        if (!COUNT_UNAPPROVED_USERS) {
                if (!lookup[value.owner]) {
                        fatal('No login found for UUID: ' + value.owner);
                }

                if (!lookup[value.owner].approved) {
                        log.warn(record, value.owner +
                            ' not approved for provisioning. ' +
                            'Skipping...');
                                cb();
                                return;
                }
        }

        if (!opts.directUpload) {
                console.log(JSON.stringify(value));
                cb();
                return;
        }

        // Construct the split key
        var splitKey = '';
        opts.splitKeys.forEach(function (key) {
                splitKey = splitKey + value[key];
        });

        // Write to splitter specifying the split key.
        zs.write(JSON.stringify(value) + '\n', splitKey, cb);
}

function processFirstLine(zs, line, opts, cb) {
        process_line = processLine;
        lineCount++;
        try {
                index = JSON.parse(line).keys.indexOf('_value');
        } catch (e) {
                fatal('Error parsing schema');
        }
        cb();
}

function processStdin(zs, opts) {
        var transform = new mod_stream.Transform({ objectMode: true });
        process_line = processFirstLine;

        transform._transform = function (chunk, encoding, done) {
                process_line(zs, chunk + '\n', opts, done);
        };

        // In the case of direct upload, we need to tell all the reducers
        // we are done. Otherwise, we will be waiting for the 'close' event
        // to be fired forever.
        if (opts.directUpload) {
                transform._flush = function (done) {
                        for (var r = 0; r < opts.nReducers; r++)
                                zs.end(r);
                        done();
                };
        }

        // Start processing stdin
        process.stdin.pipe(new mod_lstream()).pipe(transform);
}

function uploadFiles(mu, fileNames, objectNames) {
        mu.uploadReducerFiles(fileNames, objectNames, function (err) {
                if (err) {
                        fatal('Error uploading the files' + err.toString());
                        return;
                }
                //done
        });
}

function main() {
        var opts = {
                directUpload: false,
                nReducers: 0,
                splitKeys: ['']
        };

        var parser = new mod_getopt.BasicParser('n:s:u', process.argv);
        var option;
        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'n':
                        opts.nReducers = parseInt(option.optarg, 10);
                        if (isNaN(opts.nReducers) || opts.nReducers < 1) {
                                fatal('invalid number of reducers ' +
                                    option.optarg);
                        }
                        break;
                case 's':
                        opts.splitKeys = option.optarg.split(',');
                        break;
                case 'u':
                        opts.directUpload = true;
                        break;
                default:
                        fatal('Invalid option: ' + option.option);
                        break;
                }
        }

        if ((opts.directUpload && !opts.nReducers) ||
            (!opts.directUpload && opts.nReducers)) {
                fatal('Setting the number of reducers is required ' +
                    'when choosing direct upload, and vice versa');
        }

        if (opts.directUpload && !process.env['MANTA_OUTPUT_BASE']) {
                fatal('Setting MANTA_OUTPUT_BASE is required ' +
                    'when choosing direct upload');
        }

        if (opts.nReducers > 1 && opts.splitKeys.length == 1 &&
            opts.splitKeys[0] === '') {
                fatal('Please specifiy one of more split keys when ' +
                    'setting the number of reducers to more than one');
        }

        if (!opts.directUpload) {
                processStdin(null, opts);
                return;
        }

        var zs = new ZSplitter('/var/tmp', opts.nReducers);

        // Start processing stdin when the splitter is ready.
        zs.on('open', processStdin.bind(null, zs, opts));
        zs.on('error', function (err) {
                fatal(err.toString());
        });

        // Upload the files when we are done.
        zs.on('close', function () {
                var n;
                var mu = new MUploader(log);
                var objectPrefix = process.env['MANTA_OUTPUT_BASE'] +
                    mod_uuid.v4() + '.';
                var objectNames = [];
                for (n = 0; n < opts.nReducers; n++) {
                        objectNames.push(objectPrefix + n);
                }

                uploadFiles(mu, zs.getFileNames(), objectNames);
        });
}

main();
