#!/usr/node/bin/node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('carrier');
var mod_child_process = require('child_process');
var mod_events = require('events');
var mod_path = require('path');
var mod_manta = require('manta');
var mod_MemoryStream = require('readable-stream/passthrough.js');
var Big = require('big.js');
var mod_vasync = require('vasync');

var lookupPath = process.env['LOOKUP_FILE'] || '../etc/lookup.json';
var lookup = require(lookupPath); // maps uuid->login
var ERROR = false;

function zero(obj) {
        Object.keys(obj).forEach(function (k) {
                if (typeof (obj[k]) === 'object') {
                        zero(obj[k]);
                } else if (typeof (obj[k]) === 'number') {
                        obj[k] = 0;
                } else if (typeof (obj[k]) === 'string') {
                        // if the string represents an integer, set it to '0'
                        // otherwise leave it alone
                        try {
                                obj[k] = new Big(obj[k]);
                                obj[k] = '0';
                        } catch (e) {
                                // leave it as is
                        }
                }
        });
        return (obj);
}

function filter(key, value) {
        if (typeof (value) === 'number') {
                return (value.toString());
        }
        return (value);
}

function writeToUserDir(opts, cb) {
        var record = opts.record;
        var login = opts.login;
        var client = opts.client;
        var path = '/' + login + process.env['USER_DEST'];
        var line = JSON.stringify(record, filter) + '\n';
        var size = Buffer.byteLength(line);
        var mstream = new mod_MemoryStream();

        client.mkdirp(mod_path.dirname(path), function (err) {
                if (err) {
                        console.warn('Error mkdirp ' + mod_path.dirname(path));
                        console.warn(err);
                        ERROR = true;
                        cb();
                        return;
                }

                var options = {
                        size: size,
                        type: process.env['HEADER_CONTENT_TYPE'],
                        'x-marlin-stream': 'stderr'
                };

                client.put(path, mstream, options, function (err2) {
                        if (err2) {
                                console.warn('Error put ' + path);
                                console.warn(err2);
                                ERROR = true;
                                cb();
                                return;
                        }
                        cb();
                });

                process.nextTick(function () {
                        mstream.write(line);
                        mstream.end();
                });
        });

}

function main() {
        var emptyRecord;
        var queue = mod_vasync.queue(writeToUserDir, 50);

        var client = mod_manta.createClient({
                sign: null,
                url: process.env['MANTA_URL']
        });

        var carry = mod_carrier.carry(process.stdin);

        function onLine(line) {
                var record = JSON.parse(line);

                // re-emit input as output but with numbers converted to strings
                console.log(JSON.stringify(record, filter));

                var login = lookup[record.owner];

                if (!login) {
                        console.warn('No login found for UUID ' + record.owner);
                        ERROR = true;
                        return;
                }

                // we shouldn't encounter this user again; remove his entry
                // in the lookup table so that we will only have users with
                // no usage left at the end
                delete lookup[record.owner];

                // remove owner field from the user's personal report
                delete record.owner;

                // remove header bandwidth from request reports
                if (record.bandwidth) {
                        delete record.bandwidth.headerIn;
                        delete record.bandwidth.headerOut;
                }

                queue.push({
                        record: record,
                        login: login,
                        client: client
                });
        }

        carry.once('line', function firstLine(line) {
                // generate a record with all number fields set to 0 based on
                // the format of the first real record, to be used for users
                // with no usage
                emptyRecord = zero(JSON.parse(line));

                // XXX special case for compute: empty the time section
                if (emptyRecord.time) {
                        emptyRecord.time = {};
                }

                carry.on('line', onLine);
                onLine(line);
        });

        carry.once('end', function onEnd() {
                // lookup should only contain users with no usage now
                var uuids = Object.keys(lookup);
                uuids.forEach(function (k) {
                        var login = lookup[k];

                        if (!emptyRecord) {
                                console.warn('Error: an empty record template' +
                                        ' was never created. Perhaps no input' +
                                        ' was read?');
                                process.exit(1);
                        }

                        emptyRecord.owner = k;
                        console.log(JSON.stringify(emptyRecord, filter));
                        delete emptyRecord.owner;

                        queue.push({
                                record: emptyRecord,
                                login: login,
                                client: client
                        });
                });
                queue.drain = client.close.bind(client);
        });
        process.stdin.resume();
}

if (require.main === module) {
        process.on('exit', function onExit() {
                process.exit(ERROR);
        });
        main();
}
