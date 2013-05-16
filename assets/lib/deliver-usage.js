#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('carrier');
var mod_child_process = require('child_process');
var mod_events = require('events');
var mod_path = require('path');
var mod_manta = require('manta');
var mod_MemoryStream = require('readable-stream/passthrough.js');

var lookup = require('../etc/lookup.json'); // maps uuid->login
var ERROR = false;

function zero(obj) {
        Object.keys(obj).forEach(function (k) {
                if (typeof (obj[k]) === 'object') {
                        zero(obj[k]);
                } else if (typeof (obj[k]) === 'number') {
                        obj[k] = 0;
                }
        });
        return (obj);
}

function writeToUserDir(record, login, client, cb) {
        var path = '/' + login + process.env['USER_DEST'];
        var line = JSON.stringify(record) + '\n';
        var size = Buffer.byteLength(line);
        var mstream = new mod_MemoryStream();

        client.mkdirp(mod_path.dirname(path), function (err) {
                if (err) {
                        console.warn('Error mkdirp ' + mod_path.dirname(path));
                        console.warn(err);
                        cb(err);
                        return;
                }

                var opts = {
                        size: size,
                        type: process.env['HEADER_CONTENT_TYPE']
                };

                client.put(path, mstream, opts, function (err2) {
                        if (err2) {
                                console.warn('Error put ' + path);
                                console.warn(err2);
                                cb(err2);
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

        var client = mod_manta.createClient({
                sign: null,
                url: process.env['MANTA_URL']
        });

        var carry = mod_carrier.carry(process.openStdin());

        function onLine(line) {
                console.log(line); // re-emit input as output

                var record = JSON.parse(line);
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

                writeToUserDir(record, login, client, function (err) {
                        if (err) {
                                // error printed by parent
                                ERROR = true;
                        }
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
                        console.log(JSON.stringify(emptyRecord));
                        delete emptyRecord.owner;

                        writeToUserDir(emptyRecord, login, client,
                                function checkError(err) {
                                if (err) {
                                        ERROR = true;
                                }
                        });
                });
        });
}

if (require.main === module) {
        process.on('exit', function onExit() {
                process.exit(ERROR);
        });
        main();
}
