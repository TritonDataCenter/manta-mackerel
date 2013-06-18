#!/usr/node/bin/node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var Big = require('big.js');
var mod_MemoryStream = require('readable-stream/passthrough.js');
var mod_carrier = require('carrier');
var mod_child_process = require('child_process');
var mod_events = require('events');
var mod_libmanta = require('libmanta');
var mod_manta = require('manta');
var mod_path = require('path');

var lookupPath = process.env['LOOKUP_FILE'] || '../etc/lookup.json';
var lookup = require(lookupPath); // maps uuid->login
var ERROR = false;

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
        var client = mod_manta.createClient({
                sign: null,
                url: process.env['MANTA_URL']
        });

        var queue = mod_libmanta.createQueue({
                limit: 10,
                worker: writeToUserDir
        });

        queue.on('end', client.close.bind(client));

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

        carry.on('line', onLine);
        carry.once('end', queue.close.bind(queue));
        process.stdin.resume();
}

if (require.main === module) {
        process.on('exit', function onExit() {
                process.exit(ERROR);
        });
        main();
}
