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

var LOG = require('bunyan').createLogger({
        name: 'deliver-usage.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

function writeToUserDir(opts, cb) {
        LOG.debug(opts, 'writeToUserDir start');
        var record = opts.record;
        if (process.env['DATE']) {
                record.date = process.env['DATE'];
        }
        var login = opts.login;
        var client = opts.client;
        var linkPath = '/' + login + process.env['USER_LINK'];
        var path = '/' + login + process.env['USER_DEST'];
        var line = JSON.stringify(record, filter) + '\n';
        var size = Buffer.byteLength(line);
        var mstream = new mod_MemoryStream();
        var dir = mod_path.dirname(path);

        LOG.debug(dir, 'creating directory');
        client.mkdirp(dir, function (err) {
                if (err) {
                        LOG.error(err, 'error mkdirp ' + dir);
                        ERROR = true;
                        cb(err);
                        return;
                }

                LOG.info(dir, 'directory created');

                var options = {
                        size: size,
                        type: process.env['HEADER_CONTENT_TYPE'],
                        'x-marlin-stream': 'stderr'
                };

                LOG.debug({path: path, options: options}, 'putting ' + path);
                client.put(path, mstream, options, function (err2) {
                        if (err2) {
                                LOG.error(err2, 'error put ' + path);
                                ERROR = true;
                                cb(err2);
                                return;
                        }
                        LOG.info(path, 'put successful');
                        if (!process.env['USER_LINK']) {
                                cb();
                                return;
                        }
                        LOG.debug({
                                path: path,
                                linkPath: linkPath
                        }, 'creating link');
                        client.ln(path, linkPath, function (err3) {
                                if (err3) {
                                        LOG.warn(err3, 'error ln ' + linkPath);
                                        ERROR = true;
                                        cb(err3);
                                        return;
                                }
                                LOG.info({
                                        path: path,
                                        linkPath: linkPath
                                }, 'link created');
                                cb();
                                return;
                        });
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
                        LOG.error(record,
                                'No login found for UUID ' + record.owner);
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
