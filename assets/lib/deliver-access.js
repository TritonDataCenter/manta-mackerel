#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var lookup = require('../etc/lookup.json'); // maps uuid->login
var mantaFileSave = require('marlin/lib/util.js').mantaFileSave;
var mod_bunyan = require('bunyan');
var mod_carrier = require('carrier');
var mod_fs = require('fs');
var mod_manta = require('manta');
var mod_screen = require('screener');
var mod_vasync = require('vasync');

var files = {}; // keeps track of all the files we create
var ERROR = false;

/*
 * Allow all headers, then auth and x-forwarded-for headers will be removed
 * after applying this whitelist. The IP in the x-forwarded-for header will
 * be used for the top-level remoteAddress IP.
 */
var whitelist = {
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
                typeof (record.req.owner) !== 'undefined');
}


function sanitize(record) {
        var output = mod_screen.screen(record, whitelist);
        if (output.req && output.req.headers) {
                output['remoteAddress'] = output.req.headers['x-forwarded-for'];
                output.req.headers['request-uri'] = output.req['url'];
                delete output.req.headers['x-forwarded-for'];
                delete output.req.headers['url'];
                delete output.req.headers['authorization'];
        }
        return (output);
}


function write(owner, record, cb) {
        files[owner] = true;
        mod_fs.appendFile(owner, JSON.stringify(record) + '\n', function (err) {
                if (err) {
                        cb(err);
                        return;
                }
                cb();
        });
}


function saveAll(cb) {
        function save(owner, callback) {
                var login = lookup[owner];
                var key = '/' + login + process.env['ACCESS_DEST'];

                if (!login) {
                        console.warn('No login found for UUID ' + owner);
                        return;
                }

                mantaFileSave({
                        client: client,
                        filename: owner,
                        key: key,
                        headers: {type: process.env['HEADER_CONTENT_TYPE']},
                        log: log,
                        iostream: 'stdout'
                }, function saveCB(err) {
                        if (err) {
                                callback(err);
                                return;
                        }
                        console.log(key);
                        callback(null, key);
                });
        }

        var log = new mod_bunyan({
                name: 'deliver-audit',
                level: 'fatal',
                stream: 'stderr'
        });

        var client = mod_manta.createClient({
                sign: null,
                url: process.env['MANTA_URL']
        });

        mod_vasync.forEachParallel({
                func: save,
                inputs: Object.keys(files)
        }, function (err, results) {
                if (err) {
                        cb(err);
                        return;
                }
                cb(null, results);
        });
}


function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var barrier = mod_vasync.barrier();

        function onLine(line) {
                var record;
                console.log(line); // re-emit each line for aggregation

                // since bunyan logs may contain lines such as
                // [ Nov 28 21:35:27 Enabled. ]
                // we need to ignore them
                if (line[0] != '{') {
                        return;
                }

                try {
                        record = JSON.parse(line);
                } catch (e) {
                        return;
                }

                if (!shouldProcess(record)) {
                        return;
                }

                var output = sanitize(record);
                barrier.start(line);
                write(record.req.owner, output, function (err) {
                        if (err) {
                                console.warn(err.message);
                        }
                        barrier.done(line);
                });
        }

        function onDrain() {
                saveAll(function (err, results) {
                        if (err) {
                                console.warn(err);
                                process.exit(1);
                        }
                });
        }

        carry.once('end', barrier.done.bind(null, 'lines'));
        barrier.once('drain', onDrain);

        barrier.start('lines');
        carry.on('line', onLine);
}

main();
