#!/usr/node/bin/node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var lookupPath = process.env['LOOKUP_FILE'] || '../etc/lookup.json';
var lookup = require(lookupPath); // maps uuid->login
var mantaFileSave = require('manta-compute-bin').mantaFileSave;
var mod_ipaddr = require('ipaddr.js');
var mod_bunyan = require('bunyan');
var mod_carrier = require('carrier');
var mod_fs = require('fs');
var mod_manta = require('manta');
var mod_screen = require('screener');
var mod_libmanta = require('libmanta');

var files = {}; // keeps track of all the files we create
var ERROR = false;
var tmpdir = '/var/tmp';

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
                typeof (record.req.owner) !== 'undefined');
}


function sanitize(record) {
        var output = mod_screen.screen(record, whitelist);
        if (output.req && output.req.headers) {
                var ip = output.req.headers['x-forwarded-for'] || '169.254.0.1';
                var ipaddr = mod_ipaddr.parse(ip);
                if (ipaddr.kind() === 'ipv4') {
                        output['remoteAddress'] =
                                ipaddr.toIPv4MappedAddress().toString();
                } else {
                        output['remoteAddress'] = ipaddr.toString();
                }
                output.req['request-uri'] = output.req['url'];
                delete output.req.headers['x-forwarded-for'];
                delete output.req['url'];
                delete output.req.headers['authorization'];
        }
        return (output);
}


function write(opts, cb) {
        var owner = opts.owner;
        var record = opts.record;
        var path = tmpdir + '/' + owner;
        var output = JSON.stringify(record) + '\n';

        if (!files[owner]) {
                files[owner] = true;
                mod_fs.writeFile(path, output, function (err) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        cb();
                });
        } else {
                mod_fs.appendFile(path, output, function (err) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        cb();
                });
        }

}


function saveAll(cb) {
        function save(owner, callback) {
                var login = lookup[owner];
                var key = '/' + login + process.env['ACCESS_DEST'];
                var headers = {
                        'content-type': process.env['HEADER_CONTENT_TYPE']
                };

                if (!login) {
                        callback(new Error('No login found for UUID ' + owner));
                        return;
                }

                mantaFileSave({
                        client: client,
                        filename: tmpdir + '/' + owner,
                        key: key,
                        headers: headers,
                        log: log,
                        iostream: 'stderr'
                }, function saveCB(err) {
                        if (err) {
                                callback(err);
                                return;
                        }
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

        var queue = mod_libmanta.createQueue({
                worker: save,
                limit: 25
        });

        queue.on('error', function (err) {
                console.warn('error saving: ' + err.message);
                ERROR = true;
        });

        queue.once('end', function () {
                client.close();
                cb();
        });

        Object.keys(files).forEach(function (k) {
                queue.push(k);
        });
        queue.close();
}


function main() {
        var carry = mod_carrier.carry(process.openStdin());

        var queue = mod_libmanta.createQueue({
                worker: write,
                limit: 15
        });

        queue.on('error', function (err) {
                console.warn('write error: ' + err.message);
        });

        queue.once('end', function () {
                saveAll(function (err) {
                        if (err) {
                                console.warn('saveAll error:' + err.message);
                                process.exit(1);
                        }
                });
        });

        function onLine(line) {
                var record;
                //console.log(line); // re-emit each line for aggregation

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

                queue.push({
                        owner: record.req.owner,
                        record: output
                });
        }

        carry.once('end', queue.close.bind(queue));
        carry.on('line', onLine);
}

main();
