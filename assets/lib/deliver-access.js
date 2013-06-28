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
                typeof (record.req.owner) !== 'undefined');
}


function sanitize(record) {
        var output = mod_screen.screen(record, whitelist);
        if (output.req && output.req.headers) {
                var ip = output.req.headers['x-forwarded-for'] || '169.254.0.1';
                ip = ip.split(',')[0].trim();
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
        LOG.debug({record: record, output: output}, 'record sanitized');
        return (output);
}


function write(opts, cb) {
        LOG.debug(opts, 'write start');
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
                LOG.debug(owner, 'save start');
                var login = lookup[owner];
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
                url: process.env['MANTA_URL']
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

        LOG.info(Object.keys(files), 'files to upload');
        Object.keys(files).forEach(function (k) {
                uploadQueue.push(k);
        });
        uploadQueue.close();
}


function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var lineCount = 0;

        var writeQueue = mod_libmanta.createQueue({
                worker: write,
                limit: 15
        });

        writeQueue.on('error', function (err) {
                LOG.error(err, 'write error');
        });

        writeQueue.once('end', function () {
                saveAll(function (err) {
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
                        return;
                }

                try {
                        record = JSON.parse(line);
                } catch (e) {
                        LOG.error(e, line, 'Error on line ' + lineCount);
                        ERROR = true;
                        return;
                }

                if (!shouldProcess(record)) {
                        return;
                }

                var output = sanitize(record);

                writeQueue.push({
                        owner: record.req.owner,
                        record: output
                });
        }

        carry.once('end', writeQueue.close.bind(writeQueue));
        carry.on('line', onLine);
}

if (require.main === module) {
        process.on('exit', function onExit() {
                process.exit(ERROR);
        });

        main();
}
