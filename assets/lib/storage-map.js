#!/usr/node/bin/node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('carrier');
var lookupPath = process.env['LOOKUP_FILE'] || '../etc/lookup.json';
var lookup = require(lookupPath); // maps uuid->login
var ERROR = false;
var COUNT_UNAPPROVED_USERS = process.env['COUNT_UNAPPROVED_USERS'] === 'true';

var LOG = require('bunyan').createLogger({
        name: 'storage-map.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

function validSchema(obj) {
        var fields =
                ['key', 'owner', 'type'];
        for (var i = 0; i < fields.length; i++) {
                if (!obj[fields[i]]) {
                        return (false);
                }
        }
        return (true);
}


function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var index;
        var lineCount = 0;

        function onLine(line) {
                lineCount++;
                try {
                        var record = JSON.parse(line);
                } catch (e) {
                        LOG.error(e, 'Error on line ' + lineCount);
                        ERROR = true;
                        return;
                }

                if (!record.entry || !record.entry[index]) {
                        LOG.error(line, 'unrecognized line ' + lineCount);
                        ERROR = true;
                        return;
                }

                try {
                        var value = JSON.parse(record.entry[index]);
                        if (!validSchema(value)) {
                                LOG.error(line, 'invalid line ' + lineCount);
                                ERROR = true;
                                return;
                        }
                } catch (e) {
                        LOG.error(e, 'Error on line ' + lineCount);
                        ERROR = true;
                        return;
                }

                if (!COUNT_UNAPPROVED_USERS) {
                        if (!lookup[value.owner]) {
                                LOG.error(record, 'No login found for UUID ' +
                                        value.owner);
                                ERROR = true;
                                return;
                        }

                        if (!lookup[value.owner].approved) {
                                LOG.warn(record, value.owner +
                                        ' not approved for provisioning. ' +
                                        'Skipping...');
                                return;
                        }
                }

                console.log(JSON.stringify(value));
        }

        carry.once('line', function firstLine(line) {
                lineCount++;
                try {
                        index = JSON.parse(line).keys.indexOf('_value');
                } catch (e) {
                        LOG.fatal(e, line, 'error parsing schema');
                        ERROR = true;
                        return;
                }
                carry.on('line', onLine);
        });

}

if (require.main === module) {

        process.on('exit', function onExit() {
                process.exit(ERROR);
        });

        main();
}
