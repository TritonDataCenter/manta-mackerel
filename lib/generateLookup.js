#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_assert = require('assert-plus');
var mod_libmanta = require('libmanta');
var mod_bunyan = require('bunyan');

module.exports = generateLookup;

/*
 * Generates the lookup table that maps uuid->login via redis.
 *
 * - host: mahi host
 * - port: mahi port
 * - log: logger passed to the client
 * - (optional) maxParallel
 * - (optioal) redis_options
 * - cb: callback in the form f(err, result), where result is an object that
 *   maps uuid -> login.
 */
function generateLookup(opts, cb) {
    mod_assert.object(opts, 'opts');
    mod_assert.func(cb, 'cb');

    function getLogin(uuid, callback) {
        mahi.userFromUUID(uuid, function gotUser(err, user) {
            if (err) {
                cb(err);
                return;
            }
            result[uuid] = {
                login: user.login,
                approved: user.approved_for_provisioning
            };
            callback();
        });
    }

    var result = {};
    var mahi = mod_libmanta.createMahiClient(opts);
    var queue = mod_libmanta.createQueue({
        limit: 10,
        worker: getLogin
    });

    queue.once('error', cb.bind(null));
    queue.once('end', function onEnd() {
        mahi.close();
        cb(null, result);
    });

    mahi.once('error', cb.bind(null));
    mahi.once('connect', function onConnect() {
        mahi.setMembers('uuid', function gotUuids(err, uuids) {
            if (err) {
                cb(err);
                return;
            }
            for (var uuid in uuids) {
                queue.push(uuids[uuid]);
            }
            queue.close();
        });
    });
}

if (require.main === module) {
    var log = mod_bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'generateLookup',
        stream: process.stderr
    });

    generateLookup({
        host: process.argv[2],
        port: process.argv[3],
        log: log
    }, function (err, result) {
        console.log(JSON.stringify(result, null, 2));
    });
}
