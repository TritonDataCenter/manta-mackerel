/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * StorageHourlyKeyGenerator
 *
 * Finds Manta keys as job inputs for hourly storage metering.
 */

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_events = require('events');
var mod_manta = require('manta');
var mod_util = require('util');
var mod_vasync = require('vasync');
var mod_once = require('once');

// -- API

/*
 * emit('key', key)
 * emit('error', err)
 * emit('end')
 */
function StorageHourlyKeyGenerator(opts) {
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.client, 'client');
    mod_assert.object(opts.log, 'log');

    mod_assert.object(opts.args, 'opts.args');
    mod_assert.string(opts.args.source, 'opts.args.source');
    mod_assert.string(opts.args.date, 'opts.args.date');
    mod_assert.ok(!isNaN(Date.parse(opts.args.date)), 'invalid date');
    mod_assert.optionalArrayOfString(opts.args.shardBlacklist,
        'opts.args.shardBlacklist');
    mod_assert.optionalNumber(opts.args.minSize, 'opts.args.minSize');

    // date methods return numbers not strings, so pad if needed
    function pad(num) { return (num < 10 ? '0' + num : num); }
    var self = this;

    this.client = opts.client;
    this.log = opts.log;
    this.source = opts.args.source;
    this.minSize = opts.args.minSize || 0;
    this.blacklist = {};
    if (opts.args.shardBlacklist) {
        opts.args.shardBlacklist.forEach(function (s) {
            self.blacklist[s] = true;
        });
    }

    var date = new Date(opts.args.date);
    this.year = date.getUTCFullYear();
    this.month = pad(date.getUTCMonth() + 1); // Months start at 0, so add 1
    this.day = pad(date.getUTCDate());
    this.hour = pad(date.getUTCHours());
}

mod_util.inherits(StorageHourlyKeyGenerator, mod_events.EventEmitter);

/*
 * finds moray dumps from each shard (breadth-first)
 */
StorageHourlyKeyGenerator.prototype.start = function start() {
    var self = this;

    function findDump(shard, cb) {
        cb = mod_once(cb);
        self.client.ls(shard, function (err, res) {
            if (err) {
                cb(err);
                return;
            }
            var latest = null;
            var key = null;

            res.on('error', function (lserr) {
                cb(lserr);
                return;
            });

            res.on('object', function (table) {
                // the extra '-' at the end makes sure we get
                // the manta table instead of the manta_storage
                // table
                if (table.name.substr(0, 6) === 'manta-') {
                    if (!latest || table.mtime > latest) {
                        latest = table.mtime;
                        key = shard + '/' + table.name;
                    }
                }
            });

            res.once('end', function () {
                if (!key) {
                    cb(new Error(
                        'manta table dump not found' +
                        ' in ' + shard));
                    return;
                }
                self.client.info(key, function (suberr, info) {
                    if (suberr) {
                        cb(suberr);
                        return;
                    }
                    if (info.size < self.minSize) {
                        cb(new Error(key+ ' < ' +
                            self.minSize +
                            ' bytes.'));
                        return;
                    }
                    self.emit('key', key);
                    cb();
                    return;
                });
            });
        });
    }

    function onEachShard(shards, cb) {
        mod_vasync.forEachParallel({
            func: findDump,
            inputs: shards
        }, function (err, res) {
            cb(err, res);
        });
    }

    // first, find all the shards in the source directory
    self.client.ls(self.source, function (err, res) {
        var shards = [];
        if (err) {
            self.log.fatal(err);
            self.emit('error', err);
            return;
        }
        res.on('directory', function (shard) {
            var dirname = self.source + '/' + shard.name + '/' +
                self.year + '/' + self.month + '/' + self.day +
                '/' + self.hour;
            var shardNum = shard.name.split('.')[0];
            if (!self.blacklist[shardNum]) {
                shards.push(dirname);
            }
        });
        res.on('error', self.emit.bind(self, 'error'));

        // then, find the dump we want from each shard
        res.once('end', function onEnd() {
            onEachShard(shards, function (err2, res2) {
                if (err2) {
                    self.emit('error', err2);
                }
                self.emit('end');
            });
        });
    });
};

module.exports.keygen = function (opts) {
    return (new StorageHourlyKeyGenerator(opts));
};


/*
 * args: path/to/manta_config.json path/to/jobs.json date
 */
if (require.main === module) {
    var log = mod_bunyan.createLogger({
        name: 'StorageHourlyKeyGenerator',
        level: 'info'
    });
    var client = mod_manta.createClientFromFileSync(process.argv[2], log);
    var jobs = require(process.argv[3]).jobs;
    var keygen = new StorageHourlyKeyGenerator({
        client: client,
        log: log,
        args: {
            source: jobs.storage.keygenArgs.source,
            date: process.argv.slice(4).join(' ')
        }
    });
    keygen.on('key', console.log.bind(null));
    keygen.on('error', function (err) {
        console.log(err);
    });
    keygen.on('end', client.close.bind(client));
    keygen.start();
}
