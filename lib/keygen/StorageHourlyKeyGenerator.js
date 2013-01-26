// Copyright (c) 2013, Joyent, Inc. All rights reserved.
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
        mod_assert.object(opts.args.date, 'opts.args.date');

        // date methods return numbers not strings, so pad if needed
        function pad(num) { return (num < 10 ? '0' + num : num); }

        this.client = opts.client;
        this.log = opts.log;
        this.source = opts.args.source;

        var date = opts.args.date;
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

        function ifError(err) {
                if (err) {
                        self.log.fatal(err);
                        process.exit(1);
                }
        }

        function findDump(shard, cb) {
                self.client.ls(shard, function (err, res) {
                        ifError(err);
                        res.on('error', self.emit.bind(self, 'error'));
                        res.on('object', function (table) {
                                // the extra '-' at the end makes sure we get
                                // the manta instead of the manta_storage table
                                if (table.name.substr(0, 6) === 'manta-') {
                                        self.emit('key',
                                                shard + '/' + table.name);
                                }
                        });
                        res.once('end', function () {
                                cb(null, null);
                        });
                });
        }

        function onEachShard(shards) {
                mod_vasync.forEachParallel({
                        func: findDump,
                        inputs: shards
                }, function (err, res) {
                        self.emit('end');
                });
        }

        // first, find all the shards in the source directory
        self.client.ls(self.source, function (err, res) {
                var shards = [];
                ifError(err);
                res.on('directory', function (shard) {
                        var dirname = self.source + '/' + shard.name + '/' +
                                self.year + '/' + self.month + '/' + self.day +
                                '/' + self.hour;
                        shards.push(dirname);
                });
                res.on('error', self.emit.bind(self, 'error'));

                // then, find the dump we want from each shard
                res.once('end', onEachShard.bind(null, shards));
        });
};

module.exports.keygen = function (opts) {
        return (new StorageHourlyKeyGenerator(opts));
};
