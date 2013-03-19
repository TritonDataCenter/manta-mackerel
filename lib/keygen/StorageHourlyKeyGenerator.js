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
        mod_assert.string(opts.args.date, 'opts.args.date');

        // date methods return numbers not strings, so pad if needed
        function pad(num) { return (num < 10 ? '0' + num : num); }

        this.client = opts.client;
        this.log = opts.log;
        this.source = opts.args.source;

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
                self.client.ls(shard, function (err, res) {
                        if (err) {
                                self.log.fatal(err);
                                self.emit('error', err);
                                return;
                        }
                        res.on('error', self.emit.bind(self, 'error'));
                        res.on('object', function (table) {
                                // the extra '-' at the end makes sure we get
                                // the manta table instead of the manta_storage
                                // table
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
                if (err) {
                        self.log.fatal(err);
                        self.emit('error', err);
                        return;
                }
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


/*
 * args: path/to/manta_config.json path/to/config.js date
 */
if (require.main === module) {
        var log = mod_bunyan.createLogger({
                name: 'StorageHourlyKeyGenerator',
                level: 'info'
        });
        var client = mod_manta.createClientFromFileSync(process.argv[2], log);
        var config = require(process.argv[3]);
        var keygen = new StorageHourlyKeyGenerator({
                client: client,
                log: log,
                args: {
                        source: config.jobs.storage.hourly.keygenArgs.source,
                        date: process.argv.slice(4).join(' ')
                }
        });
        keygen.on('key', console.log.bind(null));
        keygen.start();
}
