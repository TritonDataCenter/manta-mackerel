#!/usr/bin/env node
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_events = require('events');
var mod_manta = require('manta');
var mod_util = require('util');
var mod_vasync = require('vasync');

function pad(num) {
        return (num < 10 ? '0' + num : num);
}

/*
 * replaces $year, $month, etc in str with the appropriate values
 */
function replaceWithDate(str, date) {

        var year = date.getFullYear();
        var month = pad(date.getMonth() + 1);
        var day = pad(date.getDate());
        var hour = pad(date.getHours());

        var result = str.replace(/\$year/g, year);
        result = result.replace(/\$month/g, month);
        result = result.replace(/\$day/g, day);
        result = result.replace(/\$hour/g, hour);

        return (result);
}

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


        this.client = opts.client;
        this.log = opts.log;
        this.source = opts.args.source;

        var date = opts.args.date;
        this.year = date.getFullYear();
        this.month = pad(date.getMonth() + 1);
        this.day = pad(date.getDate());
        this.hour = pad(date.getHours());
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


// -- Main
// run this file directly to get a list of keys for the given date
function main() {
//TODO getopts

        var dateString = process.argv.slice(2).join(' ');
        if (isNaN(Date.parse(dateString))) {
                console.error('Invalid date: %s', dateString);
                process.exit(1);
        }

        var config = require('../../cfg/config.js');

        var log = mod_bunyan.createLogger({
                level: (process.env.LOG_LEVEL || 'info'),
                name: 'mackerel',
                stream: process.stdout
        });

        var manta_config = config.manta_config_file;
        var client = mod_manta.createClientFromFileSync(manta_config, log);

        config.jobs.storage.hourly.keygenArgs.date = new Date(dateString);
        var keygen = new StorageHourlyKeyGenerator({
               client: client,
               log: log,
               args: config.jobs.storage.hourly.keygenArgs
        });

        keygen.on('key', function (key) {
                console.log(key);
        });

        keygen.start();
}

if (require.main === module) {
        main();
}

module.exports.keygen = function (opts) {
        return (new StorageHourlyKeyGenerator(opts));
};
