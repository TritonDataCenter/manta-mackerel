// Copyright (c) 2013, Joyent, Inc. All rights reserved.
/*
 * FindKeyGenerator
 *
 * Finds Manta keys that optionally match a regex one directory level deep from
 * the given path, similar to find(1) -mindepth 2 -maxdepth 2.
 * Example:
 * Given the following directories and files:
 *
 * /bit/
 * /foo/
 * /foo/bar/
 * /foo/bar/akey.txt
 * /foo/bar/key1.txt
 * /foo/bar/key2.txt
 * /foo/bar/quux/
 * /foo/bar/quux/key3.txt
 * /foo/baz/
 * /foo/baz/key4.txt
 *
 * Using this key generator with source = '/foo' and regex = 'key[0-9].txt'
 * would result in these keys being emitted:
 * /foo/bar/key1.txt
 * /foo/bar/key2.txt
 * /foo/baz/key4.txt
 *
 * Using this key generator with source = '/foo/bar' and regex = 'key' would
 * result in this key being emitted:
 * /foo/bar/quux/key3.txt
 *
 * This is used by metering to look for logs across an entire day or month. For
 * example, with a directory structure based on /year/month/day/hour/log.txt,
 * use this key generator with source = /year/month/day and regex = 'log.txt'
 * to find all log.txt files for all hours in the given day.
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
function FindKeyGenerator(opts) {
        mod_assert.object(opts, 'opts');
        mod_assert.object(opts.client, 'opts.client');
        mod_assert.object(opts.log, 'opts.log');

        mod_assert.object(opts.args, 'opts.args');
        mod_assert.string(opts.args.source, 'opts.args.source');
        mod_assert.optionalString(opts.args.regex, 'opts.args.regex');

        this.client = opts.client;
        this.log = opts.log;
        this.source = opts.args.source;
        this.regex = new RegExp(opts.args.regex);
}

mod_util.inherits(FindKeyGenerator, mod_events.EventEmitter);

FindKeyGenerator.prototype.start = function start() {
        var self = this;

        var barrier = mod_vasync.barrier();

        barrier.on('drain', function () {
                self.emit('end');
        });

        barrier.start(self.source);

        function findOneLevel(path, nest) {
                self.client.ls(path, function (err, res) {
                        if (err) {
                                self.emit('error', err);
                                barrier.done(path);
                                return;
                        }

                        res.on('object', function (obj) {
                                if (self.regex && !self.regex.test(obj.name)) {
                                        return;
                                }
                                self.emit('key', path + '/' + obj.name);
                        });

                        res.on('directory', function (dir) {
                                if (nest > 1) {
                                        return;
                                }
                                barrier.start(path + '/' + dir.name);
                                findOneLevel(path + '/' + dir.name, nest + 1);
                        });

                        res.on('error', self.emit.bind(self, 'error'));

                        res.once('end', function () {
                                barrier.done(path);
                        });
                });
        }

        findOneLevel(self.source, 0);
};

module.exports.keygen = function (opts) {
        return (new FindKeyGenerator(opts));
};
