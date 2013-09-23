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
var mod_libmanta = require('libmanta');


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
        if (typeof (opts.args.source) === 'string') {
                opts.args.source = [ opts.args.source ];
        }
        mod_assert.arrayOfString(opts.args.source, 'opts.args.source');
        mod_assert.optionalString(opts.args.regex, 'opts.args.regex');
        mod_assert.optionalNumber(opts.args.minSize, 'opts.args.minSize');

        this.client = opts.client;
        this.log = opts.log;
        this.source = opts.args.source;
        this.regex = new RegExp(opts.args.regex);
        this.minSize = opts.args.minSize || 0;
}

mod_util.inherits(FindKeyGenerator, mod_events.EventEmitter);

FindKeyGenerator.prototype.start = function start() {
        var self = this;

        var queue = new mod_libmanta.Queue({
                worker: function (task, cb) {
                        task(cb);
                },
                limit: 24
        });

        for (var s = 0; s < self.source.length; s++) {
                queue.push(findOneLevel.bind(null, self.source[s], 0));
        }

        queue.on('error', function (err) {
                if (err) {
                        self.emit('error', err);
                        self.emit('end');
                }
        });

        queue.once('drain', function () {
                queue.once('end', self.emit.bind(self, 'end'));
                queue.close();
        });

        function getInfo(path, cb) {
                self.client.info(path, function (err, info) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        if (info.size < self.minSize) {
                                cb(new Error(path + ' < ' + self.minSize +
                                        ' bytes.'));
                                return;
                        }
                        cb(null, path);
                });
        }

        function findOneLevel(path, nest, cb) {
                self.client.ls(path, function (err, res) {
                        if (err) {
                                cb(err);
                                return;
                        }

                        res.on('object', function (obj) {
                                if (self.regex && !self.regex.test(obj.name)) {
                                        return;
                                }
                                var objPath = path + '/' + obj.name;

                                queue.push((function (p, subcb) {
                                        getInfo(p, function (infoerr) {
                                                if (infoerr) {
                                                        subcb(infoerr);
                                                        return;
                                                }
                                                self.emit('key', p);
                                                subcb();
                                        });
                                }).bind(null, objPath));
                        });

                        res.on('directory', function (dir) {
                                if (nest > 1) {
                                        return;
                                }
                                var dirPath = path + '/' + dir.name;
                                queue.push(findOneLevel.bind(null,
                                        dirPath, nest + 1));
                        });

                        res.once('error', function (suberr) {
                                if (suberr) {
                                        cb(suberr);
                                }
                        });

                        res.once('end', function () {
                                cb();
                        });
                });
        }

};

module.exports.keygen = function (opts) {
        return (new FindKeyGenerator(opts));
};
