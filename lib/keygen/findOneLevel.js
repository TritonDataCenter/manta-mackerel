// Copyright (c) 2012, Joyent, Inc. All rights reserved.

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
                        if (nest > 1) {
                                return;
                        }
                        if (err) {
                                self.emit('error', err);
                        }

                        res.on('object', function (obj) {
                                if (self.regex && !self.regex.test(obj.name)) {
                                        return;
                                }
                                self.emit('key', path + '/' + obj.name);
                        });

                        res.on('directory', function (dir) {
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
