/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * ZSplitter splits input into multipe files compressed in gzip format.
 */

var mod_assert = require('assert-plus');
var mod_crypto = require('crypto');
var mod_fs = require('fs');
var mod_util = require('util');
var EventEmitter = require('events').EventEmitter;
var mod_zlib = require('zlib');

var MAX_BUFFER_SIZE = 128 * 1024; //128 KB

function ZSplitter(dir, nFiles) {
        mod_assert.string(dir, 'dir');
        mod_assert.number(nFiles, 'nFiles');
        mod_assert.ok(nFiles > 0 && nFiles % 1 === 0);

        var stat = mod_fs.statSync(dir);
        mod_assert.ok(stat.isDirectory());

        // Emit all errors we get from gzip and file streams
        var onError = function (err) {
                this.emit('error', err);
        }.bind(this);

        this._nFiles = nFiles;          // Number of files
        this._fileNames = [];           // Temorary fileNames
        this._gzipStreams = [];         // Gzip streams

        this._nReadyStreams = 0;
        this._nClosedStreams = 0;

        var self = this;
        for (var n = 0; n < nFiles; n++) {
                var gzipStream = mod_zlib.createGzip();
                this._gzipStreams.push(gzipStream);
                gzipStream.on('error', onError);
                gzipStream.buffer = '';
                gzipStream.bufferLength = 0;

                var fileName = dir + '/part' + n;
                this._fileNames.push(fileName);
                var fileStream = mod_fs.createWriteStream(fileName);
                gzipStream.pipe(fileStream);
                fileStream.on('error', onError);
                fileStream.on('open', function () {
                        // Emit 'open' when all the underlying
                        // files are open. This tells the user
                        // we are ready to receive writes.
                        if (++self._nReadyStreams == nFiles) {
                                self.emit('open');
                        }
                });

                fileStream.on('close', function () {
                        // Emit close when all the outputfiles
                        // are closed. Only then, the user can
                        // use the split files.
                        if (++self._nClosedStreams == nFiles) {
                                self.emit('close');
                        }
                });
        }
}

mod_util.inherits(ZSplitter, EventEmitter);

// Writes 'data' to a reducer based on 'splitKey' hash value.
ZSplitter.prototype.write = function (data, splitKey, cb) {
        mod_assert.string(data);
        mod_assert.string(splitKey);
        mod_assert.func(cb);

        var digest = mod_crypto.createHash('md5')
            .update(splitKey).digest('hex');
        var fileNumber = parseInt(digest.substr(0, 8), 16) % this._nFiles;
        var gz = this._gzipStreams[fileNumber];

        // Buffer the data if there is a room for it.
        if (gz.bufferLength + data.length < MAX_BUFFER_SIZE) {
                gz.buffer += data;
                gz.bufferLength += data.length;
                cb();
                return;
        }

        // If there is no room to buffer the data
        // and the stream is ready to receive writes,
        // then write both the buffer content and the
        // new data.
        if (gz.writable) {
                gz.write(gz.buffer + data);
                gz.buffer = '';
                gz.bufferLength = 0;
                cb();
                return;
        }

        // Otherwise, wait for the stream to drain
        // before writing the all the data;
        gz.once('drain', function () {
                gz.write(gz.buffer + data);
                gz.buffer = '';
                gz.bufferLength = 0;
                cb();
        });
};

ZSplitter.prototype.end = function (fileNumber) {
        mod_assert.number(fileNumber);

        if (fileNumber >= this._nFiles) {
                this.emit('error', 'fileNumber is out of range:' + fileNumber);
                return;
        }

        var gz = this._gzipStreams[fileNumber];

        if (!gz.writable) {
                gz.once('drain', function () {
                        gz.end(gz.buffer);
                });
                return;
        }

        gz.end(gz.buffer);
};

ZSplitter.prototype.getFileNames = function () {
        // Return a copy of the array to protect
        // the integrity of private members.
        return (this._fileNames.slice(0));
};

module.exports = ZSplitter;
