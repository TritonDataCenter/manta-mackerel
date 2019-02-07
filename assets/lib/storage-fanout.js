#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * The input of this phase is manta moray dumps. Specifically, the objects
 * stored in `manta` bucket. The input constitutes of JSON objects stored
 * as one object per line. The first line contains header information about
 * the structure of the rest of the lines. That is why it receives a special
 * treatment. The purpose of this task is to fanout these objects to multiple
 * reducers. In order to do this without breaking the next phase, we need to
 * preserve two things:
 *  - Each line, which is a js object, needs to go to exactly one
 *    reducer. A line can't be split between two reducers.
 *  - The first line, which contains header information, needs to go
 *    to all the reducers. This is necessary because the next phase
 *    expects this line in order to interpret the rest of its input.
 *
 * We use streams to process buffers in parallel to maximize the throughput.
 *
 *
 *                       .-------------.
 *     +                 |             |
 *     |                 |             |
 *     |                 |   stdin     |
 *     |                 |             |
 *     |                 |   Buffer    |
 *     |                 |   chunks    |
 *     |                 |             |
 *     v                 |             |
 *                       |             |
 *                     /                '\
 *                   /'-------------------'\
 *                  |                       |
 *                  --.-------------------.--
 *                    |    Multi-Line     |
 *                    |      Stream       |
 *                    |                   |
 *                    | Sequence of Lines |
 *                    |      Buffer       |
 *                    |      chunk        |
 *                   /'-------------------'\
 *                  |                       |
 *                  --.-------------------.--
 *                    | Detach-First-Line |
 *                    |      Stream       |
 *                    |                   |
 *                    |     Seperate      |
 *                    |      first        |
 *                    |      line         |
 *                    |      chunk        |
 *                   / -------------------'\
 *     .-----------/                         \--------------.
 *     |                                                    |
 *     |---|------------------------------------------------'
 *     |   |  |   |       |   |
 *     |   |  |   |       |   |
 *     |   |  |   |       |   |
 *     |   |  |   |       |   |
 *     | R |  | R |       | R |  <---- Reducer streams
 *     | e |  | e |       | e |
 *     | d |  | d |       | d |
 *     | u |  | u |       | u |
 *     | c |  | c |       | c |
 *     | e |  | e |       | e |
 *     | r |  | r |       | r |
 *     |   |  |   |       |   |
 *     |   |  |   |       |   |
 *     '---'  '---'      /'---'\
 *                      /       \
 *                     |         |
 *                     `---------`
 *                        | G |
 *                        | Z |  <---- Compress
 *                        | I |         reducer
 *                        | P |          file
 *                       /'---'\
 *                      /       \
 *                     |         |
 *                     `---------`
 *                        | F |
 *                        | i |
 *                        | l |  <---- Store
 *                        | e |       to disk
 *                        -   -
 *                      _.-----._
 *                    .-         -.
 *                    |-_       _-|
 *                    |  ~-----~  |
 *                    |           |
 *                    `._       _.'
 *                       "-----"
 *
 *
 */


/*
 * By default, at least in this version of node (0.10.40), 'events'
 * module prints a warning message when the number of registered
 * event listeners exceeds 10. To silence this warning message, we
 * set the default max to 128 - which - as of now - the maximum number
 * of reducers in a manta job.
 */

require('events').EventEmitter.prototype._maxListeners = 128;



var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_uuidv4 = require('uuid/v4');
var mod_getopt = require('posix-getopt');
var mod_stream = require('stream');
var mod_zlib = require('zlib');

var MUploader = require('./muploader');
var StringDecoder = require('string_decoder').StringDecoder;

var log = new mod_bunyan({
        'name': 'storage-fanout',
        'level': 'warn',
        'stream': process.stderr
});

function fatal(message)
{
        log.fatal(message);
        process.exit(1);
}

function uploadFiles(mu, fileNames, objectNames) {
        mu.uploadReducerFiles(fileNames, objectNames, function (err) {
                if (err) {
                        fatal('Error uploading the files' + err.toString());
                        return;
                }
                //done.
        });
}

function main() {

        var opts = {
            directUpload: false,
            nReducers: 0
        };

        var parser = new mod_getopt.BasicParser('n:u', process.argv);

        var option;
        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'n':
                        opts.nReducers = parseInt(option.optarg, 10);
                        if (isNaN(opts.nReducers) || opts.nReducers < 1) {
                                fatal('Invalid number of reducers ' +
                                    option.optarg);
                        }
                        break;
                case 'u':
                        opts.directUpload = true;
                        break;
                default:
                        /* error message already emitted by getopt */
                        fatal('Invalid option');
                        break;
                }
        }

        if ((opts.directUpload && !opts.nReducers) ||
            (!opts.directUpload && opts.nReducers)) {
                fatal('Setting the number of reducers is required ' +
                    'when choosing direct upload, and vice versa');
        }

        if (opts.directUpload && !process.env['MANTA_OUTPUT_BASE']) {
                fatal('Setting MANTA_OUTPUT_BASE is required ' +
                    'when choosing direct upload');
        }

        // Pipe stdin to stdout when not doing direct upload
        if (!opts.directUpload) {
                process.stdin.pipe(process.stdout);
                return;
        }

        // Multi-line stream - Transforms buffers read from stdin into
        // chunks of strings. We make sure that each string is sequence
        // of complete lines.
        var mls = new mod_stream.Transform({objectMode: true});
        mls.decoder = new StringDecoder();  // Our string decoder.
        mls.savedString = null;             // last incomplete line saved.
        mls._transform = function (chunk, enc, cb) {
                // prepend the saved line to the decoded chunk
                var str = this.savedString === null ? '' : this.savedString;
                str += this.decoder.write(chunk);

                /*
                 * This is not the best way to find the last line. A more
                 * efficient approach is to use lastIndexOf('\n') followed
                 * by slice(). Using lastIndexOf() causes this old version
                 * of node (0.10.40 32bit) to leak memeory.
                 * TODO: Fix this.
                 */
                var lines = str.split('\n');
                this.savedString = lines.pop();

                // Pust the complete lines downstream
                if (lines.length !== 0) {
                        this.push(str.slice(0, str.length -
                            this.savedString.length));
                }
                cb();
        };

        mls._flush = function (cb) {
                // In case we have a savedString
                if (this.savedString !== null) {
                        var lastLine = this.savedString + this.decoder.end();
                        /*
                         * Append '\n' to the last line if it is not properly
                         * terminated. If this is an incomplete line, the next
                         * phase will detect this when it tries to parse the
                         * JSON object.
                         */
                        if (lastLine !== '') {
                                // Push the last incomplete line
                                this.push(lastLine + '\n');
                        }
                }
                cb();
        };

        // We need to give special treatment to the first line because it
        // contains header information. We push it in seperate chunk so that
        // all the reducers recognize it.
        var detachFirstLine = new mod_stream.Transform({objectMode: true});
        detachFirstLine.firstLine = true;
        detachFirstLine._transform = function (str, encoding, done) {
                // Extract the first line and push it seperately.
                if (this.firstLine) {
                        var idx = str.indexOf('\n');
                        this.push(str.slice(0, idx + 1));
                        this.push(str.slice(idx + 1));
                        this.firstLine = false;
                } else {
                        // The rest of the lines pass-through.
                        this.push(str);
                }
                done();
        };

        // pause stdin while we do the plumbing.
        process.stdin.pause();
        process.stdin.pipe(mls)
                .pipe(detachFirstLine);

        var fileNames = [];
        var objectNames = [];
        var objectPrefix = process.env['MANTA_OUTPUT_BASE'] +
            mod_uuidv4() + '.';

        var openFiles = 0;
        var closedFiles = 0;
        var mu = new MUploader(log);

        for (var n = 0; n < opts.nReducers; n++) {

                var rs = new mod_stream.Transform({objectMode: true});
                detachFirstLine.pipe(rs);
                // Chunk counter is set to -1 initially so that all reducers
                // recognize and push the first line downstream.
                rs.counter = -1;
                rs.index = n;
                rs.nReducers = opts.nReducers;
                rs._transform = function (str, encoding, done) {
                        // If this the first line or a chunk that needs to be
                        // processed by this reducer, then push downstream.
                        if (this.counter == -1 ||
                            this.counter % this.nReducers == this.index) {
                                this.push(str);
                        }
                        // Advance the chunk counter in all cases.
                        this.counter++;
                        done();
                };

                // To compress reducer's data.
                var zs = mod_zlib.createGzip();
                rs.pipe(zs);

                // Reducer's object path in manta.
                objectNames.push(objectPrefix + n);
                // Reducer's temporary file path.
                fileNames.push('/var/tmp/part' + n);
                var ws = mod_fs.createWriteStream(fileNames[n]);
                zs.pipe(ws);

                ws.on('open', function () {
                        // Resume stdin when all the underlying reducers'
                        // files are ready to take input.
                        if (++openFiles == opts.nReducers) {
                                process.stdin.resume();
                        }
                });
                ws.on('close', function () {
                        // Upload the reducers' files to manta when all the data
                        // has been flushed to disk.
                        if (++closedFiles == opts.nReducers) {
                                uploadFiles(mu, fileNames, objectNames);
                        }
                });
        }
}

// Execution starts here
main();
