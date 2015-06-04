#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */


var Big = require('big.js');
var Transform = require('stream').Transform;
var lstream = require('lstream');
var util = require('util');

function plusEquals(aggrPhase, phase) {
    aggrPhase.seconds += phase.seconds;
    aggrPhase.ntasks += phase.ntasks;
    aggrPhase.bandwidth.in = aggrPhase.bandwidth.in.plus(phase.bandwidth.in);
    aggrPhase.bandwidth.out = aggrPhase.bandwidth.out.plus(phase.bandwidth.out);
}

function stringify(key, value) {
    if (value instanceof Big || typeof (value) === 'number') {
        return (value.toString());
    }
    return (value);
}

function parse(key, value) {
    if (key === '') {
        return (value);
    }
    if (typeof (value) === 'string') {
        try {
            return (new Big(value));
        } catch (e) {
            return (value);
        }
    }
    return (value);
}

function ComputeReduceStream(opts) {
    this.log = opts.log;
    this.lineNumber = 0;
    this.aggr = {};
    Transform.call(this, opts);
}
util.inherits(ComputeReduceStream, Transform);

ComputeReduceStream.prototype._transform = function _transform(line, enc, cb) {
    var self = this;
    this.lineNumber++;

    var record;
    try {
        record = JSON.parse(line, parse);
    } catch (e) {
        this.log.error({
            error: e.message,
            line: line,
            lineNumber: this.lineNumber
        }, 'error parsing line');
        cb(e);
        return;
    }

    var owner = record.owner;
    var jobs = record.jobs;
    this.aggr[owner] = this.aggr[owner] || {
        owner: owner,
        jobs: {}
    };

    Object.keys(jobs).forEach(function (j) {
        self.aggr[owner].jobs[j] = self.aggr[owner].jobs[j] || {};
        Object.keys(jobs[j]).forEach(function (p) {
            var phases = jobs[j];
            var aggrPhases = self.aggr[owner].jobs[j];
            if (typeof (aggrPhases[p]) === 'undefined') {
                aggrPhases[p] = phases[p];
                return;
            }
            plusEquals(aggrPhases[p], phases[p]);
        });
    });
};

ComputeReduceStream.prototype._flush = function _flush(cb) {
    var self = this;
    Object.keys(this.aggr).forEach(function (owner) {
        self.push(JSON.stringify(self.aggr[owner], stringify) + '\n');
    });
    cb();
};

function main() {
    var log = require('bunyan').createLogger({
        name: 'compute-reduce.js',
        stream: process.stderr,
        level: process.env.LOG_LEVEL || 'info'
    });

    var reduceStream = new ComputeReduceStream({
        log: log
    });

    reduceStream.once('error', function (error) {
        log.error({error: error}, 'compute map error');
        process.abort();
    });

    process.stdin.pipe(new lstream()).pipe(reduceStream).pipe(process.stdout);
}

if (require.main === module) {
    main();
}

module.exports = ComputeReduceStream;
