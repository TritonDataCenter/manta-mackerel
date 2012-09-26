// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var carrier = require('carrier');
var EventEmitter = require('events').EventEmitter;


///--- Globals

var BLOCK_SIZE = 4096;
var KB_PER_BLOCK = BLOCK_SIZE / 1024;



///--- Internal helpers

// Process a manta object's json record.
//
// aggregation: the map used for aggregation
// log: logger
// record: a record to be aggregated
function preAggregate(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.log, 'options.log');
        assert.object(opts.record, 'options.record');

        var aggr = opts.aggregation;
        var log = opts.log;
        var obj = opts.record;

        // The createdFrom property distinguished links from objects
        var isLink = obj.createdFrom;
        var owner = obj.owner;
        var size = Math.ceil(obj.contentLength / BLOCK_SIZE) * KB_PER_BLOCK;
        var copies = obj.sharks.length;

        log.debug({
                aggregation: aggr,
                record: obj
        }, 'preAggregate: entered');

        aggr[owner] = aggr[owner] || {
                numKb: 0,
                numKeys: 0
        };

        aggr[owner].numKb += isLink ? 0 : (size * copies);
        aggr[owner].numKeys++;

        log.debug({
                aggregation: aggr
        }, 'preAggregate: done');

        return (aggr);
}


// Aggregate a mapper's output.
//
// aggregation: the map used for aggregation
// log: logger
// record: a record from a map phase
function reduceShardRecords(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.log, 'options.log');
        assert.object(opts.record, 'options.record');

        var log = opts.log;
        var aggr = opts.aggregation;
        var obj = opts.record;

        log.debug({
                aggregation: aggr,
                record: obj
        }, 'reduceShardRecords: entered');

        aggr[obj.owner] = aggr[obj.owner] || {
                numKb: 0,
                numKeys: 0
        };

        aggr[obj.owner].numKb += obj.numKb;
        aggr[obj.owner].numKeys += obj.numKeys;

        log.debug({
                aggregation: aggr
        }, 'reduceShardRecords: done');

        return (aggr);
}


// Aggregates records
//
// aggregationFunction: the function that contains the logic on how to
//      aggregate each record, viz. preAggregate, reduceShardRecords
// emmitter: event emitter
// inputStream: a readable stream, e.g. stdin
// log: logger
function aggregate(opts) {
        assert.object(opts, 'options');
        assert.func(opts.aggregationFunction, 'options.aggregationFunction');
        assert.object(opts.emitter, 'options.emitter');
        assert.object(opts.inputStream, 'options.inputStream');
        assert.object(opts.log, 'options.log');

        var aggFunction = opts.aggregationFunction;
        var emitter = opts.emitter;
        var input = opts.inputStream;
        var log = opts.log;

        var aggr = {};
        var carry = carrier.carry(input);

        carry.on('line', function onLine(line) {
                var record;

                try {
                        record = JSON.parse(line);
                        aggFunction({
                                aggregation: aggr,
                                log: log,
                                record: record
                        });

                } catch (e) {
                        log.fatal(e, 'invalid line');
                        emitter.emit('error', 'invalid line');
                }
        });

        carry.once('end', function returnResults() {
                finish({
                        aggregation: aggr,
                        emitter: emitter,
                        log: log
                });
        });
}


// Reorganizes a map into an array in for easy streaming.
//
// aggregation: the map to reorganize into an array
// log: logger
function finish(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.emitter, 'options.emitter');
        assert.object(opts.log, 'options.log');

        var agg = opts.aggregation;
        var emitter = opts.emitter;
        var log = opts.log;

        var list = [];

        log.debug('format: entered');

        Object.keys(agg).forEach(function (key) {
                list.push({
                        owner: key,
                        numKb: agg[key].numKb,
                        numKeys: agg[key].numKeys
                });
        });

        log.debug('format: done');
        emitter.emit('end', list);
}



///--- API

function createMapReader(opts) {
        assert.object(opts, 'options');
        assert.object(opts.input, 'options.input');
        assert.object(opts.log, 'options.log');

        var input = opts.input;
        var log = opts.log;

        var emitter = new EventEmitter();

        aggregate({
                inputStream: input,
                log: log,
                aggregationFunction: preAggregate,
                emitter: emitter
        });

        return (emitter);
}


function createReduceReader(opts) {
        assert.object(opts, 'options');
        assert.object(opts.input, 'options.input');
        assert.object(opts.log, 'options.log');

        var input = opts.input;
        var log = opts.log;

        var emitter = new EventEmitter();

        aggregate({
                inputStream: input,
                log: log,
                aggregationFunction: reduceShardRecords,
                emitter: emitter
        });

        return (emitter);
}



///--- Exports

module.exports = {

        createMapReader: createMapReader,
        createReduceReader: createReduceReader

};
