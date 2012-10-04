// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var carrier = require('carrier');
var EventEmitter = require('events').EventEmitter;
var mola = require('mola/lib');

///--- Globals

var BLOCK_SIZE = 4096;
var KB_PER_BLOCK = BLOCK_SIZE / 1024;



///--- Internal helpers

// Process a manta object's json record.
//
// aggregation: the map used for aggregation
// log: logger
// record: a record to be aggregated
function mapRawRecord(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.log, 'options.log');
        assert.object(opts.record, 'options.record');

        var aggr = opts.aggregation;
        var log = opts.log;
        var obj = opts.record;
        var isDirectory = obj.type === 'directory';

        // Skip if record is a directory
        if (isDirectory) {
                return (aggr);
        }

        // The createdFrom property distinguished links from objects
        var isLink = obj.createdFrom;
        var owner = obj.owner;
        var size = Math.ceil(obj.contentLength / BLOCK_SIZE) * KB_PER_BLOCK;
        var copies = obj.sharks.length;

        log.debug({
                aggregation: aggr,
                record: obj
        }, 'mapRawRecord: entered');

        // Create a record for a customer if we haven't seen it
        aggr[owner] = aggr[owner] || {
                owner: owner,
                numKb: 0,
                numKeys: 0
        };

        aggr[owner].numKb += isLink ? 0 : (size * copies);
        aggr[owner].numKeys++;

        log.debug({
                aggregation: aggr
        }, 'mapRawRecord: done');

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
        var owner = obj.owner;

        log.debug({
                aggregation: aggr,
                record: obj
        }, 'reduceShardRecords: entered');

        // Create a record for a customer if we haven't seen it
        aggr[owner] = aggr[owner] || {
                owner: owner,
                numKb: 0,
                numKeys: 0
        };

        aggr[owner].numKb += obj.numKb;
        aggr[owner].numKeys += obj.numKeys;

        log.debug({
                aggregation: aggr
        }, 'reduceShardRecords: done');

        return (aggr);
}


// Reads streaming JSON
//
// aggregationFunction: the function that contains the logic on how to
//      aggregate each record, viz. mapRawRecord, reduceShardRecords
// emmitter: event emitter
// inputStream: a readable stream, e.g. stdin
// log: logger
function readStreamingJSON(opts) {
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
                        log.debug(e, 'invalid line');
                        emitter.emit('error', e);
                }
        });

        carry.on('error', emitter.emit.bind(emitter, 'error'));

        carry.once('end', function returnResults() {
                emitter.emit('end', mapToArray({
                        aggregation: aggr,
                        emitter: emitter,
                        log: log
                }));
        });
}


// Reads a postgres dump in JSON format
//
// aggregationFunction: the function that contains the logic on how to
//      aggregate each record, viz. mapRawRecord, reduceShardRecords
// emmitter: event emitter
// inputStream: a readable stream, e.g. stdin
// log: logger
function readpgDump(opts) {
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
        var schemaReader = mola.createSchemaReader(input);

        schemaReader.on('object', function onObject(obj) {
                if (obj._value) {
                        aggFunction({
                                aggregation: aggr,
                                log: log,
                                record: obj._value
                        });
                } else {
                        log.debug(obj, 'unrecognized schema');
                        emitter.emit('error');
                }
        });

        schemaReader.on('error', emitter.emit.bind(emitter, 'error'));
        schemaReader.on('end', function returnResults() {
                emitter.emit('end', mapToArray({
                        aggregation: aggr,
                        log: log
                }));
        });
}


// Reorganizes a map into an array for easy streaming.
//
// aggregation: the map to reorganize into an array
// log: logger
function mapToArray(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.log, 'options.log');

        var agg = opts.aggregation;
        var log = opts.log;

        var list = [];

        log.debug('mapToArray: entered');

        Object.keys(agg).forEach(function (key) {
                list.push(agg[key]);
        });

        log.debug('mapToArray: done');
        return (list);
}



///--- API

function createMapReader(opts) {
        assert.object(opts, 'options');
        assert.object(opts.input, 'options.input');
        assert.object(opts.log, 'options.log');

        var input = opts.input;
        var log = opts.log;

        var emitter = new EventEmitter();

        readpgDump({
                inputStream: input,
                log: log,
                aggregationFunction: mapRawRecord,
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

        readStreamingJSON({
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
