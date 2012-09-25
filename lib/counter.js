// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var carrier = require('carrier');



///--- Globals

var BLOCK_SIZE = 4096;
var KB_PER_BLOCK = BLOCK_SIZE / 1024;



///--- API

// Process a manta object's json record.
//
// aggregation: the map used for aggregation
// log: logger
// record: a record to be aggregated
function mapFunction(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.log, 'options.log');
        assert.object(opts.record, 'options.record');

        var aggr = opts.aggregation;
        var log = opts.log;
        var obj = opts.record;
        var owner = obj.owner;
        // The createdFrom property distinguished links from objects
        var isLink = obj.createdFrom;
        var size = Math.ceil(obj.contentLength / BLOCK_SIZE) * KB_PER_BLOCK;
        var copies = obj.sharks.length;

        log.debug({
                aggregation: aggr,
                record: obj
        }, 'mapFunction: entered');

        aggr[obj.owner] = aggr[owner] || {
                numKb: 0,
                numKeys: 0
        };

        aggr[owner].numKb += isLink ? 0 : (size * copies);
        aggr[owner].numKeys++;

        log.debug({
                aggregation: aggr
        }, 'mapFunction: done');

        return (aggr);
}


// Aggregate a mapper's output.
//
// aggregation: the map used for aggregation
// log: logger
// record: a record from a map phase
function reduceFunction(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.log, 'options.log');
        assert.object(opts.record, 'options.record');

        var aggr = opts.aggregation;
        var log = opts.log;
        var obj = opts.record;
        var owner = obj.owner;

        log.debug({
                aggregation: aggr,
                record: obj
        }, 'reduceFunction: entered');

        aggr[obj.owner] = aggr[owner] || {
                numKb: 0,
                numKeys: 0
        };

        aggr[owner].numKb += obj.numKb;
        aggr[owner].numKeys += obj.numKeys;

        log.debug({
                aggregation: aggr
        }, 'reduceFunction: done');

        return (aggr);
}


// Takes a javascript object and reformats it into a streaming json format
//
// aggregation: the map to format into streaming json
// log: logger
// outputStream: a writable stream to output to, e.g. stdout
function format(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.log, 'options.log');
        assert.object(opts.outputStream, 'options.outputStream');

        var agg = opts.aggregation;
        var log = opts.log;
        var stream = opts.outputStream;

        var template = {
                owner: undefined,
                numKb: undefined,
                numKeys: undefined
        };

        log.debug('format: entered');

        Object.keys(agg).forEach(function (key) {
                template.owner = key;
                template.numKb = agg[key].numKb;
                template.numKeys = agg[key].numKeys;
                stream.write(JSON.stringify(template) + '\n');
        });

        log.debug('format: done');
}


// Aggregates records
//
// stream: a readable stream, e.g. stdin
// aggregation: an empty object that will be used for aggregation
// log: logger
// aggregationFunction: the function that contains the logic on how to
//      aggregate each record, viz. mapFunction, reduceFunction
// callback: callback function to call once aggregation is done
function aggregate(opts) {
        assert.object(opts, 'options');
        assert.object(opts.stream, 'options.stream');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.object(opts.log, 'options.log');
        assert.func(opts.aggregationFunction, 'options.aggregationFunction');
        assert.func(opts.callback, 'options.callback');

        var aggr = opts.aggregation;
        var stream = opts.stream;
        var log = opts.log;
        var aggFunction = opts.aggregationFunction;
        var callback = opts.callback;

        var carry = carrier.carry(stream);
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
                }
        });

        carry.once('end', function returnResults() {
                callback();
        });
}



///--- Exports

module.exports = {

        mapFunction: mapFunction,
        reduceFunction: reduceFunction,
        format: format,
        aggregate: aggregate

};
