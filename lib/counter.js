// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var carrier = require('carrier');



///--- Globals

var BLOCK_SIZE = 4096;
var KB_PER_BLOCK = BLOCK_SIZE / 1024;


///--- Helpers


///--- API

// Process a manta object's json description
// The createdFrom property distinguishes links from objects.
function processRecord(opts) {
        assert.object(opts, 'options');
        assert.object(opts.aggregation, 'options.aggregation');
        assert.string(opts.line, 'options.line');
        assert.object(opts.log, 'options.log');
        assert.object(opts.record, 'options.record');

        var aggr = opts.aggregation;
        var log = opts.log;
        var obj = opts.record;
        var owner = obj.owner;
        var isLink = obj.createdFrom;
        var size = Math.ceil(obj.contentLength / BLOCK_SIZE) * KB_PER_BLOCK;
        var copies = obj.sharks.length;

        log.debug({
                aggregation: aggr,
                record: obj
        }, 'processRecord: entered');

        aggr[obj.owner] = aggr[owner] || {
                numKb: 0,
                numKeys: 0
        };

        aggr[owner].numKb += isLink ? 0 : (size * copies);
        aggr[owner].numKeys++;

        log.debug({
                aggregation: aggr
        }, 'processRecord: done');

        return (aggr);
}



///--- Exports

module.exports = {

        processRecord: processRecord

};



///--- Tests

// function main(instream, outstream) {
//         var my_carrier = carrier.carry(instream);
//         my_carrier.on('line', processLine);
//         // Print each record
//         my_carrier.on('end', function printResults() {
//                 for (var customer in customers) {
//                         outstream.write(
//                                 JSON.stringify(customers[customer]) + '\n');
//                 }
//                 outstream.end();
//         });
// }


// main(stdin, stdout);
