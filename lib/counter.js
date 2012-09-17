#!/usr/bin/env node
// Copyright 2012 Joyent, Inc.  All rights reserved.
var carrier = require('carrier');

var BLOCK_SIZE = 4096;
var stdin = process.openStdin();
var stdout = process.stdout;
var customers = {};
// Process a manta object's json description
// The createdFrom property distinguishes links from objects.
function processLine(line) {
        var object = JSON.parse(line);
        var isLink = object.createdFrom;
        var objectSizeKB = Math.ceil(object.contentLength *
                                     object.sharks.length / BLOCK_SIZE) * 4;
        if (customers[object.owner]) {
                // existing customer
                customers[object.owner].numKB += isLink ? 0 : objectSizeKB;
                customers[object.owner].numKeys++;
        } else {
                // new customer
                customers[object.owner] = {
                        customer: object.owner,
                        numKB: isLink ? 0 : objectSizeKB,
                        numKeys: 1
                };
        }
}
function main(instream, outstream) {
        var my_carrier = carrier.carry(instream);
        my_carrier.on('line', processLine);
        // Print each record
        my_carrier.on('end', function printResults() {
                for (var customer in customers) {
                        outstream.write(
                                JSON.stringify(customers[customer]) + '\n');
                }
                outstream.end();
        });
}

main(stdin, stdout);

module.exports = {
        processLine: processLine,
        main: main
}
