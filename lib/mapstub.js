var mackerel = require('../lib');
var bunyan = require('bunyan');
var log = bunyan.createLogger({
        name: 'mapstub.js'
});

var stdin = process.openStdin();
var agg = {};
function cb() {
        mackerel.format({
                aggregation: agg,
                log: log,
                outputStream: process.stdout
        });
}

mackerel.aggregate({
        stream: stdin,
        aggregation: agg,
        aggregationFunction: mackerel.mapFunction,
        log: log,
        callback: cb
});
