var mackerel = require('../lib');
var bunyan = require('bunyan');
var log = bunyan.createLogger({
        name: 'reducestub.js'
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
        aggregationFunction: mackerel.reduceFunction,
        log: log,
        callback: cb
});
