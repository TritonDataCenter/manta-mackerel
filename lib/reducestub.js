var mackerel = require('../lib');
var bunyan = require('bunyan');
var log = bunyan.createLogger({
        name: 'reducestub.js'
});

var stdin = process.openStdin();
var reducer = mackerel.createReduceReader({
        input: process.openStdin(),
        log: log
});

reducer.on('error', function (err) {
        log.fatal(err, 'er');
});

reducer.on('end', function (aggr) {
        Object.keys(aggr || {}).forEach(function (k) {
                process.stdout.write(JSON.stringify(aggr[k]) + '\n');
        });
});
