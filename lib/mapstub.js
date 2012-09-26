var mackerel = require('../lib');
var bunyan = require('bunyan');
var log = bunyan.createLogger({
        name: 'mapstub.js'
});

var stdin = process.openStdin();
var mapper = mackerel.createMapReader({
        input: process.openStdin(),
        log: log
});

mapper.on('error', function (err) {
        log.fatal(err, 'er');
});

mapper.on('end', function (aggr) {
        Object.keys(aggr || {}).forEach(function (k) {
                process.stdout.write(JSON.stringify(aggr[k]) + '\n');
        });
});
