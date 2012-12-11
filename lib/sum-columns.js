#!/usr/bin/env node
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var util = require('util');
var events = require('events');

function Carrier(reader, listener, encoding, separator) {
        var self = this;

        self.reader = reader;

        if (!separator) {
                separator = /\r?\n/;
        }

        if (listener) {
                self.addListener('line', listener);
        }

        var buffer = '';

        reader.setEncoding(encoding || 'utf8');
        reader.on('data', function (data) {
                var lines = (buffer + data).split(separator);
                buffer = lines.pop();

                lines.forEach(function (line, index) {
                        self.emit('line', line);
                });
        });

        var ender = function () {
                if (buffer.length > 0) {
                        self.emit('line', buffer);
                        buffer = '';
                }
                self.emit('end');
        };
        reader.on('end', ender);

}

util.inherits(Carrier, events.EventEmitter);

Carrier.carry = function (reader, listener, encoding, separator) {
        return (new Carrier(reader, listener, encoding, separator));
};

function isNum(n) {
        return (!isNaN(parseFloat(n)) && isFinite(n));
}

function parseLine(line) {
        return (JSON.parse(line));
}

function getAggKey(obj) {
        var key = '';
        Object.keys(obj).forEach(function (k) {
                if (!isNum(obj[k])) {
                        key += obj[k];
                }
        });
        return (key);
}
function onLine(aggr, line) {
        var parsed = parseLine(line);

        var aggKey = getAggKey(parsed);

        if (!aggr[aggKey]) {
                aggr[aggKey] = {};
                Object.keys(parsed).forEach(function (k) {
                        if (!isNum(parsed[k])) {
                                aggr[aggKey][k] = parsed[k];
                                return;
                        }
                        aggr[aggKey][k] = 0;
                });
        }
        Object.keys(parsed).forEach(function (k) {
                if (!isNum(parsed[k])) {
                        return;
                }
                aggr[aggKey][k] += parsed[k];
        });
}

function onEnd(aggr) {
        Object.keys(aggr).forEach(function (k) {
                console.log(JSON.stringify(aggr[k]));
        });
}

function main() {
        var carry = Carrier.carry(process.openStdin());

        var aggr = {};

        carry.on('line', onLine.bind(null, aggr));
        carry.once('end', onEnd.bind(null, aggr));
}

if (require.main === module) {
        main();
}
