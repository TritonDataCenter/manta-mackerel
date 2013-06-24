#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.
var computeTable = require('../../assets/etc/billingComputeTable.json').billingTable;

function summarizeCompute(record) {
        var jobs = record.jobs;
        var gbSeconds = 0;
        var bwin = 0;
        var bwout = 0;
        var phases = 0;
        var tasks = 0;
        var rawTime = 0;

        function billingLookup(usage) {
                var i;
                for (i = 0; i < computeTable.length; i++) {
                        if (usage['disk'] > computeTable[i]['disk'] ||
                                usage['memory'] > computeTable[i]['memory']) {
                                continue;
                        } else {
                                break;
                        }
                }
                return (computeTable[i].memory);
        }

        Object.keys(jobs).forEach(function (job) {
                var memoryGB;
                var seconds;
                Object.keys(jobs[job]).forEach(function (p) {
                        seconds = jobs[job][p]['seconds'];
                        memoryGB = billingLookup(jobs[job][p]) / 1024;
                        phases++;
                        tasks += Object.keys(jobs[job][p]).length;
                        rawTime += seconds;
                        bwin += jobs[job][p]['bandwidth']['in'];
                        bwout += jobs[job][p]['bandwidth']['out'];
                        gbSeconds += (seconds * memoryGB);
                });
        });

        var output = [
                record.owner,
                gbSeconds,
                rawTime,
                Object.keys(jobs).length,
                phases,
                tasks,
                bwin,
                bwout
        ].join(' ');

        console.log(output);
}

var carry = require('carrier').carry(process.stdin);
carry.on('line', function (line) {
        summarizeCompute(JSON.parse(line, function (key, value) {
                if (key === '') {
                        return (value);
                }
                if (typeof(value) === 'string') {
                        if (!isNaN(+value)) {
                                return (+value);
                        } else {
                                return (value);
                        }
                }
                return (value);
        }));
});
process.stdin.resume();
