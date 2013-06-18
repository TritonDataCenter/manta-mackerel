#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var Big = require('big.js');
var mod_carrier = require('carrier');
var ERROR = false;

function plusEquals(aggrPhase, phase) {
        aggrPhase['seconds'] += phase['seconds'];
        aggrPhase['ntasks'] += phase['ntasks'];
        aggrPhase['bandwidth']['in'] =
                aggrPhase['bandwidth']['in'].plus(phase['bandwidth']['in']);
        aggrPhase['bandwidth']['out'] =
                aggrPhase['bandwidth']['out'].plus(phase['bandwidth']['out']);
}

function stringify(key, value) {
        if (value instanceof Big) {
                return (value.toString());
        }
        return (value);
}

function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var lineCount = 0;
        var aggr = {};

        carry.on('line', function onLine(line) {
                lineCount++;

                try {
                        var record = JSON.parse(line, function (key, value) {
                                if (key === '') {
                                        return (value);
                                }
                                if (typeof (value) === 'string') {
                                        try {
                                                return (new Big(value));
                                        } catch (e) {
                                                return (value);
                                        }
                                }
                                return (value);
                        });
                } catch (e) {
                        console.warn('Error on line ' + lineCount + ': ' + e);
                        ERROR = true;
                        return;
                }

                var owner = record.owner;
                var jobs = record.jobs;
                aggr[owner] = aggr[owner] || {
                        owner: owner,
                        jobs: {}
                };

                Object.keys(jobs).forEach(function (j) {
                        aggr[owner].jobs[j] = aggr[owner].jobs[j] || {};
                        Object.keys(jobs[j]).forEach(function (p) {
                                var phases = jobs[j];
                                var aggrPhases = aggr[owner].jobs[j];
                                if (typeof (aggrPhases[p]) === 'undefined') {
                                        aggrPhases[p] = phases[p];
                                        return;
                                }
                                plusEquals(aggrPhases[p], phases[p]);
                        });
                });
        });
        carry.once('end', function () {
                Object.keys(aggr).forEach(function (o) {
                        console.log(JSON.stringify(aggr[o], stringify));
                });
        });
}

if (require.main === module) {
        process.on('exit', function onExit() {
                process.exit(ERROR);
        });

        main();
}
