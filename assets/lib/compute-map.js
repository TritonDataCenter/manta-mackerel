#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_marlin = require('marlin');
var Big = require('big.js');
var lookupPath = process.env['LOOKUP_FILE'] || '../etc/lookup.json';
var lookup = require(lookupPath); // maps uuid->approved_for_provisioning
var COUNT_UNAPPROVED_USERS = process.env['COUNT_UNAPPROVED_USERS'] === 'true';
var MALFORMED_LIMIT = process.env['MALFORMED_LIMIT'] || '0';
var ERROR = false;

var LOG = require('bunyan').createLogger({
        name: 'compute-map.js',
        stream: process.stderr,
        level: process.env['LOG_LEVEL'] || 'info'
});

function hrtimePlusEquals(oldvalue, newvalue) {
        oldvalue[0] += newvalue[0];
        oldvalue[1] += newvalue[1];
        if (oldvalue[1] > 1e9) {
                oldvalue[0]++;
                oldvalue[1] -= 1e9;
        }
        return (oldvalue);
}

/*
 * round the [seconds, nanoseconds] pair to the nearest second
 */
function roundhrtime(hrtime) {
        return (hrtime[1] >= 5e8 ? hrtime[0] + 1 : hrtime[0]);
}

/*
 * JSON.stringify filter function to convert bandwidth numbers to strings
 */
function stringify(key, value) {
        if (key === 'bandwidth') {
                value.in = value.in.toString();
                value.out = value.out.toString();
        }
        if (key === 'seconds') {
                value = Math.max(roundhrtime(value), 1);
        }
        return (value);
}

function main() {
        var aggr = {};
        var reader = new mod_marlin.MarlinMeterReader({
                summaryType: 'deltas',
                aggrKey: [ 'owner', 'jobid', 'taskid', 'phase'],
                resources: [
                        'time',
                        'vnic0.rbytes64',
                        'vnic0.obytes64',
                        'memory.physcap',
                        'config_disk'
                ],
                stream: process.stdin
        });

        reader.on('warn', function (err) {
                LOG.warn(err, 'marlin meter reader');
        });

        reader.once('end', function onEnd() {
                LOG.info('reader end');
                var len = MALFORMED_LIMIT.length;
                var stats = reader.stats();
                var malformed = stats['malformed records'];

                var threshold;

                if (MALFORMED_LIMIT[len - 1] === '%') {
                        var pct = +(MALFORMED_LIMIT.substr(0, len-1));
                        threshold = pct * stats['log records'];
                } else {
                        threshold = +MALFORMED_LIMIT;
                }

                if (isNaN(threshold)) {
                        LOG.error('MALFORMED_LIMIT not a number');
                        ERROR = true;
                        return;
                }

                if (malformed > threshold) {
                        LOG.fatal('Too many malformed lines');
                        ERROR = true;
                        return;
                }

                var report = reader.reportFlattened();
                for (var i = 0; i < report.length; i++) {
                        var owner = report[i][0];
                        var jobid = report[i][1];

                        var phase = report[i][3];
                        var res = report[i][4];

                        var seconds = res['time'];
                        var bwin = res['vnic0.rbytes64'];
                        var bwout = res['vnic0.obytes64'];
                        var memory = res['memory.physcap'] / 1048576; // to MiB
                        var disk = res['config_disk'];

                        if (!COUNT_UNAPPROVED_USERS) {
                                if (!lookup[owner]) {
                                        LOG.error('No login found for UUID ' +
                                                owner);
                                        ERROR = true;
                                        continue;
                                }

                                if (!lookup[owner].approved) {
                                        LOG.warn(owner + ' not approved for ' +
                                                'provisioning. Skipping...');
                                        continue;
                                }
                        }

                        aggr[owner] = aggr[owner] || {
                                owner: owner,
                                jobs: {}
                        };

                        aggr[owner].jobs[jobid] = aggr[owner].jobs[jobid] || {};
                        var phases = aggr[owner].jobs[jobid];
                        phases[phase] = phases[phase] || {
                                memory: memory,
                                disk: disk,
                                seconds: [0, 0],
                                ntasks: 0,
                                bandwidth: {
                                        in: new Big(0),
                                        out: new Big(0)
                                }
                        };

                        hrtimePlusEquals(phases[phase]['seconds'], seconds);
                        phases[phase]['ntasks']++;
                        phases[phase].bandwidth['in'] =
                                phases[phase].bandwidth['in'].plus(bwin);
                        phases[phase].bandwidth['out'] =
                                phases[phase].bandwidth['out'].plus(bwout);
                }

                Object.keys(aggr).forEach(function (o) {
                        console.log(JSON.stringify(aggr[o], stringify));
                });

                if (ERROR) {
                        process.exit(1);
                }
        });
}


if (require.main === module) {
        main();
}
