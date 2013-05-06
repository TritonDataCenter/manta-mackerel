#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

/*
 *    {
 *        "owner": "59159a6e-51b5-4e27-bca4-6cd9c8626eb2",
 *        "time": {
 *            "268435456": [
 *                1316,
 *                463384821
 *            ],
 *            "536870912": [
 *                60,
 *                110549076
 *            ],
 *            "2147483648": [
 *                6,
 *                237815959
 *            ]
 *        },
 *        "bandwidth": {
 *            "in": 223248,
 *            "out": 17610
 *        }
 *    }
 */

var mod_marlin = require('marlin/lib/meter.js');

function hrtimePlusEquals(oldvalue, newvalue) {
        oldvalue[0] += newvalue[0];
        oldvalue[1] += newvalue[1];
        if (oldvalue[1] > 1e9) {
                oldvalue[0]++;
                oldvalue[1] -= 1e9;
        }
        return (oldvalue);
}

function main() {
        var aggr = {};
        var reader = new mod_marlin.MarlinMeterReader({
                summaryType: 'deltas',
                aggrKey: [ 'owner', 'taskid'],
                resources: [
                        'time',
                        'vnic0.rbytes64',
                        'vnic0.obytes64',
                        'memory.physcap'
                ],
                stream: process.stdin
        });

        reader.once('end', function onEnd() {
                var report = reader.reportFlattened();
                for (var i = 0; i < report.length; i++) {
                        var owner = report[i][0];
                        var res = report[i][2];

                        aggr[owner] = aggr[owner] || {
                                owner: owner,
                                time: {},
                                bandwidth: {
                                        in: 0,
                                        out: 0
                                }
                        };

                        var stats = aggr[owner];

                        stats.time[res['memory.physcap']] =
                                stats.time[res['memory.physcap']] || [0, 0];

                        hrtimePlusEquals(
                                stats.time[res['memory.physcap']],
                                res.time);

                        stats.bandwidth.in += res['vnic0.rbytes64'];
                        stats.bandwidth.out += res['vnic0.obytes64'];
                }
                Object.keys(aggr).forEach(function (k) {
                        console.log(JSON.stringify(aggr[k]));
                });
        });
}


if (require.main === module) {
        main();
}
