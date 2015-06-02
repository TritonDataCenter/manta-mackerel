#!/usr/node/bin/node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var Big = require('big.js');
var marlin = require('marlin');

var LOOKUP_PATH = process.env.LOOKUP_FILE || '../etc/lookup.json';
var EXCLUDE_UNAPPROVED_USERS = process.env.EXCLUDE_UNAPPROVED_USERS === 'true';
if (EXCLUDE_UNAPPROVED_USERS) {
    var LOOKUP = require(LOOKUP_PATH);
}
var MALFORMED_LIMIT = process.env.MALFORMED_LIMIT || '0';

var LOG = require('bunyan').createLogger({
    name: 'compute-map.js',
    stream: process.stderr,
    level: process.env.LOG_LEVEL || 'info'
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

function aggregate(aggr, entry) {
    var owner = entry[0];
    var jobid = entry[1];
    var phase = entry[3];
    var resources = entry[4];

    if (EXCLUDE_UNAPPROVED_USERS) {
        if (!LOOKUP[owner]) {
            this.log.warn('No login found for %s', owner);
        }

        if (!LOOKUP[owner].approved) {
            this.log.warn('%s not approved for provisioning. Skipping...',
                owner);
            return (aggr);
        }
    }

    var seconds = resources.time;
    var bwin = resources['vnic0.rbytes64'];
    var bwout = resources['vnic0.obytes64'];
    var memory = resources['memory.physcap'] / 1048576; // to MiB
    var disk = resources.config_disk;

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

    hrtimePlusEquals(phases[phase].seconds, seconds);
    phases[phase].ntasks++;
    phases[phase].bandwidth.in = phases[phase].bandwidth.in.plus(bwin);
    phases[phase].bandwidth.out = phases[phase].bandwidth.out.plus(bwout);
    return (aggr);
}


function read(opts) {
    var input = opts.input;
    var output = opts.output;
    var reader = new marlin.MarlinMeterReader({
        summaryType: 'deltas',
        aggrKey: [ 'owner', 'jobid', 'taskid', 'phase'],
        resources: [
            'time',
            'vnic0.rbytes64',
            'vnic0.obytes64',
            'memory.physcap',
            'config_disk'
        ],
        stream: input
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
            threshold = parseInt(MALFORMED_LIMIT, 10);
        }

        if (!isNaN(threshold) && malformed > threshold) {
            LOG.fatal('Too many malformed lines');
            process.exit(1);
            return;
        }

        var report = reader.reportFlattened();
        var aggr = report.reduce(aggregate, {});
        Object.keys(aggr).forEach(function (o) {
            var line = JSON.stringify(aggr[o], stringify) + '\n';
            output.write(line);
        });
    });

}

function main() {
    read({
        input: process.stdin,
        output: process.stdout
    });
}


if (require.main === module) {
    main();
}
