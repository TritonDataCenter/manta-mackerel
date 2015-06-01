// TODO environment variable overrides in addEnv based on environment passed to chronos to allow for e.g. backfill, dryrun

var fmt = require('../deps/chronos/lib/common.js');
var sprintf = require('util').format;

// prepend environment variables
function addEnv(phases, env, datestamp, extra) {
    var envString = '';
    Object.keys(env).forEach(function (k) {
        var formatted = fmt(env[k], datestamp, extra);
        envString += sprintf('%s="%s" ', k, formatted);
    });

    for (var i = 0; i < phases.length; i++) {
        var phase = phases[i];
        var next = phases[i+1];
        if (next && next.type === 'reduce') {
            var nextReducers = sprintf('NUM_REDUCERS=%s ', next.count);
            phase.init = envString + nextReducers + phase.init;
        } else {
            phase.init = envString + phase.init;
        }
    }

    return (phases);
}

function _gen_storage_phases(job, datestamp) {
    var user = job.user;
    var root = sprintf('/%s/stor/usage', user);
    var extraFmt = {
        root: root
    };

    var phases = [
        {
            type: 'map',
            memory: job.mapMemory,
            disk: job.mapDisk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/storage-map',
                root + '/assets/etc/lookup.json',
                root + '/assets/lib/storage-map.js',
                root + '/assets/node_modules.tar'
            ],
            init: root + '/assets/bin/init',
            exec: root + '/assets/bin/storage-map'
        },
        {
            type: 'reduce',
            memory: job.reduce1Memory,
            disk: job.reduce1Disk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/storage-reduce1',
                root + '/assets/lib/storage-reduce1.js',
                root + '/assets/node_modules.tar'
            ],
            init: root + '/assets/bin/init',
            exec: root + '/assets/bin/storage-reduce1',
            count: job.reduce1Count
        },
        {
            type: 'reduce',
            memory: job.reduce2Memory,
            disk: job.reduce2Disk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/storage-reduce2',
                root + '/assets/lib/sum-columns.js',
                root + '/assets/node_modules.tar'
            ],
            init: root + '/assets/bin/init',
            exec: root + '/assets/bin/storage-reduce2',
            count: job.reduce2Count
        },
        {
            type: 'reduce',
            exec: 'cat'
        }
    ];

    addEnv(phases, job.env, datestamp, extraFmt);

    return (phases);
}


function _gen_request_phases(job, datestamp) {
    var user = job.user;
    var root = sprintf('/%s/stor/usage', user);
    var extraFmt = {
        root: root
    };
    var phases = [
        {
            type: 'map',
            memory: job.mapMemory,
            disk: job.mapDisk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/request-map',
                root + '/assets/etc/lookup.json',
                root + '/assets/lib/request-map.js',
                root + '/assets/node_modules.tar'
            ],
            init: root + '/assets/bin/init',
            exec: root + '/assets/bin/request-map'
        },
        {
            type: 'reduce',
            memory: job.reduce1Memory,
            disk: job.reduce1Disk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/request-reduce',
                root + '/assets/etc/lookup.json',
                root + '/assets/lib/deliver-usage.js',
                root + '/assets/lib/sum-columns.js',
                root + '/assets/node_modules.tar'
            ],
            init: '/assets/bin/init',
            exec: '/assets/bin/request-reduce',
            count: job.reduce1Count
        },
        {
            type: 'reduce',
            exec: 'cat',
        }
    ];

    addEnv(phases, job.env, datestamp, extraFmt);

    return (phases);
}


function _gen_compute_phases(job, datestamp) {
    var user = job.user;
    var root = sprintf('/%s/stor/usage', user);
    var extraFmt = {
        root: root
    };
    var phases = [
        {
            type: 'map',
            memory: job.mapMemory,
            disk: job.mapDisk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/compute-map',
                root + '/assets/etc/lookup.json',
                root + '/assets/lib/compute-map.js',
                root + '/assets/node_modules.tar'
            ],
            init: root + '/assets/bin/init',
            exec: root + '/assets/bin/compute-map'
        },
        {
            type: 'reduce',
            memory: job.reduce1Memory,
            disk: job.reduce1Disk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/compute-reduce',
                root + '/assets/etc/lookup.json',
                root + '/assets/lib/compute-reduce.js',
                root + '/assets/lib/deliver-usage.js',
                root + '/assets/node_modules.tar'
            ],
            init: '/assets/bin/init',
            exec: '/assets/bin/compute-reduce',
            count: job.reduce1Count
        },
        {
            type: 'reduce',
            exec: 'cat',
        }
    ];

    addEnv(phases, job.env, datestamp, extraFmt);

    return (phases);
}


function _gen_access_logs_phases(job, datestamp) {
    var user = job.user;
    var root = sprintf('/%s/stor/usage', user);
    var extraFmt = {
        root: root
    };
    var phases = [
        {
            type: 'map',
            memory: job.mapMemory,
            disk: job.mapDisk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/deliver-access-map',
                root + '/assets/etc/lookup.json',
                root + '/assets/node_modules.tar'
            ],
            init: root + '/assets/bin/init',
            exec: root + '/assets/bin/deliver-access-map'
        },
        {
            type: 'reduce',
            memory: job.reduce1Memory,
            disk: job.reduce1Disk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/deliver-access-reduce',
                root + '/assets/etc/lookup.json',
                root + '/assets/lib/deliver-access.js',
                root + '/assets/node_modules.tar'
            ],
            init: '/assets/bin/init',
            exec: '/assets/bin/deliver-access-reduce',
            count: job.reduce1Count
        }
    ];

    addEnv(phases, job.env, datestamp, extraFmt);

    return (phases);
}


function _gen_summary_phases(job, datestamp) {
    var user = job.user;
    var root = sprintf('/%s/stor/usage', user);
    var extraFmt = {
        root: root
    };
    var phases = [
        {
            type: 'map',
            memory: job.mapMemory,
            disk: job.mapDisk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/summarize-map',
                root + '/assets/etc/billingComputeTable.json',
                root + '/assets/etc/lookup.json',
                root + '/assets/lib/summarize-map.js',
                root + '/assets/node_modules.tar'
            ],
            init: root + '/assets/bin/init',
            exec: root + '/assets/bin/summarize-map'
        },
        {
            type: 'reduce',
            memory: job.reduce1Memory,
            disk: job.reduce1Disk,
            assets: [
                root + '/assets/bin/init',
                root + '/assets/bin/summarize-reduce',
                root + '/assets/etc/lookup.json',
                root + '/assets/lib/deliver-usage.js',
                root + '/assets/lib/sum-columns.js',
                root + '/assets/lib/summarize-reduce.js',
                root + '/assets/node_modules.tar'
            ],
            init: '/assets/bin/init',
            exec: '/assets/bin/summary-reduce',
            count: job.reduce1Count
        },
        {
            type: 'reduce',
            exec: 'cat',
        }
    ];

    addEnv(phases, job.env, datestamp, extraFmt);

    return (phases);
}




function register(regfunc) {
    regfunc({
        category: 'mackerel',
        name: 'storage',
        phases: _gen_storage_phases
    });

    regfunc({
        category: 'mackerel',
        name: 'request',
        phases: _gen_request_phases
    });

    regfunc({
        category: 'mackerel',
        name: 'compute',
        phases: _gen_compute_phases
    });

    regfunc({
        category: 'mackerel',
        name: 'access-logs',
        phases: _gen_access_logs_phases
    });

    regfunc({
        category: 'mackerel',
        name: 'summary',
        phases: _gen_summary_phases
    });
}

module.exports = {
    register: register
};
