/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var events = require('events');
var manta = require('manta');
var meter = require('../lib/meter');
var once = require('once');
var fs = require('fs');
var mod_path = require('path');
var exec = require('child_process').exec;

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');

///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;
var config = process.env.CONFIG ? require(process.env.CONFIG) :
        require('../etc/test-config.js');
var jobs = require('../etc/jobs.json');
var lookupFile = mod_path.resolve(__dirname, '..', config.lookupFile);

///--- Tests
test('configureJob - date format', function (t) {
        var date = new Date(Date.parse('2013-07-10T05:00:00'));
        var jobConfig = {
                keygenArgs: {
                        source: '/$year/$month/$day/$hour'
                },
                job: {
                        name: 'metering-$year-$month-dayT$hour',
                        phases: [ {
                                type: 'map',
                                exec: '/poseidon/stor/exec'
                        } ]
                },
                env: {
                        VAR_A: '$year-$month-$day'
                }
        };

        var expected = {
                keygenArgs: {
                        source: '/2013/07/10/05',
                        date: date.toISOString()
                },
                job: {
                        name: 'metering-2013-07-dayT05',
                        phases: [ {
                                type: 'map',
                                exec: 'VAR_A="2013-07-10" /poseidon/stor/exec'
                        } ]
                },
                env: {
                        VAR_A: '2013-07-10'
                }
        };
        meter.configureJob({
                jobConfig: jobConfig,
                date: date,
                mantaDir: '/test'
        });
        t.deepEqual(jobConfig, expected);
        t.done();
});

test('configureJob - reducer count', function (t) {
        var date = new Date(Date.parse('2013-07-10T05:00:00'));
        var jobConfig = {
                keygenArgs: {
                        source: '/$year/$month/$day/$hour'
                },
                job: {
                        name: 'metering-$year-$month-dayT$hour',
                        phases: [ {
                                type: 'map',
                                exec: '/poseidon/stor/exec'
                        }, {
                                type: 'reduce',
                                exec: '/poseidon/stor/reduce1',
                                count: 5
                        }, {
                                type: 'reduce',
                                exec: '/poseidon/stor/reduce2',
                                count: 7
                        } ]
                }
        };

        var expected = {
                keygenArgs: {
                        source: '/2013/07/10/05',
                        date: date.toISOString()
                },
                job: {
                        name: 'metering-2013-07-dayT05',
                        phases: [ {
                                type: 'map',
                                exec: 'NUM_REDUCERS=5 /poseidon/stor/exec'
                        }, {
                                type: 'reduce',
                                exec: 'NUM_REDUCERS=7 /poseidon/stor/reduce1',
                                count: 5
                        }, {
                                type: 'reduce',
                                exec: '/poseidon/stor/reduce2',
                                count: 7
                        } ]
                }
        };
        meter.configureJob({
                jobConfig: jobConfig,
                date: date,
                mantaDir: '/test'
        });
        t.deepEqual(jobConfig, expected);
        t.done();
});

test('configureJob - relative paths', function (t) {
        var date = new Date(Date.parse('2013-07-10T05:00:00'));
        var jobConfig = {
                keygenArgs: {
                        source: '/$year/$month/$day/$hour'
                },
                job: {
                        name: 'metering-$year-$month-dayT$hour',
                        phases: [ {
                                type: 'map',
                                exec: 'bin/map',
                                init: 'bin/init'
                        }, {
                                type: 'reduce',
                                assets: [
                                        'bin/reduce1'
                                ],
                                exec: 'bin/reduce1',
                                init: '/not/relative',
                                count: 5
                        }, {
                                type: 'reduce',
                                exec: '/assets/poseidon/stor/assets/reduce2',
                                count: 7
                        } ]
                }
        };

        var expected = {
                keygenArgs: {
                        source: '/2013/07/10/05',
                        date: date.toISOString()
                },
                job: {
                        name: 'metering-2013-07-dayT05',
                        phases: [ {
                                type: 'map',
                                init: '/assets/test/bin/init',
                                exec: 'NUM_REDUCERS=5 /assets/test/bin/map'
                        }, {
                                type: 'reduce',
                                assets: [
                                        '/test/bin/reduce1'
                                ],
                                init: '/not/relative',
                                exec: 'NUM_REDUCERS=7 /assets/test/bin/reduce1',
                                count: 5
                        }, {
                                type: 'reduce',
                                exec: '/assets/poseidon/stor/assets/reduce2',
                                count: 7
                        } ]
                }
        };
        meter.configureJob({
                jobConfig: jobConfig,
                date: date,
                mantaDir: '/test'
        });
        t.deepEqual(jobConfig, expected);
        t.done();
});

test('generateAssetMap', function (t) {
        var job = {
                name: 'metering-2013-07-dayT05',
                phases: [ {
                        type: 'map',
                        exec: 'NUM_REDUCERS=5 /assets/test/bin/map'
                }, {
                        type: 'reduce',
                        assets: [
                                '/test/bin/reduce1'
                        ],
                        exec: 'NUM_REDUCERS=7 /assets/test/bin/reduce1',
                        count: 5
                }, {
                        type: 'reduce',
                        assets: [
                                '/test/bin/reduce1'
                        ],
                        exec: '/assets/poseidon/stor/assets/reduce2',
                        count: 7
                } ]
        };
        var expected = {
                '/test/bin/reduce1': '/local/bin/reduce1'
        };
        var actual = meter.generateAssetMap({
                job: job,
                mantaDir: '/test',
                localDir: '/local'
        });

        t.deepEqual(actual, expected);
        t.done();
});

test('generateAssetMap - overrides', function (t) {
        var job = {
                name: 'metering-2013-07-dayT05',
                phases: [ {
                        type: 'map',
                        exec: 'NUM_REDUCERS=5 /assets/test/bin/map'
                }, {
                        type: 'reduce',
                        assets: [
                                '/test/bin/reduce1'
                        ],
                        exec: 'NUM_REDUCERS=7 /assets/test/bin/reduce1',
                        count: 5
                }, {
                        type: 'reduce',
                        assets: [
                                '/test/bin/reduce2'
                        ],
                        exec: '/assets/poseidon/stor/assets/reduce2',
                        count: 7
                } ]
        };
        var expected = {
                '/test/bin/reduce1': '/local/bin/reduce1',
                '/test/bin/reduce2': '/other/dir/reduce2'
        };
        var actual = meter.generateAssetMap({
                job: job,
                mantaDir: '/test',
                localDir: '/local',
                overrides: {
                        '/test/bin/reduce2': '/other/dir/reduce2'
                }
        });

        t.deepEqual(actual, expected);
        t.done();
});
