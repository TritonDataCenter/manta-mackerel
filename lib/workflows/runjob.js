// These are not really needed, but javascriptlint will complain otherwise:
var mod_jobrunner;
var mod_keygen;

var VERSION = '0.0.74';
/*
 * params:
 * mantaConfig
 * assets
 * jobManifest
 * keygenArgs
 * backfillPath (TODO moray bucket instead?)
 * [monitorBackoff]
 * [logOpts]
 *
 */

function uploadAssets(job, cb) {
        var log;

        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        var client = mod_manta.createClient(job.params.mantaConfig);
        job.errors = [];

        mod_jobrunner.uploadAssets({
                assets: job.params.assets,
                jobManifest: job.params.jobManifest,
                client: client,
                log: log
        }, function callback(err) {
                client.close();
                if (err) { job.errors.push(err); }
                cb(err);
        });
}

function createJob(job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        log.info({params: job.params});
        var client = mod_manta.createClient(job.params.mantaConfig);

        mod_jobrunner.createJob({
                jobManifest: job.params.jobManifest,
                log: log,
                client: client
        }, function callback(err, jobPath) {
                client.close();
                job.jobPath = jobPath;
                if (err) { job.errors.push(err); }
                cb(err);
        });
}

function addInputKeys(job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        var client = mod_manta.createClient(job.params.mantaConfig);

        var keygen = mod_keygen.keygen({
                client: client,
                log: log,
                args: job.params.keygenArgs
        });

        mod_jobrunner.addInputKeys({
                keygen: keygen,
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(err) {
                client.close();
                if (err) { job.errors.push(err); }
                cb(err);
        });
}

function addKeysFallback(err, job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        log.error({err: err}, 'fallback, ending job');

        var client = mod_manta.createClient(job.params.mantaConfig);

        mod_jobrunner.endJobInput({
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(e) {
                client.close();
                if (e) { job.errors.push(e); }

                // this fallback does not resolve the problem, only mitigates
                // side effects, so callback with an error here to trigger the
                // workflow error branch
                cb('Error adding keys');
        });
}


function endJobInput(job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        var client = mod_manta.createClient(job.params.mantaConfig);

        mod_jobrunner.endJobInput({
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(err) {
                client.close();
                if (err) { job.errors.push(err); }
                cb(err);
        });
}

function monitorJob(job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        var client = mod_manta.createClient(job.params.mantaConfig);

        mod_jobrunner.monitorJob({
                monitorBackoff: job.params.monitorBackoff,
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(err) {
                client.close();
                if (err) { job.errors.push(err); }
                cb(err);
        });
}

function getResults(job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        var client = mod_manta.createClient(job.params.mantaConfig);

        mod_jobrunner.getResults({
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(err, res) {
                // TODO translate res.[errors|failures] to error
                client.close();
                log.info(res);
                job.results = res;
                if (err) { job.errors.push(err); }
                cb(err, res);
        });
}

function recordResults(job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        var client = mod_manta.createClient(job.params.mantaConfig);

        job.params.results = job.results || {
                errors: [],
                failures: [],
                outputs: []
        };

        var jobInfo = {
                name: job.params.jobManifest.name,
                service: job.params.service,
                period: job.params.period,
                date: job.params.date
        };

        var results = {
                jobPath: job.jobPath,
                timestamp: new Date().toISOString(),
                errors: job.errors.concat(job.results.errors),
                failures: job.results.failures,
                outputs: job.results.outputs
        };
        //TODO record results

}

function onerror(job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        log.error({chain_results: job.chain_results});

        /*
        var a = {
                name: 'a'
        };
        a.itself = a;

        job.log.info('before');
        job.log.info(a);
        job.log.info('after');
        */
        var wf = new wfClient({
                url: wfUrl,
                path: wfPath,
                log: bunyan.createLogger({
                        level: 'trace',
                        name: 'wfclient',
                        path: '/var/tmp/wfclient.log'
                })
        });

        /*
        // using task's logger will make the runner crash
        // TODO investigate this more
        var wf = new wfClient({
                url: wfUrl,
                path: wfPath,
                log: job.log
        });
        */
        var params = job.params; // deep clone?

        if (job.params.numAttempts >= job.params.maxAttempts) {
                wf.createJob(job.params, /* ... */ );
                // retry
        } else {
                // error
        }

};

function fatalFallback(err, job, cb) {
        job.log.fatal(err);
        cb();
}

var workflow = {
        name: 'runjob-' + VERSION,
        version: VERSION,
        chain: [ {
                name: 'Upload Assets',
                timeout: 60, // seconds
                retry: 3,
                body: uploadAssets
        }, {
                name: 'Create Job',
                timeout: 10, // seconds
                retry: 3,
                body: createJob
        }, {
                name: 'Add Input Keys',
                timeout: 60, // seconds
                retry: 1, // 1 retry because we don't want keys duplicated
                body: addInputKeys,
                fallback: addKeysFallback
        }, {
                name: 'End Job Input',
                timeout: 10, // seconds
                retry: 3,
                body: endJobInput
        }, {
                name: 'Monitor Job',
                timeout: 3600, // seconds (one hour)
                retry: 1, // retry built in to the function
                body: monitorJob
        }, {
                name: 'Get Results',
                timeout: 30, // seconds
                retry: 3,
                body: getResults
        }/*, {
                name: 'Record Results',
                timeout: 30, // seconds
                retry: 3,
                body: recordResults
        }*/ ],
        onerror: [ {
                name: 'On Error',
                timeout: 60, // seconds
                retry: 1,
                body: onerror
        }/*, {
                name: 'Record Results On Error',
                timeout: 30, // seconds
                retry: 3,
                body: recordResults,
                fallback: fatalFallback
        } */]
};

module.exports = {
        workflow: workflow,
};
