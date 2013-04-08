// These are not really needed, but javascriptlint will complain otherwise:
var jobrunner;
var keygen;
var manta;
var mackerel;

var VERSION = '0.0.99';
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

        var client = manta.createClient(job.params.mantaConfig);

        mackerel.jobrunner.uploadAssets({
                assets: job.params.assets,
                jobManifest: job.params.jobManifest,
                client: client,
                log: log
        }, function callback(err) {
                client.close();
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

        var client = manta.createClient(job.params.mantaConfig);

        mackerel.jobrunner.createJob({
                jobManifest: job.params.jobManifest,
                log: log,
                client: client
        }, function callback(err, jobPath) {
                client.close();
                job.jobPath = jobPath;
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

        var client = manta.createClient(job.params.mantaConfig);

        var keygenerator = mackerel[job.params.keygen].keygen({
                client: client,
                log: log,
                args: job.params.keygenArgs
        });

        mackerel.jobrunner.addInputKeys({
                keygen: keygenerator,
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(err) {
                client.close();
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

        log.warn({err: err}, 'fallback, ending job');

        var client = manta.createClient(job.params.mantaConfig);

        mackerel.jobrunner.endJobInput({
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(e) {
                client.close();

                if (e) {
                        cb(e);
                        return;
                }

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

        var client = manta.createClient(job.params.mantaConfig);

        mackerel.jobrunner.endJobInput({
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(err) {
                client.close();
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

        var client = manta.createClient(job.params.mantaConfig);

        mackerel.jobrunner.monitorJob({
                monitorBackoff: job.params.monitorBackoff,
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(err) {
                client.close();
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

        var client = manta.createClient(job.params.mantaConfig);

        mackerel.jobrunner.getResults({
                jobPath: job.jobPath,
                log: log,
                client: client
        }, function callback(err, res) {
                // TODO translate res.[errors|failures] to error
                client.close();
                log.info(res);
                cb(err, res);
        });
}

function onerror(job, cb) {
        var log;
        if (job.params.logOpts) {
                log = job.log.child(job.params.logOpts);
        } else {
                log = job.log;
        }

        log.warn({chain_results: job.chain_results});

        cb('retry');
}

var workflow = {
        name: 'runjob-' + VERSION,
        version: VERSION,
        chain: [ {
                name: 'Upload Assets',
                timeout: 60,
                retry: 3,
                body: uploadAssets
        }, {
                name: 'Create Job',
                timeout: 10,
                retry: 3,
                body: createJob
        }, {
                name: 'Add Input Keys',
                timeout: 60,
                retry: 1, // 1 retry because we don't want keys duplicated
                body: addInputKeys,
                fallback: addKeysFallback
        }, {
                name: 'End Job Input',
                timeout: 10,
                retry: 3,
                body: endJobInput
        }, {
                name: 'Monitor Job',
                timeout: 3600,
                retry: 1, // retry built in to the function
                body: monitorJob
        }, {
                name: 'Get Results',
                timeout: 30,
                retry: 3,
                body: getResults
        } ],
        onerror: [ {
                name: 'On Error',
                timeout: 60,
                retry: 1,
                body: onerror
        } ],
        max_attempts: 2,
        /*
        initial_delay: 10000,
        max_delay: 180000*/
};

module.exports = workflow;
