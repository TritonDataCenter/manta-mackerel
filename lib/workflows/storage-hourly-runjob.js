var runjob = require('./runjob');

var workflow = runjob.workflow;
workflow.name = 'storage-hourly-' + workflow.name;
workflow.chain[2].modules = {
        'mod_keygen': '/home/dev/mackerel/lib/keygen/StorageHourlyKeyGenerator',
        'mod_jobrunner': '/home/dev/mackerel/lib/jobrunner',
        'mod_manta': '/home/dev/mackerel/node_modules/manta'
};

module.exports = workflow;
