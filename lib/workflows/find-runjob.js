var runjob = require('./runjob');

var workflow = runjob.workflow;
workflow.name = 'find-' + workflow.name;
workflow.chain[2].modules = {
        'mod_jobrunner': '/home/dev/mackerel/lib/jobrunner',
        'mod_keygen': '/home/dev/mackerel/lib/keygen/FindKeyGenerator',
        'mod_manta': '/home/dev/mackerel/node_modules/manta'
};

module.exports = workflow;
