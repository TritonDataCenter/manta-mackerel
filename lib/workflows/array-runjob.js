var runjob = require('./runjob');
var path = require('path');

var keygen = path.resolve(__dirname, '../keygen/ArrayKeyGenerator');
var node_modules = path.resolve(__dirname, '../../node_modules');
var workflow = runjob.workflow;
workflow.name = 'array-' + workflow.name;
workflow.chain[2].modules = {
        'mod_keygen': keygen,
        'mod_jobrunner': '/home/dev/mackerel/lib/jobrunner',
        'mod_manta': node_modules + '/manta'
};

module.exports = workflow;
