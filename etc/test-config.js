var c = require('./config.js');

module.exports = c;

c.mantaConfigFile = '/home/dev/config/manta_config.json';
c.mahi.host = '10.99.99.44';
c.workflow.url = 'http://localhost:8080';

if (require.main === module) {
    console.log(JSON.stringify(c, null, 2));
}
