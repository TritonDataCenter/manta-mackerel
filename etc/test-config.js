var c = require('./config.json');

module.exports = c;

c.mahi.host = '10.99.99.44';
c.workflow.url = 'http://localhost:8080';
c.manta.url = '10.99.99.23';
c.manta.user = 'fredkuo';
c.manta.sign.key = '/home/dev/.ssh/poseidon_id_rsa';
c.manta.sign.keyId = '';


if (require.main === module) {
    console.log(JSON.stringify(c, null, 2));
}

