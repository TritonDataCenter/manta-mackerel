var c = require('./config.js');

module.exports = c;

c.mahi.host = '10.99.99.44';
c.workflow.url = 'http://localhost:8080';
c.manta.url = '';
c.manta.user = 'fredkuo';
c.manta.sign.key = '/home/dev/.ssh/poseidon_id_rsa';
c.manta.sign.keyId = '';

c.jobs.storage.deliver.keygen = 'ArrayKeyGenerator';
c.jobs.storage.deliver.keygenArgs = {
    array: ['/fredkuo/stor/storage-raw.json.gz']
};


if (require.main === module) {
    console.log(JSON.stringify(c, null, 2));
}

