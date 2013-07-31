var c = require('./config.json');

module.exports = c;

c.mahi.host = '10.99.99.44';
c.workflow.url = 'http://localhost:8080';
c.manta.url = 'https://us-east.manta.joyent.com';
c.manta.user = 'fredkuo';
c.manta.sign.key = '/home/dev/.ssh/id_rsa';
c.manta.sign.keyId = 'e3:4d:9b:26:bd:ef:a1:db:43:ae:4b:f7:bc:69:a7:24';
c.mantaBaseDirectory = '/fredkuo/stor/mackerel-test';
c.lookupFile = 'test/test_data/lookup.json';


if (require.main === module) {
    console.log(JSON.stringify(c, null, 2));
}

