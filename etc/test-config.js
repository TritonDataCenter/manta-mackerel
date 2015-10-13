/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var c = require('./config.json');

module.exports = c;

c.mahi.host = 'authcache.emy-10.joyent.us';
c.workflow.url = 'http://172.25.10.19';
c.manta.url = process.env['MANTA_URL'];
c.manta.user = process.env['MANTA_USER'];
c.manta.sign.key = process.env['HOME'] + '/.ssh/id_rsa';
c.manta.sign.keyId = process.env['MANTA_KEY_ID'];
c.mantaBaseDirectory = '/' + process.env['MANTA_USER'] + '/stor/mackerel-test';
c.manta.rejectUnauthorized = false;
c.lookupFile = 'test/test_data/lookup.json';


if (require.main === module) {
    console.log(JSON.stringify(c, null, 2));
}

