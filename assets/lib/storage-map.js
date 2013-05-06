#!/usr/bin/env node
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var mod_carrier = require('carrier');

/* BEGIN JSSTYLED */
/*
 * sample first line
 * {
 *   "name": "manta",
 *   "keys": [
 *     "_id",
 *     "_txn_snap",
 *     "_key",
 *     "_value",
 *     "_etag",
 *     "_mtime",
 *     "dirname",
 *     "owner",
 *     "objectid",
 *     "type"
 *   ]
 * }
 *
 */

/*
 * sample entry
 * {
 *   "entry": [
 *     "58",
 *     "\\N",
 *     "/cc56f978-00a7-4908-8d20-9580a3f60a6e/stor/logs/postgresql/2012/11/12/18/49366a2c.log.bz2",
 *     "{
 *              \"dirname\":\"/cc56f978-00a7-4908-8d20-9580a3f60a6e/stor/logs/postgresql/2012/11/12/18\",
 *              \"key\":\"/cc56f978-00a7-4908-8d20-9580a3f60a6e/stor/logs/postgresql/2012/11/12/18/49366a2c.log.bz2\",
 *              \"headers\":{},
 *              \"mtime\":1352746869592,
 *              \"owner\":\"cc56f978-00a7-4908-8d20-9580a3f60a6e\",
 *              \"type\":\"object\",
 *              \"contentLength\":84939,
 *              \"contentMD5\":\"iSdRMW7Irsw1UwYoRDFmIA==\",
 *              \"contentType\":\"application/x-bzip2\",
 *              \"etag\":\"5fcc0345-1044-4b67-b7e8-98ee692001bc\",
 *              \"objectId\":\"5fcc0345-1044-4b67-b7e8-98ee692001bc\",
 *              \"sharks\":[{
 *                      \"availableMB\":20477,
 *                      \"percentUsed\":1,
 *                      \"datacenter\":\"bh1-kvm1\",
 *                      \"server_uuid\":\"44454c4c-4700-1034-804a-c7c04f354d31\",
 *                      \"zone_uuid\":\"ef8b166a-ac3e-4d59-bb73-a65e2b17ba44\",
 *                      \"url\":\"http://ef8b166a-ac3e-4d59-bb73-a65e2b17ba44.stor.bh1-kvm1.joyent.us\"
 *              }, {
 *                      \"availableMB\":20477,
 *                      \"percentUsed\":1,
 *                      \"datacenter\":\"bh1-kvm1\",
 *                      \"server_uuid\":\"44454c4c-4700-1034-804a-c7c04f354d31\",
 *                      \"zone_uuid\":\"59fb8bd3-67a7-4da2-bb68-287e2db01ec1\",
 *                      \"url\":\"http://59fb8bd3-67a7-4da2-bb68-287e2db01ec1.stor.bh1-kvm1.joyent.us\"
 *              }]
 *      }",
 *     "6C4D4587",
 *     "1352746869598",
 *     "/cc56f978-00a7-4908-8d20-9580a3f60a6e/stor/logs/postgresql/2012/11/12/18",
 *     "cc56f978-00a7-4908-8d20-9580a3f60a6e",
 *     "5fcc0345-1044-4b67-b7e8-98ee692001bc",
 *     "object"
 *   ]
 * }
 */
/* END JSSTYLED */


function validSchema(obj) {
        var fields =
                ['key', 'owner', 'type'];
        for (var i = 0; i < fields.length; i++) {
                if (!obj[fields[i]]) {
                        return (false);
                }
        }
        return (true);
}


function main() {
        var carry = mod_carrier.carry(process.openStdin());
        var index;

        function onLine(line) {
                try {
                        var record = JSON.parse(line);
                } catch (e) {
                        console.warn(e);
                        return;
                }

                if (!record.entry || !record.entry[index] ||
                        !validSchema(JSON.parse(record.entry[index]))) {

                        console.warn('Unrecognized line: ' + line);
                        return;
                }

                console.log(record.entry[index]);
        }

        carry.once('line', function firstLine(line) {
                index = JSON.parse(line).keys.indexOf('_value');
                carry.on('line', onLine);
        });

}

if (require.main === module) {
        main();
}
