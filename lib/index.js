// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var storage = require('./storage');



///--- Exports

module.exports = {};
module.exports.storage = {};


// Reexport
Object.keys(storage).forEach(function (k) {
        module.exports.storage[k] = storage[k];
});
