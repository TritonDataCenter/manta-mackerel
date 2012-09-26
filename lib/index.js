// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var storage = require('./storage');



///--- Exports

module.exports = {};


// Reexport
Object.keys(storage).forEach(function (k) {
        module.exports[k] = storage[k];
});
