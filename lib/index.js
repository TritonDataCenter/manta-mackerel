// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var counter = require('./counter');



///--- Exports

module.exports = {};


// Reexport
Object.keys(counter).forEach(function (k) {
        module.exports[k] = counter[k];
});
