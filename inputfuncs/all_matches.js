/* vim: set ts=8 sts=8 sw=8 noet: */

var mod_path = require('path');
var mod_minimatch = require('minimatch');
var mod_verror = require('verror');

var lib_common = require('../lib/common');

var VError = mod_verror.VError;

function
input__all_matches(manta, pattern, dt, callback)
{
	/*
	 * Expand any date macros before attempting to use the path:
	 */
	pattern = lib_common.fmt(pattern, dt);

	var dir = mod_path.dirname(pattern);
	var filepat = mod_path.basename(pattern);

	manta.ls(dir, function (err, res) {
		if (err) {
			if (err.name === 'NotFoundError') {
				/*
				 * Mark this pattern as a missing 'file'
				 * and return:
				 */
				callback(null, [], [ pattern ]);
				return;
			}
			/*
			 * Otherwise, return an error:
			 */
			callback(new VError(err, 'ls failed on "%s"', dir));
			return;
		}

		var ents = [];
		res.on('object', function (obj) {
			if (mod_minimatch(obj.name, filepat)) {
				ents.push(obj.name);
			}
		});
		res.on('end', function () {
			if (ents.length === 0) {
				/*
				 * Mark this pattern as a missing 'file'
				 * and return:
				 */
				callback(null, [], [ pattern ]);
				return;
			}
			var out = ents.map(function (ent) {
				return (mod_path.join(dir, ent));
			});
			callback(null, out);
		});
	});
}

module.exports = input__all_matches;
