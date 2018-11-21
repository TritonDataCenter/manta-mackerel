#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

NODE_EXEC ?= node
NODEUNIT=../node_modules/nodeunit/bin/nodeunit

.PHONY: test
test:
	find . -name '*.test.js' | xargs -n 1 $(NODE_EXEC) $(NODEUNIT)
