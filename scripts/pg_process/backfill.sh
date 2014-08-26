#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

# ghetto backfill script -- replace echo ' ' with whatever hour you are missing
echo $1 |  mjob create --memory=8192 --disk=512 -s /poseidon/stor/pgdump/assets/postgresql.conf -s /poseidon/stor/pgdump/assets/process_dump.sh -m /assets/poseidon/stor/pgdump/assets/process_dump.sh -w
