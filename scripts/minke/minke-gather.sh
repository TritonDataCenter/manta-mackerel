#!/bin/bash

set -o pipefail
set -o errexit
minke=/poseidon/stor/minke

comped="EmployeePersonalUse.txt EmployeeWork.txt ThirdPartyFree.txt misc.txt"
cdir=$minke/comped
mdir=/poseidon/stor
summary=$mdir/usage/summary
mjs=$minke/scripts/process.js
outdir=`pwd`/minke.$$

mmkdir -p $minke/scripts
mkdir -p $outdir

js=$outdir/process.js
summaries=$outdir/summaries

cat > $js <<EOF
var comped = {
EOF

for comp in $comped; do
	mget $cdir/$comp | tr '' '\n' | \
	    awk '{ printf("\t\"%s\": true,\n", $0)}' >> $js
done

cat >> $js <<EOF
};

var last = '';
var showcomp = process.argv.length >= 3 ? true : false;

process.stdin.on('data', function (chunk) {
	chunk = last + chunk;

	for (;;) {
		var nl = chunk.indexOf('\n');

		if (nl == -1) {
			last = chunk;
			return;
		}

		line = chunk.substr(0, nl);
		chunk = chunk.substr(nl + 1);	

		payload = JSON.parse(line);

		if ((!showcomp && comped[payload.owner]) ||
		    (showcomp && !comped[payload.owner]))
			continue;

		console.log(payload.owner + ' ' + payload.storageGBHours +
		    ' ' + payload.computeGBSeconds);
	}
});

process.stdin.resume();
process.stdin.setEncoding('utf8');
EOF

mput -f $js $mjs
mfind $summary | grep json | sort > $summaries

function crank
{
	today=$outdir/today$1
	yesterday=$outdir/yesterday$1
	lastweek=$outdir/lastweek$1
	overtime=$outdir/overtime$1
	filter="node /assets/$mjs $1"

	tail -1 $summaries | \
	    mjob create -o -s $mjs -m "$filter" > $today

	tail -2 $summaries | head -1 | \
	    mjob create -o -s $mjs -m "$filter" > $yesterday

	tail -8 $summaries | head -1 | \
	    mjob create -o -s $mjs -m "$filter" > $lastweek

	cat $summaries | \
	    mjob create -o -s $mjs -m "echo \`echo \$MANTA_INPUT_OBJECT | \
	    cut -d/ -f6-8\` \`cat \$MANTA_INPUT_FILE | $filter | \
	    awk '{ storage += \$2; compute += \$3; if (\$3 != 0) { cust++ } } \
	    END { printf(\"%d %d %d %d\", NR, storage, compute, cust) }'\`" | \
	    sort | grep -v "0 0 0" > $overtime
}

crank
crank .joyent

ndir=minke.`tail -1 $overtime | awk '{ print $1 }' | tr '/' '-'`

echo "minke: directory is $ndir"

if [[ -d $ndir ]]; then
	echo "minke: $ndir exists; exiting"
	rm -rf $outdir
	exit 0
fi

mv $outdir $ndir
./minke-report.sh $ndir
./minke-report.sh $ndir .joyent " (Joyent only)"
