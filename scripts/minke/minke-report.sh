#!/bin/bash

outdir=$1
kind=$2
outfile=$outdir/report$kind.html
edir=/poseidon/stor/minke

today=$outdir/today$kind
yesterday=$outdir/yesterday$kind
lastweek=$outdir/lastweek$kind
overtime=$outdir/overtime$kind

LDAP_CREDS="-D cn=root -w secret"
LDAP_URL=ldaps://ufds.us-east-2.joyent.us
PATH=/usr/openldap/bin:$PATH

function userlookup
{
	if [[ `uname -s` -ne "Darwin" ]]; then
		LDAPTLS_REQCERT=allow ldapsearch -LLL -x -H $LDAP_URL \
		    $LDAP_CREDS -b ou=users,o=smartdc uuid=$1
		return
	fi

	cat <<EOF
dn: uuid=$1, ou=users, o=smartdc
cn:: QW5kcsOpcyBWYWxlbmNpYW5v
company: Desarrollos Avanzados de Occidente DAO S.A.
email: joyent@andresvalenciano.com
givenname:: QW5kcsOpcw==
login: andres
objectclass: sdcperson
pwdchangedtime: 1366098340425
sn: Valenciano
uuid: $1
approved_for_provisioning: false
created_at: 1366382411326
updated_at: 1370531701896

EOF
}

function getattr
{
	grep "^$1:" $2 | cut -d: -f2 | cut -c2-
}

function userattr
{
	echo "<div class=user${1}>"
	getattr $1 $2
	echo "</div>"
}

function userdate
{
	created=$(expr `getattr $1 $2` / 1000)

	if [[ `uname -s` -ne "Darwin" ]]; then
		date -d @$created +%m/%d/%Y
		return
	fi

	echo $created
}

function usertable
{
	tmp1=/tmp/minkegen.$$
	tmp2=/tmp/minkegen.$$.2
	tmp3=/tmp/minkegen.$$.3

	sort $2 | awk '{ print $1 }' > $tmp1
	sort $3 | awk '{ print $1 }' > $tmp2
	cat /dev/null > $tmp3

cat >> $outfile <<EOF
<h2>$1</h2><div class=usertable><table class=\"fixed\" width=100%>
    <col width=25%/>
    <col width=10%/>
    <col width=25%/>
    <col width=18%/>
    <col width=9%/>
    <col width=7%/>
    <col width=7%/>
<tr>
<td><b>UUID</b>
<td><b>Login</b>
<td><b>Company</b>
<td><b>E-mail</b>
<td><b>Created</b>
<td><b>Storage (today)</b>
<td><b>Compute (today)</b>
EOF
 
	for uuid in `comm -3 $tmp1 $tmp2 | awk '{ print $1 }'`; do
		echo `grep $uuid $3` >> $tmp3
	done

	for uuid in `sort -rn -k3 -k2 $tmp3 | awk '{ print $1 }'`; do
		use=`grep $uuid $3`

		userlookup $uuid > $tmp1

		echo "<tr><td>`userattr uuid $tmp1`" >> $outfile
		echo "<td>`userattr login $tmp1`" >> $outfile
		echo "<td>`userattr company $tmp1`" >> $outfile
		echo "<td>`userattr email $tmp1`" >> $outfile
		echo "<td>`userdate created_at $tmp1`" >> $outfile
		echo "<td>`echo $use | awk '{ print $2 }'`" >> $outfile
		echo "<td>`echo $use | awk '{ print $3 }'`" >> $outfile
	done

	rm $tmp1 $tmp2 $tmp3

	echo "</table></div>" >> $outfile
	echo >> $outfile
}

cat > $outfile <<EOF
<!DOCTYPE html>
<meta charset="utf-8">
<style>

body {
  font: 12px sans-serif;
}

.axis path,
.axis line {
  fill: none;
  stroke: #000;
  shape-rendering: crispEdges;
}

.x.axis path {
  display: none;
}

.line {
  fill: none;
  stroke: steelblue;
  stroke-width: 3px;
}

.useruuid {
  font: 12px monospace;
}

</style>
<body>
<h1>Manta Executive Report$3: `tail -1 $overtime | \
    awk '{ print $1 }'`</h1>
<table width=95%>
<tr>
<td>
<div id="usersgraph7">
<h2>Users over the past week</h2>
</div>
<td>
<div id="usersgraph28">
<h2>Users over the past four weeks</h2>
</div>
<td>
<div id="usersgraph0">
<h2>Users since launch</h2>
</div>
</tr>
<tr>
<td>
<div id="storagegraph7">
<h2>Storage over the past week</h2>
</div>
<td>
<div id="storagegraph28">
<h2>Storage over the past four weeks</h2>
</div>
<td>
<div id="storagegraph0">
<h2>Storage since launch</h2>
</div>
</tr>
<tr>
<td>
<div id="computegraph7">
<h2>Compute over the past week</h2>
</div>
<td>
<div id="computegraph28">
<h2>Compute over the past four weeks</h2>
</div>
<td>
<div id="computegraph0">
<h2>Compute since launch</h2>
</div>
</tr>
<tr>
<td>
<div id="totalcomputegraph7">
<h2>Cumulative compute over the past week</h2>
</div>
<td>
<div id="totalcomputegraph28">
<h2>Cumulative compute over the past four weeks</h2>
</div>
<td>
<div id="totalcomputegraph0">
<h2>Cumulative compute since launch</h2>
</div>
</tr>
<tr>
<td>
<div id="computecustomersgraph7">
<h2>Compute customers over the past week</h2>
</div>
<td>
<div id="computecustomersgraph28">
<h2>Compute customers over the past four weeks</h2>
</div>
<td>
<div id="computecustomersgraph0">
<h2>Compute customers since launch</h2>
</div>
</tr>
</table>

EOF

usertable "Users new today" $yesterday $today
usertable "Users new in the last week" $lastweek $today
usertable "All users" /dev/null $today

cat >> $outfile <<EOF
<hr>
<h3>Explanatory notes</h3>
All storage is expressed in units of gigabyte-hours (one gigabyte of
non-replicated Manta storage for one hour); all compute is expressed in
units of gigabyte-seconds (one second of compute that includes up to
one gigabyte of DRAM and eight gigabytes of local, volatile storage).
Pricing is \$0.0000589 per gigabyte-hour of storage and
\$0.00004 per gigabyte second of compute.

<script src="https://us-east.manta.joyent.com/poseidon/public/minke/d3.v3.min.js" charset="utf-8"></script>

<script>
function graph(data, field, days, label)
{
	var margin = {top: 20, right: 20, bottom: 30, left: 70},
	    width = 360 - margin.left - margin.right,
	    height = 280 - margin.top - margin.bottom;

	var parseDate = d3.time.format("%Y/%m/%d").parse;

	var x = d3.time.scale()
	    .range([0, width]);

	var y = d3.scale.linear()
	    .range([height, 0]);

	var color = d3.scale.category10();

	var xAxis = d3.svg.axis()
	    .scale(x)
	    .orient("bottom")
	    .ticks(3);

	var yAxis = d3.svg.axis()
	    .scale(y)
	    .orient("left")
	    .ticks(5);

	var line = d3.svg.line()
	    .interpolate("basis")
	    .x(function(d) { return x(d.date); })
	    .y(function(d) { return y(d[field]); });

	var svg = d3.select('#' + field + 'graph' + days).append("svg")
	    .attr("width", width + margin.left + margin.right)
	    .attr("height", height + margin.top + margin.bottom)
	  .append("g")
	    .attr("transform",
	      "translate(" + margin.left + "," + margin.top + ")");

	color.domain([ field ]);

	var values = [];
	var latest = parseDate(data[data.length - 1].date).valueOf();

	data.forEach(function(d) {
		var date = parseDate(d.date);
	
		if (days != 0 &&
		    (latest - date.valueOf()) / 1000 > days * 86400)
			return;

		var value = { date: date };
		value[field] = d[field];
		values.push(value);
	});

	var cities = [ { name: field, values: values } ];

	x.domain(d3.extent(values, function(d) { return d.date; }));

	y.domain([
	    d3.min(cities, function(c) {
		return (d3.min(c.values, function(v) { return v[field]; }));
	    }),
	    d3.max(cities, function(c) {
		return (d3.max(c.values, function(v) { return v[field]; }));
	    })
	]);

	svg.append("g")
	      .attr("class", "x axis")
	      .attr("transform", "translate(0," + height + ")")
	      .call(xAxis);

	svg.append("g")
	      .attr("class", "y axis")
	      .call(yAxis)
	    .append("text")
	      .attr("transform", "rotate(-90)")
	      .attr("y", 6)
	      .attr("dy", ".71em")
	      .style("text-anchor", "end")
	      .text(label ? label :
	        (field[0].toUpperCase() + field.substring(1)));

	var city = svg.selectAll(".city")
	    .data(cities)
	    .enter().append("g")
	      .attr("class", "city");

	city.append("path")
	    .attr("class", "line")
	    .attr("d", function(d) { return line(d.values); })
	    .style("stroke", function(d) { return color(d.name); });
};

var data = [
EOF

cat $overtime | \
    awk '{ ttlcomp += $4; printf("\t{ \"date\": \"%s\", \"users\": %s, \
    \"storage\": %s, \"compute\": %s, \"totalcompute\": %s, \
    \"computecustomers\": %s, \"revenue\": %10.2f },\n", \
    $1, $2, $3, $4, ttlcomp, $5, ($3 * 0.0000589) + ($4 * 0.00004) ) }' >> \
    $outfile

cat >> $outfile <<EOF
];

graph(data, 'users', 0);
graph(data, 'users', 7);
graph(data, 'users', 28);

graph(data, 'storage', 0);
graph(data, 'storage', 7);
graph(data, 'storage', 28);

graph(data, 'compute', 0);
graph(data, 'compute', 7);
graph(data, 'compute', 28);

graph(data, 'totalcompute', 0, 'Compute');
graph(data, 'totalcompute', 7, 'Compute');
graph(data, 'totalcompute', 28, 'Compute');

graph(data, 'computecustomers', 0, 'Customers');
graph(data, 'computecustomers', 7, 'Customers');
graph(data, 'computecustomers', 28, 'Customers');

</script>
EOF

mput -f $outfile $edir/latest.html

