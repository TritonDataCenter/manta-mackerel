#1/bin/bash
json -ga entry | json -ga | grep { | json -c 'type!=="directory"' -ga owner objectId contentLength sharks.length
