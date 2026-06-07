#!/bin/bash

PROXY=${1:-node3}

echo "Injecting latency into $PROXY..."

curl -X POST 
http://localhost:8474/proxies/$PROXY/toxics 
-H "Content-Type: application/json" 
-d '{
"name":"latency",
"type":"latency",
"stream":"downstream",
"attributes":{
"latency":2000
}
}'

echo "Waiting 15 seconds..."
sleep 15

echo "Removing latency..."

curl -X DELETE 
http://localhost:8474/proxies/$PROXY/toxics/latency

echo "Done."
