#!/bin/bash

PROXY=${1:-node5}

echo "Disconnecting $PROXY..."

curl -X POST http://localhost:8474/proxies/$PROXY 
-H "Content-Type: application/json" 
-d '{"enabled":false}'

echo "$PROXY isolated for 30 seconds..."
sleep 30

echo "Reconnecting $PROXY..."

curl -X POST http://localhost:8474/proxies/$PROXY 
-H "Content-Type: application/json" 
-d '{"enabled":true}'

echo "Done."
