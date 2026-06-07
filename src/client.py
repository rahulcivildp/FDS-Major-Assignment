import socket
import json
import sys

from crypto_utils import sign_message
from keys import load_or_create_keys
from config import NODES

private_key, public_key = load_or_create_keys(0)

# Default to Node 1 if not provided
node_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1

host, port = NODES[node_id]

payload = {
    "type": "TRANSACTION",
    "data": {
        "id": 1,
        "amount": 100
    }
}

signature = sign_message(
    private_key,
    json.dumps(payload).encode()
)

packet = {
    "payload": payload,
    "signature": signature.hex(),
    "sender": 0
}

try:
    s = socket.socket()

    s.connect((host, port))

    s.send(
        json.dumps(packet).encode()
    )

    print(
        f"Transaction sent to "
        f"Node {node_id} "
        f"({host}:{port})"
    )

    s.close()

except Exception as e:

    print(
        f"Failed to connect to "
        f"Node {node_id}: {e}"
    )