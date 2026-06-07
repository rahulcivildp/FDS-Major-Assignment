from node import Node
import asyncio
from config import NODES
import sys


class AdversaryNode(Node):

    async def handle_client(
        self,
        reader,
        writer
    ):

        print(
            f"Adversary {self.node_id} "
            f"dropping message"
        )

        writer.close()


if __name__ == "__main__":

    node_id = int(sys.argv[1])

    port = NODES[node_id][1]

    peers = {
        k: v
        for k, v in NODES.items()
        if k != node_id
    }

    node = AdversaryNode(
        node_id,
        port,
        peers
    )

    asyncio.run(node.start())