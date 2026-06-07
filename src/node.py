# node.py

import asyncio
import json
import time

from transaction_log import TransactionLog
from crypto_utils import sign_message, verify_signature
from keys import load_or_create_keys, load_all_public_keys

from messages import (
    HEARTBEAT,
    ELECTION,
    ELECTION_OK,
    NEW_LEADER,
    TRANSACTION,
    PREPARE,
    PROMISE,
    ACCEPT,
    ACCEPTED,
    PRE_PREPARE,
    COMMIT
)

CONSENSUS_MODE = "PBFT" # "PAXOS" or "PBFT"

class Node:

    def __init__(self, node_id, port, peers):
        self.public_keys = load_all_public_keys()

        print("Loaded public keys:", self.public_keys.keys())

        self.node_id = node_id
        self.port = port
        self.peers = peers

        self.leader_id = None
        self.pbft_prepare_votes = {}
        self.pbft_commit_votes = {}

        self.last_heartbeat = time.time()

        self.election_in_progress = False

        self.received_ok = False

        self.proposal_number = 0

        self.promises = set()

        self.accepted_nodes = set()

        self.current_value = None

        self.highest_prepare_seen = 0

        self.log = TransactionLog(
            self.node_id
        )

        self.accept_sent = False

        self.private_key, self.public_key = load_or_create_keys(
            self.node_id
        )

        self.committed_transactions = set()

        self.pbft_commit_sent = {}

        self.heartbeat_count = 0
        self.last_heartbeat_time = time.time()
        
        self.alive_nodes = set(self.peers.keys())
        
    async def send_message(self, host, port, message):

        payload = json.dumps(message).encode()

        signature = sign_message(
            self.private_key,
            payload
        )

        packet = {
            "payload": message,
            "signature": signature.hex(),
            "sender": self.node_id
        }

        try:

            print(
                f"Node {self.node_id} -> "
                f"{host}:{port} : "
                f"{message['type']}"
            )

            reader, writer = await asyncio.open_connection(
                host,
                port
            )

            writer.write(
                json.dumps(packet).encode()
            )

            await writer.drain()

            writer.close()
            await writer.wait_closed()

            return True

        except Exception as e:

            print(
                f"Failed to send message: {e}"
            )

            return False
    
    async def handle_client(
        self,
        reader,
        writer
    ):
  
        print(
            f"Node {self.node_id} "
            f"accepted connection"
        )

        data = await reader.read(4096)

        message = json.loads(data.decode())

        # Signed packet
        if "payload" in message:

            payload = message["payload"]

            signature = bytes.fromhex(
                message["signature"]
            )

            sender = message["sender"]

            if sender not in self.public_keys:
                print(
                    f"Missing public key for Node {sender}"
                )
                writer.close()
                return

            valid = verify_signature(
                self.public_keys[sender],
                json.dumps(payload).encode(),
                signature
            )

            print(
                f"Signature from Node {sender}: {valid}"
            )

            if not valid:
                print(
                    f"SECURITY ALERT: Invalid signature "
                    f"from Node {sender}"
                )
                writer.close()
                return

            message = payload

        # Unsigned packet (debug/legacy)
        else:

            print(
                "WARNING: Unsigned message received"
            )

        msg_type = message.get("type")

        print(
            f"Node {self.node_id} received:",
            message
        )

        if msg_type == ELECTION:

            await self.handle_election(message)

        elif msg_type == ELECTION_OK:

            self.received_ok = True

            print(
                f"Node {self.node_id} received ELECTION_OK"
            )

        elif msg_type == TRANSACTION:

            print(
                f"Transaction received: "
                f"{message['data']}"
            )

            if self.leader_id == self.node_id:

                try:

                    if CONSENSUS_MODE == "PAXOS":

                        print(
                            "I am leader. Starting Paxos."
                        )

                        await self.propose_transaction(
                            message["data"]
                        )


                    elif CONSENSUS_MODE == "PBFT":

                        print(
                            "I am leader. Starting PBFT."
                        )

                        await self.pbft_pre_prepare(
                            message["data"]
                        )

                except Exception as e:

                    print(
                        f"Consensus error: {e}"
                    )

            else:

                print(
                    f"Forwarding transaction "
                    f"to leader {self.leader_id}"
                )

                host, port = self.peers[
                    self.leader_id
                ]

                await self.send_message(
                    host,
                    port,
                    message
                )

        elif msg_type == PRE_PREPARE:

            await self.handle_pre_prepare(
                message
            )

        elif msg_type == PREPARE:

            print(
                f"Node {self.node_id} received PREPARE"
            )

            if CONSENSUS_MODE == "PAXOS":

                await self.handle_prepare(message)

            elif CONSENSUS_MODE == "PBFT":

                await self.handle_pbft_prepare(message)

        elif msg_type == PROMISE:

            await self.handle_promise(message)

        elif msg_type == ACCEPT:

            await self.handle_accept(message)

        elif msg_type == ACCEPTED:

            await self.handle_accepted(message)

        elif msg_type == HEARTBEAT:

            leader = message["leader"]

            if leader < self.node_id:

                print(
                    f"Node {self.node_id}: "
                    f"Ignoring lower leader {leader}"
                )

                if self.election_in_progress:

                    print(
                        f"Node {self.node_id}: "
                        f"Election already running"
                    )

                else:

                    asyncio.create_task(
                        self.start_election()
                    )

                return

            self.leader_id = leader

            self.last_heartbeat = time.time()

            print(
                f"Node {self.node_id}: "
                f"Heartbeat received from leader "
                f"{leader}"
            )

        elif msg_type == NEW_LEADER:

            new_leader = message["leader"]

            self.leader_id = new_leader

            self.last_heartbeat = time.time()
            
            self.received_ok = True

            self.election_in_progress = False

            print(
                f"Node {self.node_id}: "
                f"NEW LEADER = {new_leader}"
            )

        elif msg_type == COMMIT:

            await self.handle_pbft_commit(
                message
            )

        writer.close()

    async def handle_election(self, message):

        sender = message["node_id"]

        print(
            f"Node {self.node_id} received "
            f"ELECTION from Node {sender}"
        )

        if self.node_id > sender:

            if sender not in self.peers:
                return

            host, port = self.peers[sender]

            await self.send_message(
                host,
                port,
                {
                    "type": ELECTION_OK,
                    "node_id": self.node_id
                }
            )

            if not self.election_in_progress:

                self.election_in_progress = True

                asyncio.create_task(
                    self.start_election()
                )

    async def handle_prepare(
        self,
        message
    ):

        proposal = message["proposal"]

        if proposal > self.highest_prepare_seen:

            self.highest_prepare_seen = proposal

            leader = message["leader"]

            host, port = self.peers[leader]

            await self.send_message(
                host,
                port,
                {
                    "type": PROMISE,
                    "proposal": proposal,
                    "node": self.node_id
                }
            )

    async def handle_promise(self, message):

        self.promises.add(
            message["node"]
        )

        print(
            f"Promises: {self.promises}"
        )

        majority = (
            len(self.peers) + 1
        ) // 2 + 1

        print(
            f"Need majority: {majority}"
        )

        print(
            f"Current promises: "
            f"{len(self.promises)}"
        )

        if (
            len(self.promises) >= majority - 1
            and not self.accept_sent
        ):

            self.accept_sent = True

            print(
                "PROMISE MAJORITY REACHED"
            )

            print(
                "Sending ACCEPT messages"
            )

            for node, addr in self.peers.items():

                host, port = addr

                await self.send_message(
                    host,
                    port,
                    {
                        "type": ACCEPT,
                        "proposal":
                            self.proposal_number,
                        "value":
                            self.current_value
                    }
                )

    async def handle_accept(
        self,
        message
    ):

        self.current_value = message["value"]

        leader = self.leader_id

        if leader in self.peers:

            host, port = self.peers[leader]

            print(
                f"Node {self.node_id} "
                f"received ACCEPT"
            )

            await self.send_message(
                host,
                port,
                {
                    "type": ACCEPTED,
                    "node": self.node_id
                }
            )

    async def handle_accepted(
        self,
        message
    ):

        self.accepted_nodes.add(
            message["node"]
        )

        majority = (
            len(self.peers) + 1
        ) // 2 + 1

        if len(self.accepted_nodes) >= majority - 1:

            print(
                "CONSENSUS REACHED"
            )

            print(
                self.current_value
            )

            self.commit_once(
                self.current_value
            )

            print(
                f"Transaction committed "
                f"to ledger_{self.node_id}.json"
            )

            self.accepted_nodes.clear()
            
    async def heartbeat_loop(self):

        await asyncio.sleep(1)

        while True:

            await asyncio.sleep(2)

            if (
                self.leader_id is not None
                and self.leader_id == self.node_id
            ):

                self.heartbeat_count += 1

                elapsed = (
                    time.time()
                    - self.last_heartbeat_time
                )

                print(
                    f"Node {self.node_id} sending "
                    f"heartbeat #{self.heartbeat_count} "
                    f"(interval: {elapsed:.1f}s)"
                )

                self.last_heartbeat_time = time.time()

                tasks = []

                for node, addr in self.peers.items():

                    host, port = addr

                    tasks.append(
                        self.send_message(
                            host,
                            port,
                            {
                                "type": HEARTBEAT,
                                "leader": self.node_id
                            }
                        )
                    )

                results = await asyncio.gather(
                    *tasks,
                    return_exceptions=True
                )

                failed = sum(
                    1 for r in results
                    if isinstance(r, Exception) or not r
                )

                if failed > 0:
                    print(
                        f"Node {self.node_id}: "
                        f"{failed} heartbeats failed"
                    )

    async def start_election(self):

        self.election_in_progress = True

        try:

            self.received_ok = False

            print(
                f"Node {self.node_id} "
                f"starting election"
            )

            higher_nodes = [
                node
                for node in self.peers
                if node > self.node_id
            ]

            if not higher_nodes:

                self.leader_id = self.node_id

                print(
                    f"Node {self.node_id} "
                    f"became leader "
                    f"(highest node)"
                )

                await self.broadcast_leader()
                return

            for node in higher_nodes:

                host, port = self.peers[node]

                await self.send_message(
                    host,
                    port,
                    {
                        "type": ELECTION,
                        "node_id": self.node_id
                    }
                )

            await asyncio.sleep(4)

            if not self.received_ok:

                self.leader_id = self.node_id

                print(
                    f"Node {self.node_id} "
                    f"became leader"
                )

                await self.broadcast_leader()

            else:

                print(
                    f"Node {self.node_id} "
                    f"received ELECTION_OK"
                )

        finally:

            self.election_in_progress = False


    async def broadcast_leader(self):

        print(
            f"Node {self.node_id} broadcasting NEW_LEADER"
        )

        for node, addr in self.peers.items():

            host, port = addr

            await self.send_message(
                host,
                port,
                {
                    "type": NEW_LEADER,
                    "leader": self.node_id
                }
            )

    async def monitor_leader(self):

        last_log_time = time.time()

        while True:

            if self.leader_id != self.node_id:

                elapsed = (
                    time.time()
                    - self.last_heartbeat
                )

                now = time.time()

                if (
                    now - last_log_time > 5
                ):
                    print(
                        f"Node {self.node_id}: "
                        f"Leader {self.leader_id} "
                        f"heartbeat age: {elapsed:.1f}s"
                    )
                    last_log_time = now

                if (
                    self.leader_id is not None
                    and elapsed > 8
                    and not self.election_in_progress
                ):

                    print(
                        f"Node {self.node_id}: "
                        f"Leader {self.leader_id} "
                        f"timeout detected "
                        f"({elapsed:.1f}s)"
                    )

                    self.leader_id = None
                    await self.start_election()

                    self.last_heartbeat = time.time()

            await asyncio.sleep(1)

    async def delayed_startup(self):

        await asyncio.sleep(
            self.node_id
        )

        if self.leader_id is None:

            if self.election_in_progress:

                print(
                    f"Node {self.node_id}: "
                    f"Election already running"
                )

            else:

                self.election_in_progress = True

                await self.start_election()

        else:

            print(
                f"Node {self.node_id}: "
                f"Leader already known "
                f"({self.leader_id})"
            )

    async def start(self):

        server = await asyncio.start_server(
            self.handle_client,
            "0.0.0.0",
            self.port
        )

        print(
            f"Node {self.node_id} running on {self.port}"
        )

        async with server:

            asyncio.create_task(
                self.heartbeat_loop()
            )

            asyncio.create_task(
                self.monitor_leader()
            )

            # Start election automatically
            asyncio.create_task(
                self.delayed_startup()
            )

            await server.serve_forever()

    async def propose_transaction(
        self,
        transaction
    ):

        self.proposal_number += 1

        print(
            f"Starting Paxos proposal "
            f"{self.proposal_number}"
        )

        self.current_value = transaction

        self.accept_sent = False
        self.accepted_nodes.clear()
        self.promises.clear()

        for node, addr in self.peers.items():

            host, port = addr

            print(
                f"Sending PREPARE to Node {node}"
            )

            await self.send_message(
                host,
                port,
                {
                    "type": PREPARE,
                    "proposal": self.proposal_number,
                    "leader": self.node_id
                }
            )

    def commit_once(
        self,
        transaction
    ):

        tx_id = transaction["id"]

        if tx_id in self.committed_transactions:

            return

        self.committed_transactions.add(
            tx_id
        )

        print(
            f"COMMITTED: {transaction}"
        )

        self.log.append(transaction)

        self.pbft_prepare_votes.pop(tx_id, None)
        self.pbft_commit_votes.pop(tx_id, None)
        self.pbft_commit_sent.pop(tx_id, None)
        
    async def handle_pbft_commit(
        self,
        message
    ):

        tx = message[TRANSACTION]

        tx_id = tx["id"]

        sender = message["node"]

        if tx_id not in self.pbft_commit_votes:
            self.pbft_commit_votes[tx_id] = set()

        self.pbft_commit_votes[tx_id].add(sender)

        if CONSENSUS_MODE == "PBFT":
            self.pbft_commit_votes[tx_id].add(
                self.node_id
            )

        n = len(self.peers) + 1

        threshold = (2 * n) // 3 + 1

        if len(
            self.pbft_commit_votes[tx_id]
        ) >= threshold:

            print(
                "PBFT CONSENSUS REACHED"
            )

            self.commit_once(tx)

    async def handle_pre_prepare(
        self,
        message
    ):

        tx = message[TRANSACTION]

        tx_id = tx["id"]

        if tx_id not in self.pbft_prepare_votes:

            self.pbft_prepare_votes[tx_id] = set()

        self.pbft_prepare_votes[tx_id].add(
            self.node_id
        )

        prepare_msg = {
            "type": PREPARE,
            TRANSACTION: tx,
            "node": self.node_id
        }

        tasks = []

        for node_id, (host, port) in self.peers.items():

            if node_id != self.node_id:

                tasks.append(
                    self.send_message(
                        host,
                        port,
                        prepare_msg
                    )
                )

        await asyncio.gather(*tasks)

    async def handle_pbft_prepare(
        self,
        message
    ):

        tx = message[TRANSACTION]
        tx_id = tx["id"]
        sender = message["node"]

        if tx_id not in self.pbft_prepare_votes:
            self.pbft_prepare_votes[tx_id] = set()

        self.pbft_prepare_votes[tx_id].add(sender)

        if tx_id not in self.pbft_commit_sent:
            self.pbft_commit_sent[tx_id] = False

        n = len(self.peers) + 1
        threshold = (2 * n) // 3 + 1

        if (
            len(self.pbft_prepare_votes[tx_id]) >= threshold
            and not self.pbft_commit_sent[tx_id]
        ):

            self.pbft_commit_sent[tx_id] = True

            commit_msg = {
                "type": COMMIT,
                TRANSACTION: tx,
                "node": self.node_id
            }

            tasks = []

            for node_id, (host, port) in self.peers.items():

                if node_id != self.node_id:

                    tasks.append(
                        self.send_message(
                            host,
                            port,
                            commit_msg
                        )
                    )

            await asyncio.gather(*tasks)

    async def pbft_pre_prepare(
        self,
        transaction
    ):

        tx_id = transaction["id"]

        self.pbft_prepare_votes[tx_id] = {
            self.node_id
        }

        msg = {
            "type": PRE_PREPARE,
            TRANSACTION: transaction,
            "leader": self.node_id
        }

        tasks = []

        for node_id, (host, port) in self.peers.items():

            if node_id != self.node_id:

                tasks.append(
                    self.send_message(
                        host,
                        port,
                        msg
                    )
                )

        await asyncio.gather(*tasks)
            
if __name__ == "__main__":

    import sys
    from config import NODES

    node_id = int(sys.argv[1])

    port = NODES[node_id][1]

    peers = {
        k: v
        for k, v in NODES.items()
        if k != node_id
    }

    node = Node(
        node_id,
        port,
        peers
    )

    asyncio.run(node.start())