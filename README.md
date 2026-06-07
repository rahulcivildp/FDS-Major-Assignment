# Fundamentals of Distributed Systems (FDS) Assignment

## Overview

This project implements a fault-tolerant distributed ledger system demonstrating core distributed systems concepts including:

- Bully Leader Election Algorithm
- Paxos Consensus Protocol
- PBFT (Practical Byzantine Fault Tolerance)
- RSA-based Message Authentication
- Heartbeat-based Failure Detection
- Persistent Transaction Logging
- Byzantine/Adversarial Node Simulation
- Chaos Engineering using Toxiproxy

The system consists of multiple distributed nodes that coordinate to maintain a consistent transaction ledger while tolerating node failures, network delays, and malicious behavior.

---

## Features

### 1. Leader Election (Bully Algorithm)

- Highest node ID becomes leader.
- Nodes monitor leader heartbeats.
- Automatic election on leader failure.
- Dynamic leader announcement using NEW_LEADER messages.

### 2. Paxos Consensus

Implemented phases:

1. PREPARE
2. PROMISE
3. ACCEPT
4. ACCEPTED

Ensures a transaction is committed only after majority agreement.

### 3. PBFT Consensus

Implemented phases:

1. PRE_PREPARE
2. PREPARE
3. COMMIT

Supports Byzantine fault tolerance and consensus despite malicious nodes.

### 4. RSA Digital Signatures

Every node possesses:

- Private Key
- Public Key

All inter-node messages are:

- Signed using RSA-2048
- Verified before processing

### 5. Heartbeat Monitoring

Leader periodically sends HEARTBEAT messages to followers.

Followers:
- Track heartbeat timestamps
- Detect failures
- Trigger elections when timeout occurs

### 6. Transaction Logging

Committed transactions are persisted to disk:

- ledger_1.json
- ledger_2.json
- ledger_3.json
- ledger_4.json
- ledger_5.json

### 7. Byzantine Node Simulation

An adversarial node is included to simulate Byzantine behavior.

### 8. Chaos Engineering

Toxiproxy is used to inject:
- Network latency
- Jitter
- Connection disruptions

---

## Project Structure

```text
FDS-assignment/
│
├── src/
│   ├── node.py
│   ├── client.py
│   ├── adversary.py
│   ├── config.py
│   ├── crypto_utils.py
│   ├── keys.py
│   ├── messages.py
│   └── transaction_log.py
│
├── keys/
├── tests/
│   └── chaos_test.sh
│
├── ledger_*.json
└── README.md
```

---

## Running the System

### Start Nodes

```bash
python src/node.py 1 5001
python src/node.py 2 5002
python src/node.py 3 5003
python src/node.py 4 5004
python src/node.py 5 5005
```

### Send Transaction

```bash
python src/client.py
```

---

## Leader Failure Demonstration

1. Start all nodes.
2. Allow leader election.
3. Stop current leader (Ctrl+C).
4. Remaining nodes detect timeout.
5. New leader is elected automatically.
6. Submit another transaction.
7. Consensus continues normally.

---

## Toxiproxy Setup

Communication proxies:

- 6001 -> 5001
- 6002 -> 5002
- 6003 -> 5003
- 6004 -> 5004
- 6005 -> 5005

Run chaos testing:

```bash
bash tests/chaos_test.sh node5_proxy
```

---

## Security

Implemented security mechanisms:

- RSA-2048 Key Pairs
- Digital Signatures
- Signature Verification
- Message Authentication
- Tamper Detection

---

## Consensus Modes

```python
CONSENSUS_MODE = "PAXOS"
```

or

```python
CONSENSUS_MODE = "PBFT"
```

---

## Technologies Used

- Python
- AsyncIO
- Cryptography
- JSON
- Docker
- Toxiproxy

---

## Learning Outcomes

- Distributed Coordination
- Consensus Algorithms
- Byzantine Fault Tolerance
- Failure Detection
- Secure Communication
- Fault Injection Testing

---

## Scribe

Tanmay Sarkar G25AI1050

Fundamentals of Distributed Systems

PGDDE Trimester III Major Assignment
