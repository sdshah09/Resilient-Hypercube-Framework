# Peer-to-Peer Publisher-Subscriber System

## Overview
This project implements a decentralized, fault-tolerant Publisher-Subscriber system using a hypercube topology. Nodes communicate with each other to publish, subscribe, and manage topics. The system supports failover, topic reclamation, and dynamic node addition and removal.

## Features
1. **Topic Creation**: Nodes can create topics and replicate them across neighbors for redundancy.
2. **Publishing**: Messages can be published to topics, and subscribers receive the messages in real time.
3. **Subscription**: Nodes can subscribe to topics and receive updates.
4. **Failover**: When a primary node fails, replicas take over as the new primary.
5. **Reclamation**: Failed nodes can rejoin and reclaim topics that belong to them.
6. **Dynamic Network**: Nodes can be added or removed dynamically, with real-time adjustments to the system.
7. **Heartbeat Mechanism**: Periodic heartbeats monitor node status and trigger failover processes.

---

## Installation

### Prerequisites
1. **Python 3.8 or higher**.
2. **Asyncio**: Used for asynchronous communication between nodes.

---

## Usage

### 1. **Starting a Node**
Each node in the system is started as a separate process. Use the following command to start a node:
```bash
python peer.py --node_id <node_id> --port <port>
```

- `--node_id`: The binary ID of the node (e.g., `000`).
- `--port`: The port on which the node will listen.

Example:
```bash
python peer.py --node_id 000 --port 5555
python peer.py --node_id 001 --port 5556
python peer.py --node_id 010 --port 5557
python peer.py --node_id 011 --port 5558
python peer.py --node_id 100 --port 5559
python peer.py --node_id 101 --port 5560
python peer.py --node_id 110 --port 5561
python peer.py --node_id 111 --port 5562
```

### 2. **Running the Test Script**
The `test.py` script automates the process of testing failover and topic reclamation. To run the test:
```bash
python test.py
```

---

## Test Functionality

### **Failover and Reclamation Test**
This test demonstrates the system's ability to handle node failures and reclamation:
1. **Create a Topic**:
   - A topic is created on a specific node.
2. **Publish Initial Messages**:
   - Messages are published to the topic, and subscribers receive updates.
3. **Simulate Node Failure**:
   - The primary node for the topic is killed, triggering failover to a replica.
4. **Verify Failover**:
   - New messages are published to the topic, now managed by the new primary.
5. **Rejoin Node**:
   - The failed node is restarted and reclaims the topic.
6. **Verify Reclamation**:
   - Messages are published through the reclaimed primary.

---

## Key Components

### **PeerNode**
The main class that implements the node's functionality:
- `create_topic`: Creates a topic and replicates it to neighbors.
- `publish`: Publishes messages to a topic.
- `subscribe`: Allows a node to subscribe to a topic.
- `handle_node_failure`: Handles failover when a node goes offline.
- `handle_rejoin`: Reclaims topics when a node rejoins the network.

### **PeerNodeInterface**
Provides an interface for interacting with nodes programmatically:
- `send_request`: Sends a command to a node and returns the response.
- `create_topic`: Sends a `create_topic` command.
- `publish`: Sends a `publish` command.
- `delete_topic`: Sends a `delete_topic` command.

### **Publisher**
Encapsulates publishing operations for a given topic.

### **Subscriber**
Handles subscribing and listening for messages on a topic.

---

## Example Workflow

### 1. **Create a Topic**
```python
await publisher.create_topic("sports")
```

### 2. **Publish a Message**
```python
await publisher.publish("sports", "Match Update 1")
```

### 3. **Subscribe to a Topic**
```python
await subscriber.subscribe_and_listen()
```

### 4. **Simulate Failover**
Simulate a node failure by killing its process:
```bash
kill <process_id>
```

### 5. **Rejoin Node**
Restart the failed node:
```bash
python peer.py --node_id <node_id> --port <port>
```

---

## Configuration

### Node Configuration
- Node IDs are represented as binary strings (e.g., `000`, `001`).
- Each node connects to its neighbors in a hypercube topology.
- Topics are hashed to determine the primary node.

### Heartbeat
- Nodes send periodic heartbeats to monitor the status of neighbors.
- If a heartbeat fails, the system initiates failover.

---

## File Structure
```
.
├── peer.py                     # Main node implementation
├── publisher.py                # Publisher class
├── subscriber.py               # Subscriber class
├── test.py                     # Test script for failover and reclamation
├── test_benchmark_analysis/
│   ├── peer_node_interface.py  # Interface for interacting with nodes
│   └── __init__.py             # Module initialization
├── README.md                   # Documentation
└── logs/                       # Logs for each node
```

---

## Logging
Each node logs its operations in a file named `peer_node_<port>.log`. Logs include:
- Topic creation and deletion.
- Message publishing.
- Node failure and rejoin events.
- Heartbeat activity.

---

## Future Improvements
1. **Scalability**: Extend support for larger networks with advanced routing.
2. **Security**: Implement authentication for node communication.
3. **Performance Metrics**: Add benchmarking tools for measuring throughput and latency.

---

## Contact
For questions or issues, feel free to contact:

- **Contributors**: [Shaswat Shah, Manikanta Teja Patamsett]

---

