import argparse
import asyncio
import json
import hashlib
from datetime import datetime
import platform
import subprocess


class PeerNode:
    def __init__(self, host='localhost', port=5555, node_id='000', total_bits=3):
        self.host = host
        self.port = port
        self.node_id = node_id  # Binary string identifier, e.g., '000'
        self.total_bits = total_bits  # Number of bits in node IDs
        self.total_nodes = 2 ** total_bits  # Total nodes in the hypercube
        self.topics = {}  # Store topics and messages
        self.subscribers = {}  # Store subscribers for each topic
        self.neighbors = self.get_neighbors()  # Neighboring nodes in the hypercube
        self.routing_table = {}  # Cache routes to other nodes
        self.running = True
        self.log_file = f"peer_node_{self.port}.log"  # Log events
        self.topic_replicas = {}
        self.offline_neighbors = set()
        self.topics_lock = asyncio.Lock()
        self.subscribers_lock = asyncio.Lock()
        self.is_primary = {}
        self.connection_pool = {}
        self.heartbeat_interval =5

    async def get_connection(self, host, port):
        key = f"{host}:{port}"
        if key not in self.connection_pool:
            reader, writer = await asyncio.open_connection(host, port)
            self.connection_pool[key] = (reader, writer)
        return self.connection_pool[key]

    async def close_connection_pool(self):
        for reader, writer in self.connection_pool.values():
            writer.close()
            await writer.wait_closed()

    async def start(self):
        server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        self.log_event(f"Peer node {self.node_id} started on {self.host}:{self.port}")

        # Start periodic tasks
        asyncio.create_task(self.send_heartbeat())  # Start sending heartbeats
        asyncio.create_task(self.check_topic_promotion())  # Check for topic promotion/demotion

        async with server:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                server.close()
                await server.wait_closed()
                self.log_event(f"Peer node {self.node_id} shut down.")
    async def shutdown(self):
        self.running = False
        await self.close_connection_pool()
        self.log_event("Node shutdown gracefully.")

    async def handle_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        self.log_event(f"Connected by {addr}")

        while True:
            data = await reader.read(1024)
            if not data:
                break
            message = data.decode()
            self.log_event(f"Received message from {addr}: {message}")

            # Process the request and send a response
            response = await self.process_request(message)
            writer.write(response.encode())
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def process_request(self, message):
        try:
            request = json.loads(message)
            # print("Request in process_request:", request)
            command = request.get('command')
            if command == "create_topic":
                topic = request.get('topic')
                return await self.create_topic(topic)
            elif command == "delete_topic":
                topic = request.get('topic')
                return await self.delete_topic(topic)
            elif command == "publish":
                topic = request.get('topic')
                msg = request.get('message')
                return await self.publish(topic, msg)
            elif command == "subscribe":
                topic = request.get('topic')
                subscriber_host = request.get('subscriber_host')
                subscriber_port = request.get('subscriber_port')
                subscriber_node_id = request.get('subscriber_node_id')
                return await self.subscribe(topic, subscriber_host, subscriber_port, subscriber_node_id)
            elif command == "subscribe_to_peer":
                topic = request.get('topic')
                subscriber_host = request.get('subscriber_host')
                subscriber_port = request.get('subscriber_port')
                subscriber_node_id = request.get('subscriber_node_id')
                return await self.handle_subscription(topic, subscriber_host, subscriber_port, subscriber_node_id)
            elif command == "pull":
                topic = request.get('topic')
                return await self.pull(topic)
            elif command == "receive_message":
                topic = request.get('topic')
                message = request.get('message')
                self.log_event(f"Received message on topic '{topic}': {message}")
                return json.dumps({"status": "success", "message": f"Message '{message}' received on topic '{topic}'"})
            elif command == "route":
                target_node_id = request.get('target_node_id')
                return await self.route_message(target_node_id, message)
            elif command == "create_topic_replica":
                topic = request.get('topic')
                origin_node_id = request.get('origin_node_id')
                return await self.create_topic_replica(topic, origin_node_id)
            elif command == "replicate_message":
                topic = request.get('topic')
                message = request.get('message')
                return await self.replicate_message(topic, message)
            elif command == "recover_topics":
                node_id = request.get('node_id')
                return await self.handle_recover_topics(node_id)
            elif command == "heartbeat":
                return json.dumps({"status": "success", "message": "Heartbeat received"})
            else:
                return json.dumps({"status": "error", "message": "Unknown command"})
        except json.JSONDecodeError:
            return json.dumps({"status": "error", "message": "Invalid JSON format"})

    def hash_topic(self, topic):
        hash_value = int(hashlib.sha256(topic.encode()).hexdigest(), 16)
        node_id_int = hash_value % self.total_nodes
        node_id = format(node_id_int, f'0{self.total_bits}b')
        return node_id

    def get_neighbors(self):
        neighbors = []
        node_int = int(self.node_id, 2)
        for i in range(self.total_bits):
            neighbor_int = node_int ^ (1 << i)
            neighbor_id = format(neighbor_int, f'0{self.total_bits}b')
            neighbors.append(neighbor_id)
        return neighbors

    async def create_topic(self, topic):
        topic_node_id = self.hash_topic(topic)
        if topic_node_id == self.node_id:
            if topic in self.topics:
                return json.dumps({"status": "error", "message": "Topic already exists"})
            self.topics[topic] = []
            self.subscribers[topic] = set()
            self.log_event(f"Created topic '{topic}'")
            self.topic_replicas[topic] = set()
            await self.replicate_topic_to_neighbors(topic)
            return json.dumps({"status": "success", "message": f"Topic '{topic}' created"})
        else:
            # Route the request to the responsible node
            return await self.forward_request(topic_node_id, {"command": "create_topic", "topic": topic})

    async def replicate_topic_to_neighbors(self, topic):
        successful_replicas = []
        for neighbor_id in self.neighbors:
            if await self.send_replication_request(topic, neighbor_id):
                successful_replicas.append(neighbor_id)
        self.topic_replicas[topic] = set(successful_replicas)

    async def send_replication_request(self, topic, neighbor_id):
        host, port = self.get_node_address(neighbor_id)
        try:
            reader, writer = await self.get_connection(host, port)
            replication_request = json.dumps({
                "command": "create_topic_replica",
                "topic": topic,
                "origin_node_id": self.node_id
            })
            writer.write(replication_request.encode())
            await writer.drain()
            response = await reader.read(1024)
            return json.loads(response).get("status") == "success"
        except Exception:
            return False

    async def create_topic_replica(self, topic, origin_node_id):
        if topic in self.topics:
            return json.dumps({"status": "error", "message": "Replica already exists"})
        self.topics[topic] = []
        self.subscribers[topic] = set()
        self.log_event(f"Created replica of topic '{topic}' from node {origin_node_id}")
        return json.dumps({"status": "success", "message": f"Replica of topic '{topic}' created"})

    async def replicate_message(self, topic, message):
        if topic in self.topics:
            self.topics[topic].append(message)
            self.log_event(f"Replicated message on topic '{topic}': {message}")

            # Forward the message to local subscribers
            await self.forward_message_to_subscribers(topic, message)

            return json.dumps({"status": "success", "message": "Message replicated"})
        else:
            return json.dumps({"status": "error", "message": "Topic not found in replica"})

    async def handle_recover_topics(self, node_id):
        # Return topics that belong to the requesting node
        topics_to_recover = []
        for topic in self.topics:
            if self.hash_topic(topic) == node_id:
                topics_to_recover.append(topic)
        # Send back the topics and messages
        response = {
            "status": "success",
            "topics": topics_to_recover,
            "messages": {topic: self.topics[topic] for topic in topics_to_recover}
        }
        return json.dumps(response)

    async def delete_topic(self, topic):
        topic_node_id = self.hash_topic(topic)
        if topic_node_id == self.node_id:
            if topic not in self.topics:
                return json.dumps({"status": "error", "message": "Topic does not exist"})
            del self.topics[topic]
            del self.subscribers[topic]
            self.log_event(f"Deleted topic '{topic}'")
            return json.dumps({"status": "success", "message": f"Topic '{topic}' deleted"})
        else:
            # Route the request to the responsible node
            return await self.forward_request(topic_node_id, {"command": "delete_topic", "topic": topic})

    async def publish(self, topic, message):
        topic_node_id = self.hash_topic(topic)
        if topic_node_id == self.node_id or self.is_primary.get(topic, False):
            if topic in self.topics:
                self.topics[topic].append(message)
                self.log_event(f"Published message on topic '{topic}': {message}")

                # Forward the message to local subscribers
                await self.forward_message_to_subscribers(topic, message)

                # Propagate the message to replicas
                await self.propagate_message_to_replicas(topic, message)

                return json.dumps({"status": "success", "message": f"Message published on topic '{topic}'"})
            else:
                return json.dumps({"status": "error", "message": "Topic does not exist"})
        else:
            # Route the request to the responsible node or a replica acting as primary
            response = await self.forward_request(topic_node_id, {"command": "publish", "topic": topic, "message": message})
            if json.loads(response).get("status") == "error":
                replicas = self.topic_replicas.get(topic, [])
                for replica_id in replicas:
                    if replica_id != self.node_id:
                        response = await self.route_message(replica_id, json.dumps({"command": "publish", "topic": topic, "message": message}))
                        if json.loads(response).get("status") == "success":
                            return response
            return json.dumps({"status": "error", "message": f"Failed to route message for topic '{topic}'"})

    async def propagate_message_to_replicas(self, topic, message):
        replicas = self.topic_replicas.get(topic, [])
        for replica_id in replicas:
            replica_host, replica_port = self.get_node_address(replica_id)
            try:
                reader, writer = await asyncio.open_connection(replica_host, replica_port)
                replication_request = json.dumps({
                    "command": "replicate_message",
                    "topic": topic,
                    "message": message
                })
                writer.write(replication_request.encode())
                await writer.drain()
                response = await reader.read(1024)
                response_data = json.loads(response.decode())

                if response_data.get("status") == "success":
                    self.log_event(f"Propagated message to replica {replica_id}")
                else:
                    self.log_event(
                        f"Failed to propagate message to replica {replica_id}: {response_data.get('message')}")

                writer.close()
                await writer.wait_closed()
            except Exception as e:
                self.log_event(f"Error propagating message to replica {replica_id}: {e}")

    async def subscribe(self, topic, subscriber_host, subscriber_port, subscriber_node_id):
        topic_node_id = self.hash_topic(topic)
        if topic_node_id == self.node_id or self.is_primary.get(topic, False):
            if topic not in self.topics:
                self.topics[topic] = []
                self.subscribers[topic] = set()
            self.subscribers[topic].add((subscriber_host, subscriber_port, subscriber_node_id))
            self.log_event(f"Subscriber {subscriber_node_id} subscribed to topic '{topic}'")
            return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}'"})
        else:
            # Forward the subscription request to the responsible node or primary replica
            response = await self.forward_request(topic_node_id, {
                "command": "subscribe",
                "topic": topic,
                "subscriber_host": subscriber_host,
                "subscriber_port": subscriber_port,
                "subscriber_node_id": subscriber_node_id
            })
            if json.loads(response).get("status") == "error":
                replicas = self.topic_replicas.get(topic, [])
                for replica_id in replicas:
                    if replica_id != self.node_id:
                        response = await self.route_message(replica_id, json.dumps({
                            "command": "subscribe",
                            "topic": topic,
                            "subscriber_host": subscriber_host,
                            "subscriber_port": subscriber_port,
                            "subscriber_node_id": subscriber_node_id
                        }))
                        if json.loads(response).get("status") == "success":
                            return response
            return response

    async def handle_subscription(self, topic, subscriber_host, subscriber_port, subscriber_node_id):
        if topic in self.topics:
            self.subscribers[topic].add((subscriber_host, subscriber_port, subscriber_node_id))
            self.log_event(f"Peer {subscriber_node_id} subscribed to topic '{topic}' on replica")
            return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}' on replica"})
        else:
            return json.dumps({"status": "error", "message": "Topic not found"})

    async def pull(self, topic):
        topic_node_id = self.hash_topic(topic)
        if topic_node_id == self.node_id or self.is_primary.get(topic, False):
            if topic in self.topics:
                messages = self.topics[topic]
                if messages:
                    self.log_event(f"Pulled messages from topic '{topic}': {messages}")
                    self.topics[topic] = []  # Clear messages after pulling
                    return json.dumps({"status": "success", "messages": messages})
                else:
                    return json.dumps({"status": "error", "message": "No messages to pull"})
            else:
                return json.dumps({"status": "error", "message": "Topic not found"})
        else:
            # Route the pull request to the topic's node
            return await self.forward_request(topic_node_id, {"command": "pull", "topic": topic})

    async def forward_message_to_subscribers(self, topic, message):
        subscribers = self.subscribers.get(topic, [])
        for subscriber_host, subscriber_port, subscriber_node_id in subscribers:
            try:
                reader, writer = await asyncio.open_connection(subscriber_host, subscriber_port)
                publish_request = json.dumps({
                    "command": "receive_message",
                    "topic": topic,
                    "message": message
                })
                writer.write(publish_request.encode())
                await writer.drain()
                response = await reader.read(1024)
                self.log_event(f"Sent message to subscriber {subscriber_node_id}: {response.decode()}")
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                self.log_event(f"Error sending message to subscriber {subscriber_node_id}: {e}")

    async def forward_request(self, target_node_id, request):
        # Try routing to target node
        response = await self.route_message(target_node_id, json.dumps(request))
        response_data = json.loads(response)
        if response_data.get("status") == "error":
            # If failed, try replicas
            replicas = self.topic_replicas.get(request.get('topic'), [])
            for replica_id in replicas:
                if replica_id != self.node_id:
                    response = await self.route_message(replica_id, json.dumps(request))
                    response_data = json.loads(response)
                    if response_data.get("status") != "error":
                        return response
        return response

    async def promote_to_primary(self, topic):
        if topic not in self.topics:
            return json.dumps({"status": "error", "message": f"Topic '{topic}' not replicated here."})
        # Check if already primary
        if self.is_primary.get(topic, False):
            return json.dumps({"status": "success", "message": f"Already primary for topic '{topic}'"})
        # Promote to primary
        if self.hash_topic(topic) != self.node_id:
            return json.dumps({"status": "error", "message": "Not authorized to promote topic"})
        self.is_primary[topic] = True
        self.log_event(f"Promoted to primary for topic '{topic}'")
        return json.dumps({"status": "success", "message": f"Promoted to primary for topic '{topic}'"})

    async def recover_topics(self):
        for neighbor_id in self.neighbors:
            neighbor_host, neighbor_port = self.get_node_address(neighbor_id)
            try:
                reader, writer = await asyncio.open_connection(neighbor_host, neighbor_port)
                recovery_request = json.dumps({
                    "command": "recover_topics",
                    "node_id": self.node_id
                })
                writer.write(recovery_request.encode())
                await writer.drain()
                response = await reader.read(1024)
                response_data = json.loads(response.decode())
                for topic in response_data.get("topics", []):
                    if topic not in self.topics:
                        self.topics[topic] = []
                        self.subscribers[topic] = set()
                    self.topics[topic].extend(response_data.get("messages", []))
                    self.log_event(f"Recovered topic '{topic}' from neighbor {neighbor_id}")
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                self.log_event(f"Error recovering topics from neighbor {neighbor_id}: {e}")
        # Set primary status for topics
        for topic in self.topics:
            if self.hash_topic(topic) == self.node_id:
                self.is_primary[topic] = True
                self.log_event(f"Resumed primary status for topic '{topic}'")


    async def send_heartbeat(self):
        """
        Send heartbeat messages to neighbors using ncat/netcat or the connection pool.
        Updates the node status and offline_neighbors list.
        """
        while self.running:
            for neighbor_id in self.neighbors:
                host, port = self.get_node_address(neighbor_id)
                try:
                    # Attempt to reuse or create a connection
                    reader, writer = await self.get_connection(host, port)

                    # Send a heartbeat request
                    heartbeat_request = json.dumps({
                        "command": "heartbeat",
                        "node_id": self.node_id
                    })
                    writer.write(heartbeat_request.encode())
                    await writer.drain()

                    # Read response to ensure the node is online
                    response = await reader.read(1024)
                    response_data = json.loads(response.decode())

                    if response_data.get("status") == "success":
                        # Mark neighbor as online
                        if neighbor_id in self.offline_neighbors:
                            self.offline_neighbors.remove(neighbor_id)
                            self.log_event(f"Node {neighbor_id} came back online.")
                    else:
                        # Handle unexpected response
                        self.log_event(f"Unexpected response from {neighbor_id}: {response_data}")

                except (ConnectionError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                    # Mark neighbor as offline
                    if neighbor_id not in self.offline_neighbors:
                        self.offline_neighbors.add(neighbor_id)
                        self.log_event(f"Node {neighbor_id} went offline: {e}")

                except Exception as e:
                    self.log_event(f"Error during heartbeat check for {neighbor_id}: {e}")

            # Wait for the next heartbeat interval
            await asyncio.sleep(self.heartbeat_interval)

    def get_ping_command(self):
        """Return the appropriate command for ncat/netcat based on the OS."""
        system = platform.system()
        if system == "Windows":
            # Use ncat on Windows
            return "ncat -zv {host} {port}"
        elif system in ("Linux", "Darwin"):
            # Use netcat/nc on Linux/MacOS
            return "nc -zv {host} {port}"
        else:
            raise RuntimeError(f"Unsupported operating system: {system}")
    async def check_topic_promotion(self):
        for topic in self.topics:
            primary_node_id = self.hash_topic(topic)
            if primary_node_id == self.node_id:
                self.is_primary[topic] = True
            elif primary_node_id not in self.offline_neighbors:
                if self.is_primary.get(topic, False):
                    self.is_primary[topic] = False
                    self.log_event(f"Demoted from primary for topic '{topic}'")

    async def route_message(self, target_node_id, message):
        if target_node_id == self.node_id:
            # Directly process the message
            return await self.process_request(message)
        else:
            next_hop_id = self.find_next_hop(target_node_id)
            if next_hop_id is None:
                self.log_event(f"No available next hop to reach node {target_node_id}")
                return json.dumps({"status": "error", "message": f"Cannot reach node {target_node_id}"})
            if next_hop_id in self.offline_neighbors:
                self.offline_neighbors.remove(next_hop_id)
                self.log_event(f"Neighbor {next_hop_id} is back online")
            neighbor_host, neighbor_port = self.get_node_address(next_hop_id)
            print(f"Neighbor Host and Port in route_message is: {neighbor_host} {neighbor_port}")
            try:
                reader, writer = await asyncio.open_connection(neighbor_host, neighbor_port)
                writer.write(message.encode())
                await writer.drain()
                response = await reader.read(1024)
                writer.close()
                await writer.wait_closed()
                return response.decode() if response else json.dumps(
                    {"status": "error", "message": "No response from forwarded node"})
            except Exception as e:
                self.log_event(f"Failed to route message to {next_hop_id}: {e}")
                self.offline_neighbors.add(next_hop_id)
                return json.dumps({"status": "error", "message": f"Failed to route message to {target_node_id}"})

    def find_next_hop(self, target_node_id):
        current_int = int(self.node_id, 2)
        target_int = int(target_node_id, 2)
        differing_bits = current_int ^ target_int
        for i in range(self.total_bits - 1, -1, -1):
            if differing_bits & (1 << i):
                next_hop_int = current_int ^ (1 << i)
                next_hop_id = format(next_hop_int, f'0{self.total_bits}b')
                if next_hop_id not in self.offline_neighbors:
                    return next_hop_id
        return None

    def get_node_address(self, node_id):
        # Map node IDs to host and port numbers
        base_port = 5555
        node_int = int(node_id, 2)
        node_port = base_port + node_int
        return self.host, node_port

    def log_event(self, event):
        if "heartbeat" not in event:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.log_file, "a") as log:
                log.write(f"[{timestamp}] {event}\n")
            print(f"[LOG {self.node_id}] {event}")


def main():
    parser = argparse.ArgumentParser(description="Decentralized P2P Publisher-Subscriber Peer Node")
    parser.add_argument('--host', type=str, default='localhost', help='Host address of the peer node')
    parser.add_argument('--port', type=int, default=5555, help='Port to use for the peer node')
    parser.add_argument('--node_id', type=str, required=True, help='Binary Node ID (e.g., 000 for node 0)')
    parser.add_argument('--total_bits', type=int, default=3, help='Total bits in node IDs (e.g., 3 bits for 8 nodes)')
    args = parser.parse_args()

    node = PeerNode(args.host, args.port, args.node_id, args.total_bits)
    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        print(f"Shutting down peer node {args.node_id}.")
        asyncio.run(node.shutdown())



if __name__ == "__main__":
    main()
