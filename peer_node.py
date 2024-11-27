import argparse
import asyncio
import json
from datetime import datetime
import subprocess
from PeerNodeHelper import PeerNodeHelper

class PeerNode:
    def __init__(self, host='localhost', port=5555, node_id='000', total_bits=3):
        self.helper = PeerNodeHelper(host,node_id,port,total_bits)
        self.host = host
        self.port = port
        self.node_id = node_id  # Binary string identifier, e.g., '000'
        self.total_bits = total_bits  # Number of bits in node IDs
        self.total_nodes = 2 ** total_bits  # Total nodes in the hypercube
        self.topics = {}  # Store topics and messages
        self.subscribers = {}  # Store subscribers for each topic
        self.neighbors = self.helper.get_neighbors()  # Neighboring nodes in the hypercube
        self.running = True
        self.log_file = f"peer_node_{self.port}.log"  # Log events
        self.offline_neighbors = set()
        self.replicate_tables = {}
        self.heartbeat_interval = 4
        
    async def start(self):
        server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        self.helper.log_event(f"Peer node {self.node_id} started on {self.host}:{self.port}")
        asyncio.create_task(self.send_heartbeat())
        asyncio.create_task(self.handle_node_rejoin())  # Sync topics if node is recovering
        async with server:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                server.close()
                await server.wait_closed()
                self.helper.log_event(f"Peer node {self.node_id} shut down.")
                
    async def send_heartbeat(self):
        """Send heartbeat using ncat or netcat based on the operating system."""
        command = self.helper.get_ping_command()
        while self.running:
            for neighbor_id in self.neighbors:
                host, port = self.helper.get_node_address(neighbor_id)
                try:
                    # Use the appropriate command for the platform
                    result = subprocess.run(command.format(host=host, port=port).split(),
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    if result.returncode == 0:
                        # Node is online
                        if neighbor_id in self.offline_neighbors:
                            self.offline_neighbors.remove(neighbor_id)
                            self.helper.log_event(f"Node {neighbor_id} came back online.")
                    else:
                        # Node is offline or connection failed
                        if neighbor_id not in self.offline_neighbors:
                            self.offline_neighbors.add(neighbor_id)
                            self.helper.log_event(f"Node {neighbor_id} went offline.")
                            self.handle_failure_detection(neighbor_id)  # Handle failure
                except Exception as e:
                    self.helper.log_event(f"Error during heartbeat check for {neighbor_id}: {e}")
            await asyncio.sleep(self.heartbeat_interval)


    async def handle_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        # self.helper.log_event(f"Connected by {addr}")

        while True:
            data = await reader.read(1024)
            if not data:
                break
            message = data.decode()
            self.helper.log_event(f"Received message from {addr}: {message}")

            # Process the request and send a response
            response = await self.process_request(message)
            writer.write(response.encode())
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def process_request(self, message):
        try:
            request = json.loads(message)
            print("Request in process Request it is: ",request)
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
                return await self.subscribe(topic)
            elif command == "subscribe_to_peer":
                topic = request.get('topic')
                subscriber_port = request.get('subscriber_port')
                subscriber_host = request.get('subscriber_host')
                subscriber_node_id = request.get('subscriber_node_id')
                return await self.handle_subscription(topic, subscriber_host,subscriber_port, subscriber_node_id)
            elif command == "pull":
                topic = request.get('topic')
                return await self.pull(topic)
            elif command == "receive_message":
                topic = request.get('topic')
                message = request.get('message')
                self.helper.log_event(f"Received message on topic '{topic}': {message}")
                return json.dumps({"status": "success", "message": f"Message '{message}' received on topic '{topic}'"})
            elif command == "route":
                target_node_id = request.get('target_node_id')
                visited_nodes = set(request.get('visited_nodes', []))
                ttl = request.get('ttl', 10)
                return await self.route_message(target_node_id, message, visited_nodes, ttl)
            elif command == "replicate_topic":
                topic = request.get('topic')
                primary_node_id = request.get("primary_node_id")
                primary_node_neighbors =request.get('neighbors') # This will contains the list of neighbors of the current node
                return await self.replicate_topic(topic,primary_node_id,primary_node_neighbors)
            elif command == "update_replication_status":
                topic = request.get('topic')
                primary_node_id = request.get('primary_node_id')
                return await self.update_replication_status(topic,primary_node_id)
            else:
                return json.dumps({"status": "error", "message": "Unknown command"})
        except json.JSONDecodeError:
            return json.dumps({"status": "error", "message": "Invalid JSON format"})


    async def create_topic(self, topic):
        topic_node_id = self.helper.hash_topic(topic)
        if topic_node_id == self.node_id:
            if topic in self.topics:
                return json.dumps({"status": "error", "message": "Topic already exists"})
            self.topics[topic] = []
            self.subscribers[topic] = set()
            self.replicate_tables[topic] = {"primary":self.node_id,"replicas":set()}
            # Create replicas
            for neighbor_id in self.neighbors:
                await self.forward_request(neighbor_id, {
                    "command": "replicate_topic",
                    "topic": topic,
                    "primary_node_id":self.node_id,
                    "neighbors":self.neighbors
                })
            return json.dumps({"status": "success", "message": f"Topic '{topic}' created with replicas"})
        else:
            return await self.forward_request(topic_node_id, {"command": "create_topic", "topic": topic})

    async def replicate_topic(self,topic,primary_node_id,primary_node_neighbors):
        if topic not in self.replicate_tables:
            self.replicate_tables[topic] = {"primary": primary_node_id, "replicas": set(primary_node_neighbors)-{primary_node_id}}
        self.helper.log_event(f"Replicated topic '{topic}' on node {self.node_id}")
        return json.dumps({"status": "success", "message": f"Topic '{topic}' replicated"})
    
    async def handle_node_rejoin(self):
        """
        Handles recovery when a node rejoins the network.
        Fetches topics it should own from replicas and updates the replication table.
        """
        recovered_topics = []

        for topic, data in self.replicate_tables.items():
            topic_node_id = self.helper.hash_topic(topic)
            if topic_node_id == self.node_id:
                # This node should be the primary holder
                if data["primary"] != self.node_id:
                    # Update primary to self and notify replicas
                    data["primary"] = self.node_id
                    self.helper.log_event(f"Reclaimed primary ownership for topic '{topic}'")
                    for replica_id in data["replicas"]:
                        if replica_id != self.node_id:
                            await self.forward_request(replica_id, {
                                "command": "update_replication_status",
                                "topic": topic,
                                "primary_node_id": self.node_id
                            })
                # Attempt to recover data
                topic_data = await self.pull_topic_from_replicas(topic)
                if topic_data:
                    self.topics[topic] = topic_data
                    recovered_topics.append(topic)
                    self.helper.log_event(f"Recovered topic '{topic}' with data: {topic_data}")
                else:
                    self.helper.log_event(f"No data found for topic '{topic}' during recovery")

            self.helper.log_event(f"Recovered topics upon rejoin: {recovered_topics}")
            
    async def pull_topic_from_replicas(self, topic):
        """
        Pulls the latest topic data from replica nodes.
        """
        for node_id in self.replicate_tables.get(topic, []):
            if node_id != self.node_id:
                host, port = self.helper.get_node_address(node_id)
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    request = json.dumps({"command": "pull", "topic": topic})
                    writer.write(request.encode())
                    await writer.drain()
                    response = await reader.read(1024)
                    writer.close()
                    await writer.wait_closed()
                    data = json.loads(response.decode())
                    if data.get("status") == "success":
                        return data.get("messages", [])
                except Exception as e:
                    self.helper.log_event(f"Failed to fetch topic '{topic}' from {node_id}: {e}")
        return []
    
    async def update_replication_status(self, topic, primary_node_id):
        """
        Updates the replication status when a primary node changes.
        """
        if topic in self.replicate_tables:
            self.replicate_tables[topic]["primary"] = primary_node_id
            self.helper.log_event(f"Updated replication status for topic '{topic}' with new primary {primary_node_id}")
        else:
            self.replicate_tables[topic] = {"primary": primary_node_id, "replicas": set()}
        self.helper.log_event(f"Replicate tables in update replication status is: {self.replicate_tables}")
        return json.dumps({"status": "success", "message": "Replication status updated"})

    def handle_failure_detection(self, failed_node_id):
        """
        Handles a node failure by updating the replication table and electing a new primary.
        """
        print("Inside handle failure detection")
        print("Replica table is: ",self.replicate_tables)
        for topic, data in self.replicate_tables.items():
            print(f"Topic and data in self.replicate table are: {topic} and {data}")
            if failed_node_id == data.get("primary"):
                # Primary has failed; elect a new primary
                replicas = data["replicas"]
                print("Replicas if failed node is primary are: ",replicas)
                if replicas:
                    new_primary = sorted(replicas)[0]  # Elect the lowest-ID node as the new primary
                    data["primary"] = new_primary
                    replicas.remove(new_primary)  # Remove the new primary from replicas
                    self.helper.log_event(f"New primary for topic '{topic}' is {new_primary}")
                    # Notify replicas of the new primary
                    for replica in replicas:
                        asyncio.create_task(self.forward_request(replica, {
                            "command": "update_replication_status",
                            "topic": topic,
                            "primary_node_id": new_primary
                        }))
                else:
                    # No replicas available; topic is lost
                    self.helper.log_event(f"Topic '{topic}' is lost; no replicas available")
                    del self.replicate_tables[topic]
            elif failed_node_id in data["replicas"]:
                # A replica has failed; just update the replica set
                data["replicas"].remove(failed_node_id)
                self.helper.log_event(f"Removed node {failed_node_id} from replicas for topic '{topic}'")

    async def delete_topic(self, topic):
        topic_node_id = self.helper.hash_topic(topic)
        if topic_node_id == self.node_id:
            if topic not in self.topics:
                return json.dumps({"status": "error", "message": "Topic does not exist"})
            del self.topics[topic]
            del self.subscribers[topic]
            del self.replicate_tables[topic]
            self.helper.log_event(f"Deleted topic '{topic}'")
            return json.dumps({"status": "success", "message": f"Topic '{topic}' deleted"})
        else:
            # Route the request to the responsible node
            return await self.forward_request(topic_node_id, {"command": "delete_topic", "topic": topic})

    async def publish(self, topic, message):
        """
        Publishes a message to a topic. Handles primary node failure by routing to replicas.
        """
        # Check if the topic exists in replicate_tables
        if topic in self.replicate_tables:
            primary_node_id = self.replicate_tables[topic]["primary"]
            if primary_node_id == self.node_id:
                # Publish locally
                if topic in self.topics:
                    self.topics[topic].append(message)
                    self.helper.log_event(f"Published message on topic '{topic}': {message}")
                    await self.forward_message_to_subscribers(topic, message)
                    return json.dumps({"status": "success", "message": f"Message published on topic '{topic}'"})
                else:
                    return json.dumps({"status": "error", "message": "Topic does not exist"})
            else:
                # Forward the publish request to the primary
                response = await self.forward_request(primary_node_id, {
                    "command": "publish",
                    "topic": topic,
                    "message": message
                })
                if "error" in response:
                    # If primary fails, try routing to a replica
                    self.helper.log_event(f"Primary node {primary_node_id} failed for topic '{topic}'. Trying replicas.")
                    return await self.route_to_replica_publish(topic, message)
                return response
        else:
            # If topic is not in replicate_tables, hash to find the responsible node
            topic_node_id = self.helper.hash_topic(topic)
            if topic_node_id == self.node_id:
                # Create the topic locally
                self.topics[topic] = [message]
                self.subscribers[topic] = set()
                self.helper.log_event(f"Created topic '{topic}' and published message locally: {message}")
                return json.dumps({"status": "success", "message": f"Topic '{topic}' created and message published"})
            else:
                # Forward the request to the hashed node
                return await self.forward_request(topic_node_id, {
                    "command": "publish",
                    "topic": topic,
                    "message": message
                })
                
    async def route_to_replica_publish(self, topic, message):
        """
        Attempts to publish a message to one of the replicas if the primary is unavailable.
        """
        if topic in self.replicate_tables:
            replicas = self.replicate_tables[topic]["replicas"]
            for replica_id in replicas:
                host, port = self.helper.get_node_address(replica_id)
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    request = json.dumps({"command": "publish", "topic": topic, "message": message})
                    writer.write(request.encode())
                    await writer.drain()
                    response = await reader.read(1024)
                    writer.close()
                    await writer.wait_closed()
                    if "success" in response:
                        return response  # Successful publish to a replica
                except Exception as e:
                    self.helper.log_event(f"Failed to publish to replica {replica_id}: {e}")
        return json.dumps({"status": "error", "message": "Failed to publish to primary or any replicas"})

    async def subscribe(self, topic):
        """
        Subscribes to a topic. Handles cases where the primary node is unavailable.
        """
        # Check if the topic is known
        if topic in self.replicate_tables:
            primary_node_id = self.replicate_tables[topic]["primary"]
            if primary_node_id == self.node_id:
                # Topic is local to this node
                if topic not in self.topics:
                    self.topics[topic] = []
                    self.subscribers[topic] = set()
                self.subscribers[topic].add((self.host, self.port, self.node_id))
                self.helper.log_event(f"Local subscription to topic '{topic}' by {self.node_id}")
                return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}'"})
            else:
                try:
                    # Forward the subscription request to the primary
                    return await self.forward_request(primary_node_id, {
                        "command": "subscribe_to_peer",
                        "topic": topic,
                        "subscriber_host": self.host,
                        "subscriber_port": self.port,
                        "subscriber_node_id": self.node_id
                    })
                except Exception as e:
                    # Primary is unreachable, reroute to a replica
                    self.helper.log_event(f"Primary node {primary_node_id} for topic '{topic}' is unreachable. Trying replicas.")
                    return await self.route_to_replica_subscribe(topic)
        else:
            # Topic is not known; hash to find the responsible node
            topic_node_id = self.helper.hash_topic(topic)
            if topic_node_id == self.node_id:
                # Create the topic locally if it doesn't exist
                self.topics[topic] = []
                self.subscribers[topic] = set()
                self.subscribers[topic].add((self.host, self.port, self.node_id))
                self.helper.log_event(f"Created and subscribed to topic '{topic}' locally by {self.node_id}")
                return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}'"})
            else:
                # Forward the request to the hashed node
                return await self.forward_request(topic_node_id, {
                    "command": "subscribe_to_peer",
                    "topic": topic,
                    "subscriber_host": self.host,
                    "subscriber_port": self.port,
                    "subscriber_node_id": self.node_id
                })
    async def route_to_replica_subscribe(self, topic):
        """
        Attempts to subscribe to a replica if the primary is unavailable.
        """
        if topic in self.replicate_tables:
            replicas = self.replicate_tables[topic]["replicas"]
            for replica_id in replicas:
                host, port = self.helper.get_node_address(replica_id)
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    request = json.dumps({
                        "command": "subscribe_to_peer",
                        "topic": topic,
                        "subscriber_host": self.host,
                        "subscriber_port": self.port,
                        "subscriber_node_id": self.node_id
                    })
                    writer.write(request.encode())
                    await writer.drain()
                    response = await reader.read(1024)
                    writer.close()
                    await writer.wait_closed()
                    data = json.loads(response.decode())
                    if data.get("status") == "success":
                        return response  # Successful subscription to a replica
                except Exception as e:
                    self.helper.log_event(f"Failed to subscribe to replica {replica_id} for topic '{topic}': {e}")
        return json.dumps({"status": "error", "message": "Failed to subscribe to primary or any replicas"})


    async def handle_subscription(self, topic,subscriber_host, subscriber_port, subscriber_node_id):
        if topic in self.topics:
            self.subscribers[topic].add((subscriber_host, subscriber_port, subscriber_node_id))
            self.helper.log_event(f"Peer {subscriber_node_id} subscribed to topic '{topic}'")
            return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}'"})
        else:
            return json.dumps({"status": "error", "message": "Topic not found"})

    async def pull(self, topic):
        """
        Pulls messages from the primary node or a replica if the primary is unavailable.
        """
        # Check if the topic is in the replicate table
        if topic in self.replicate_tables:
            primary_node_id = self.replicate_tables[topic]["primary"]
            
            if primary_node_id == self.node_id:
                # This node is the primary
                if topic in self.topics:
                    messages = self.topics[topic]
                    if messages:
                        self.helper.log_event(f"Pulled messages from topic '{topic}': {messages}")
                        self.topics[topic] = []  # Clear messages after pulling
                        return json.dumps({"status": "success", "messages": messages})
                    else:
                        return json.dumps({"status": "error", "message": "No messages to pull"})
                else:
                    return json.dumps({"status": "error", "message": "Topic not found"})
            else:
                # Attempt to pull from the primary node
                try:
                    response = await self.forward_request(primary_node_id, {"command": "pull", "topic": topic})
                    data = json.loads(response)
                    if "success" in data.get("status", "").lower():
                        return response
                except Exception as e:
                    self.helper.log_event(f"Failed to pull messages from primary node {primary_node_id} for topic '{topic}': {e}")
                # If the primary fails, pull from replicas
                self.helper.log_event(f"Primary node {primary_node_id} for topic '{topic}' is unreachable. Trying replicas.")
                return await self.pull_from_replicas(topic)
        else:
            # Hash to find the responsible node if topic is unknown
            topic_node_id = self.helper.hash_topic(topic)
            if topic_node_id == self.node_id:
                # This node is responsible but the topic doesn't exist
                if topic in self.topics:
                    messages = self.topics[topic]
                    if messages:
                        self.helper.log_event(f"Pulled messages from topic '{topic}': {messages}")
                        self.topics[topic] = []  # Clear messages after pulling
                        return json.dumps({"status": "success", "messages": messages})
                    else:
                        return json.dumps({"status": "error", "message": "No messages to pull"})
                else:
                    return json.dumps({"status": "error", "message": "Topic not found"})
            else:
                # Forward the request to the hashed node
                try:
                    return await self.forward_request(topic_node_id, {"command": "pull", "topic": topic})
                except Exception as e:
                    self.helper.log_event(f"Failed to pull messages from hashed node {topic_node_id} for topic '{topic}': {e}")
                    return json.dumps({"status": "error", "message": "Failed to pull messages from hashed node"})

    async def pull_from_replicas(self, topic):
        """
        Attempts to pull messages from one of the replicas if the primary is unavailable.
        """
        if topic in self.replicate_tables:
            replicas = self.replicate_tables[topic]["replicas"]
            for replica_id in replicas:
                host, port = self.helper.get_node_address(replica_id)
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    request = json.dumps({"command": "pull", "topic": topic})
                    writer.write(request.encode())
                    await writer.drain()
                    response = await reader.read(1024)
                    writer.close()
                    await writer.wait_closed()
                    data = json.loads(response.decode())
                    if data.get("status") == "success":
                        return response  # Successfully pulled messages from a replica
                except Exception as e:
                    self.helper.log_event(f"Failed to pull messages from replica {replica_id} for topic '{topic}': {e}")
        return json.dumps({"status": "error", "message": "Failed to pull messages from primary or any replicas"})

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
                self.helper.log_event(f"Sent message to subscriber {subscriber_node_id}: {response.decode()}")
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                self.helper.log_event(f"Error sending message to subscriber {subscriber_node_id}: {e}")

    async def forward_request(self, target_node_id, request, ttl=10):
        # Route the request through the hypercube
        if target_node_id not in self.offline_neighbors:
            route_request = {
                "command": "route",
                "target_node_id": target_node_id,
                "original_request": request,
                "visited_nodes": [self.node_id],
                "ttl": ttl
            }
            return await self.route_message(target_node_id, json.dumps(route_request), visited_nodes={self.node_id}, ttl=ttl)
        return json.dumps({"status": "error", "message": "Failed to Route the message to target node id because of failing node"})

    async def route_message(self, target_node_id, message, visited_nodes=None, ttl=10):
        """
        Routes a message to the target node. Prevents infinite loops by using TTL and tracking visited nodes.
        """
        if visited_nodes is None:
            visited_nodes = set()
        visited_nodes.add(self.node_id)

        if ttl <= 0:
            self.helper.log_event(f"TTL expired while routing to {target_node_id}")
            return json.dumps({"status": "error", "message": "TTL expired during routing"})

        if target_node_id == self.node_id:
            # Process the original request
            original_request = json.loads(message).get("original_request")
            return await self.process_request(json.dumps(original_request))

        next_hop_id = self.helper.find_next_hop(target_node_id,visited_nodes)
        if next_hop_id is None or next_hop_id in visited_nodes:
            self.helper.log_event(f"No next hop found for target node {target_node_id} or next hop already visited.")
            return json.dumps({"status": "error", "message": "Failed to find next hop for the target node"})

        neighbor_host, neighbor_port = self.helper.get_node_address(next_hop_id)
        try:
            reader, writer = await asyncio.open_connection(neighbor_host, neighbor_port)
            # Update the message with the visited nodes and TTL
            routing_message = json.loads(message)
            routing_message['visited_nodes'] = list(visited_nodes)
            routing_message['ttl'] = ttl - 1
            writer.write(json.dumps(routing_message).encode())
            await writer.drain()
            response = await reader.read(1024)
            writer.close()
            await writer.wait_closed()
            return response.decode() if response else json.dumps({"status": "error", "message": "No response from forwarded node"})
        except Exception as e:
            self.helper.log_event(f"Failed to route message to {next_hop_id}: {e}")
            return json.dumps({"status": "error", "message": f"Failed to route message to {next_hop_id}"})

    async def route_to_replica(self, message, max_retries=3):
        """
        Attempts to forward the message to a replica if the primary is unavailable.
        Retries a limited number of times before failing.
        """
        original_request = json.loads(message).get("original_request")
        topic = original_request.get("topic")
        if topic in self.replicate_tables:
            replicas = list(self.replicate_tables[topic]["replicas"])
            retry_count = 0

            for replica_id in replicas:
                if retry_count >= max_retries:
                    self.helper.log_event(f"Max retries reached for routing topic '{topic}' to replicas.")
                    break
                host, port = self.helper.get_node_address(replica_id)
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    writer.write(message.encode())
                    await writer.drain()
                    response = await reader.read(1024)
                    writer.close()
                    await writer.wait_closed()
                    data = json.loads(response.decode())
                    if data.get("status") == "success":
                        return response  # Successfully routed to a replica
                except Exception as e:
                    self.helper.log_event(f"Failed to route message to replica {replica_id}: {e}")
                    retry_count += 1

        return json.dumps({"status": "error", "message": "Failed to route to any replicas after retries"})

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
        node.running = False


if __name__ == "__main__":
    main()
