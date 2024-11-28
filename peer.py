import argparse
import asyncio
import json
from PeerNodeHelper import PeerNodeHelper
import time

NODES = ['000','001','010','011','100','101','110','111']

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
        self.neighbors = self.helper.get_neighbors()  # Neighboring nodes in the hypercube\
        self.running = True
        self.log_file = f"peer_node_{self.port}.log"  # Log events
        self.replica_table = {} # {topic: [replicas]}
        self.offline_nodes = set() # Keep track of nodes that are offline
        self.heartbeat_interval = 4
        self.print_count = 3
        self.replicate_subscribers = {} #{key = topic: value = list of nodes that subscribed to that topic}
        
    async def discover_topics_from_neighbors(self):
        """
        Query neighbors to discover topics belonging to this node.
        """
        topics_to_reclaim = {}

        for neighbor_id in self.neighbors:
            try:
                response = await self.forward_request(neighbor_id, {
                    "command": "discover_topics",
                    "node_id": self.node_id
                })
                response_data = json.loads(response)
                if response_data.get("status") == "success":
                    for topic, data in response_data.get("topics", {}).items():
                        if data["primary"] == self.node_id or self.node_id in data["replicas"]:
                            topics_to_reclaim[topic] = data
            except Exception as e:
                self.helper.log_event(f"Error discovering topics from neighbor {neighbor_id}: {e}")

        return topics_to_reclaim
    
    async def get_primary_holder(self, topic):
        """
        Retrieve the primary holder of a topic by forwarding the request if not locally found.
        """
        # Check if the topic exists in the local replica_table
        if topic in self.replica_table:
            primary = self.replica_table[topic]["primary"]
            self.helper.log_event(f"Primary for topic '{topic}' found locally: {primary}")
            return primary

        # If not found locally, forward the request to a neighbor
        self.helper.log_event(f"Primary for topic '{topic}' not found locally. Forwarding request to neighbors.")
        for neighbor_id in self.neighbors:
            try:
                request = json.dumps({
                    "command": "find_primary",
                    "topic": topic
                })
                response = await self.route_message(neighbor_id, request)
                if response:
                    response_data = json.loads(response)
                    if response_data.get("status") == "success":
                        return response_data.get("primary")
            except Exception as e:
                self.helper.log_event(f"Error contacting neighbor {neighbor_id}: {e}")
        self.helper.log_event(f"Failed to locate primary for topic '{topic}'.")
        return None
    
    async def add_offline_node(self,failed_node):
        # for node in range(self.total_nodes):
        for node in NODES:
            await self.forward_request(node,
                                       {"command":"add_offline_node",
                                        "failed_node":failed_node}
                                       )
            
    async def remove_offline_node(self,online_node):
        for node in NODES:
            await self.forward_request(node,{
                "command":"remove_offline_node",
                "online_node":online_node
            })
        if online_node == self.node_id:
            self.helper.log_event(f"Node {self.node_id} rejoined the network. Reclaiming topics.")
            await self.handle_rejoin()
            
    async def send_heartbeat(self):
        while self.running:
            for neighbor_id in self.neighbors:
                host,port = self.helper.get_node_address(neighbor_id)
                try:
                    reader,writer = await asyncio.open_connection(host,port)
                    request = json.dumps({
                        "command":"heartbeat",
                        "node":neighbor_id
                    })
                    writer.write(request.encode())
                    await writer.drain()
                    response = await reader.read(1024)
                    response_data = json.loads(response.decode())
                    if response_data.get("status") == "success":
                        if neighbor_id in self.offline_nodes:
                            await self.remove_offline_node(neighbor_id)
                            self.helper.log_event(f"Node {neighbor_id} that went offline came back online") 
                    else:
                        if neighbor_id not in self.offline_nodes:
                            await self.add_offline_node(neighbor_id)
                            self.helper.log_event(f"Node {neighbor_id} went offline going into handle failure node")
                            await self.handle_node_failure(neighbor_id)
                except Exception as e:
                            if neighbor_id not in self.offline_nodes:
                                await self.add_offline_node(neighbor_id)
                                self.helper.log_event(f"Node {neighbor_id} went offline going into handle failure node")
                                await self.handle_node_failure(neighbor_id)
                            if self.print_count<=3:
                                self.helper.log_event(f"Error in heartbeat function: {e}")
                            self.print_count+=1
                finally:
                    if 'writer' in locals():
                        writer.close()
                        await writer.wait_closed()

            await asyncio.sleep(self.heartbeat_interval)
    
    async def handle_node_failure(self, failed_node):
        """
        Handles the failure of a node, including primary node re-election if necessary.
        """
        self.helper.log_event(f"Handling failure of node: {failed_node}")

        # Iterate over topics to identify the ones affected by the failure
        for topic, replica_data in self.replica_table.items():
            is_primary = replica_data.get("primary") == failed_node
            replicas = replica_data.get("replicas", [])
            if is_primary:
                self.helper.log_event(f"Primary node {failed_node} for topic '{topic}' has failed. Initiating election.")

                if replicas:
                    # Elect a new primary from the replicas
                    new_primary = self.replica_table[topic]["replicas"].pop() # Select the first available replica
                    self.replica_table[topic]["primary"] = new_primary  # Update the primary node
                    self.helper.log_event(f"Node {new_primary} is the new primary for topic '{topic}'.")
                else:
                    # No replicas available, the topic becomes unavailable
                    self.replica_table[topic]["primary"] = None
                    self.helper.log_event(f"No replicas available for topic '{topic}'. Topic is now unavailable.")
            elif failed_node in replicas:
                # If the failed node is a replica, remove it from the list
                self.replica_table[topic]["replicas"].remove(failed_node)
                self.helper.log_event(f"Replica node {failed_node} removed for topic '{topic}'.")

    async def handle_rejoin(self):
        """
        Handle rejoining the network and reclaim topics that belong to this node.
        """
        self.helper.log_event("Rejoining the network and reclaiming topics.")

        # Step 1: Query neighbors for topics owned or replicated
        discovered_topics = await self.discover_topics_from_neighbors()

        # Step 2: Reclaim ownership for topics that belong to this node
        await self.reclaim_topics(discovered_topics)

    async def start(self):
        server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        self.helper.log_event(f"Peer node {self.node_id} started on {self.host}:{self.port}")
        heartbeat = asyncio.create_task(self.send_heartbeat())
        await self.handle_rejoin()

        async with server:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                server.close()
                await server.wait_closed()
                self.helper.log_event(f"Peer node {self.node_id} shut down.")
            finally:
                if heartbeat:
                    heartbeat.cancel()
                    await heartbeat
                
    async def handle_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')

        while True:
            try:
                data = await reader.read(1024)
                if not data:
                    break

                message = data.decode()
                if "heartbeat" not in message and "pull" not in message:
                    self.helper.log_event(f"Received message from {addr}: {message}")

                # Process the request and get a response
                response = await self.process_request(message)

                # Ensure the response is serialized to JSON before writing
                if isinstance(response, dict):
                    response = json.dumps(response)

                writer.write(response.encode())  # Send the response as bytes
                await writer.drain()

            except Exception as e:
                self.helper.log_event(f"Error in handle_connection: {e}")
                break

        writer.close()
        await writer.wait_closed()

    async def process_request(self, message):
        try:
            request = json.loads(message)
            # print("Request in process Request it is: ",request)
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
                subscriber_node_id = request.get('subscriber_node_id')
                subscriber_host = request.get('subscriber_host')
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
                return await self.route_message(target_node_id, message)
            elif command == "replicate_topic":
                topic = request.get('topic')
                primary_node_id = request.get('primary_node_id')
                primary_node_neighbors = request.get('neighbors')
                return await self.replicate_topic(topic,primary_node_id,primary_node_neighbors)
            elif command == "heartbeat":
                return json.dumps({"status":"success","message":f"Node {self.node_id} is online"})
            elif command == "replicate_subscriber":
                topic = request.get('topic')
                subscriber_port = request.get('subscriber_port')
                subscriber_node_id = request.get('subscriber_node_id')
                subscriber_host = request.get('subscriber_host')
                if topic not in self.replicate_subscribers:
                    self.replicate_subscribers[topic] = set()
                self.replicate_subscribers[topic].add((subscriber_host,subscriber_port,subscriber_node_id))
                self.helper.log_event(f"Replicated subscriber to topic: {topic} and the table is {self.replicate_subscribers}")
                return json.dumps({"status":"success","message":f"Replicated subscriber to topic: {topic} on node {self.node_id}"})
            elif command == "find_primary":
                topic = request.get('topic')
                ttl = request.get('ttl', 7)  # Set a default TTL if not provided
                if topic in self.replica_table:
                    primary = self.replica_table[topic]["primary"]
                    return json.dumps({"status": "success", "primary": primary})
                if ttl <= 0:
                    return json.dumps({"status": "error", "message": "TTL expired while searching for primary"})

                # Forward to neighbors
                for neighbor_id in self.neighbors:
                    try:
                        response = await self.route_message(neighbor_id, json.dumps({
                            "command": "find_primary",
                            "topic": topic,
                            "ttl": ttl - 1  # Decrement TTL
                        }))
                        if response:
                            return response
                    except Exception as e:
                        self.helper.log_event(f"Error forwarding 'find_primary' to neighbor {neighbor_id}: {e}")
                return json.dumps({"status": "error", "message": f"Primary for topic '{topic}' not found."})
            elif command == "add_offline_node":
                failed_node = request.get('failed_node')
                self.offline_nodes.add(failed_node)
                self.helper.log_event(f"Added Failed node to offline nodes set : {failed_node}, {self.offline_nodes}")
                return json.dumps({'status':'success','message':f'Removed {failed_node} because it came online'})
            elif command == "remove_offline_node":
                online_node = request.get('online_node')
                if online_node in self.offline_nodes:
                    self.offline_nodes.remove(online_node)
                self.helper.log_event(f"Removed node: {online_node} from offiline nodes set because it came online {self.offline_nodes}")
                return json.dumps({'status':'success','message':f'Removed {online_node} because it came online'})
            elif command == "discover_topics":
                requesting_node_id = request.get("node_id")
                topics_to_report = {}

                for topic, data in self.replica_table.items():
                    if data["primary"] == requesting_node_id or requesting_node_id in data["replicas"]:
                        topics_to_report[topic] = data

                return json.dumps({
                    "status": "success",
                    "topics": topics_to_report
                })
            elif command == "transfer_topic":
                topic = request.get("topic")
                new_primary = request.get("new_primary")
                if topic in self.topics:
                    # Transfer topic ownership but retain the replica
                    topic_data = self.topics[topic]
                    self.replica_table[topic]["primary"] = new_primary  # Update primary in replica table
                    return json.dumps({
                        "status": "success",
                        "topic_data": topic_data
                    })
                else:
                    return json.dumps({
                        "status": "error",
                        "message": f"Topic '{topic}' not found on node {self.node_id}"
                    })
            else:
                return json.dumps({"status": "error", "message": "Unknown command"})
        except json.JSONDecodeError:
            return json.dumps({"status": "error", "message": "Invalid JSON format"})
        
    async def reclaim_topics(self, topics_to_reclaim):
        """
        Reclaim ownership or replicas for topics that belong to this node.
        """
        for topic, data in topics_to_reclaim.items():
            current_primary = data["primary"]
            replicas = data["replicas"]

            if current_primary == self.node_id:
                # The node is the rightful primary, fetch full topic data
                self.helper.log_event(f"Reclaiming primary ownership of topic '{topic}' from {current_primary}.")
                response = await self.forward_request(current_primary, {
                    "command": "transfer_topic",
                    "topic": topic,
                    "new_primary": self.node_id
                })
                response_data = json.loads(response)
                if response_data.get("status") == "success":
                    self.topics[topic] = response_data.get("topic_data", [])
                    self.replica_table[topic] = {
                        "primary": self.node_id,
                        "replicas": replicas
                    }
            else:
                # The node should retain its replica if it's listed
                if self.node_id in replicas:
                    self.helper.log_event(f"Retaining replica for topic '{topic}'.")
                    if topic not in self.replica_table:
                        self.replica_table[topic] = {"primary": current_primary, "replicas": replicas}
                        
    async def create_topic(self, topic):
        topic_node_id = self.helper.hash_topic(topic)
        if topic_node_id == self.node_id:
            if topic in self.topics:
                return json.dumps({"status": "error", "message": "Topic already exists"})
            self.topics[topic] = []
            self.subscribers[topic] = set()
            self.replica_table[topic] = {"primary":self.node_id,"replicas":set()}
            self.helper.log_event(f"Created topic: {topic} on node: {topic_node_id}")
            # Create replicas
            for neighbor_id in self.neighbors:
                self.helper.log_event(f"Neighbor id in create topic function is: {neighbor_id}")
                await self.forward_request(neighbor_id, {
                    "command": "replicate_topic",
                    "topic": topic,
                    "primary_node_id":self.node_id,
                    "neighbors":self.neighbors
                })
            return json.dumps({"status": "success", "message": f"Topic '{topic}' created with replicas"})
        else:
            self.helper.log_event(f"Neighbor id in create topic else part function is: {topic_node_id}")
            return await self.forward_request(topic_node_id, {"command": "create_topic", "topic": topic})

    async def replicate_topic(self,topic,primary_node_id,primary_node_neighbors):
        if topic not in self.replica_table:
            self.replica_table[topic] = {"primary": primary_node_id, "right_node":self.helper.hash_topic(topic),"replicas": sorted(set(primary_node_neighbors)-{primary_node_id})}
        self.helper.log_event(f"Replicated topic '{topic}' on node {self.node_id}")
        return json.dumps({"status": "success", "message": f"Topic '{topic}' replicated"})

    async def delete_topic(self, topic):
        # topic_node_id = self.helper.hash_topic(topic)
        # topic_node_id = self.replica_table[topic]["primary"]
        topic_node_id = await self.get_primary_holder(topic)
        if topic_node_id == self.node_id:
            if topic not in self.topics:
                return json.dumps({"status": "error", "message": "Topic does not exist"})
            del self.topics[topic]
            del self.subscribers[topic]
            del self.replica_table[topic]
            del self.replicate_subscribers[topic]
            self.helper.log_event(f"Deleted topic '{topic}'")
            return json.dumps({"status": "success", "message": f"Topic '{topic}' deleted"})
        else:
            # Route the request to the responsible node
            self.helper.log_event(f"Topic Node in delete topic function is: {topic_node_id}")
            return await self.forward_request(topic_node_id, {"command": "delete_topic", "topic": topic})

    async def publish(self, topic, message):
        # topic_node_id = self.helper.hash_topic(topic)
        topic_node_id = await self.get_primary_holder(topic)
        if topic_node_id in self.offline_nodes:
            self.helper.log_event(f"Node {topic_node_id} is offline cannot publish the message. Try after sometime")
            return json.dumps({"status":"error","message":f"Node {topic_node_id} is offline cannot publish the message. Try after sometime"})
        if topic_node_id == self.node_id: 
            if topic in self.topics:
                self.topics[topic].append(message)
                self.helper.log_event(f"Published message on topic '{topic}': {message}")
                await self.forward_message_to_subscribers(topic, message)
                return json.dumps({"status": "success", "message": f"Message published on topic '{topic}'"})
            else:
                return json.dumps({"status": "error", "message": "Topic does not exist"})
        else:
            # Route the request to the responsible node
            self.helper.log_event(f"Topic Node id in publish function is: {topic_node_id}")
            return await self.forward_request(topic_node_id, {"command": "publish", "topic": topic, "message": message})

    async def subscribe(self, topic):
        # topic_node_id = self.helper.hash_topic(topic)
        topic_node_id = await self.get_primary_holder(topic)
        if topic_node_id not in self.offline_nodes:
            if topic_node_id == self.node_id:
                if topic not in self.topics:
                    self.topics[topic] = []
                    self.subscribers[topic] = set()
                    self.replicate_subscribers[topic] = set()
                self.subscribers[topic].add((self.host, self.port, self.node_id))
                self.replicate_subscribers[topic].add((self.host, self.port, self.node_id))
                self.helper.log_event(f"Local subscription to topic '{topic}' by {self.node_id}")
                return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}'"})
            else:
                self.helper.log_event(f"Topic node in susbcribe function is: {topic_node_id}")
                response = await self.forward_request(topic_node_id, {
                    "command": "subscribe_to_peer",
                    "topic": topic,
                    "subscriber_host": self.host,
                    "subscriber_port": self.port,
                    "subscriber_node_id": self.node_id
                })
                return response
        else:
            self.helper.log_event(f"Cannot subscribe because the node {topic_node_id} is offline")
            return json.dumps({"status":"error","message":f"Cannot subscribe node {topic_node_id} is offline"})

    async def handle_subscription(self,topic, subscriber_host,subscriber_port, subscriber_node_id):
        if topic in self.topics:
            self.subscribers[topic].add((subscriber_host, subscriber_port, subscriber_node_id))
            self.helper.log_event(f"Peer {subscriber_node_id} subscribed to topic '{topic}'")
            for neighbor_id in self.neighbors:
                await self.forward_request(neighbor_id,
                                           {"command":"replicate_subscriber",
                                            "topic":topic,
                                            "subscriber_host":subscriber_host,
                                            "subscriber_port":subscriber_port,
                                            "subscriber_node_id":subscriber_node_id}
                                           )
            return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}'"})
        else:
            return json.dumps({"status": "error", "message": "Topic not found"})


    async def pull(self, topic):
        """
        Pull messages for a given topic. Use cached primary holder if available, otherwise resolve dynamically.
        """
            # Cache miss or expired entry
        topic_node_id = await self.get_primary_holder(topic)
        if not topic_node_id:
            self.helper.log_event(f"Failed to locate primary for topic '{topic}'. Pull aborted.")
            return json.dumps({"status": "error", "message": f"Cannot locate primary for topic '{topic}'"})

        print(f"Pulling from Topic Node: {topic_node_id} for topic '{topic}'")

        if topic_node_id == self.node_id:
            # Local pull
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
            # Forward the pull request to the primary node
            response = await self.forward_request(topic_node_id, {"command": "pull", "topic": topic})
            response_data = json.loads(response)
            return response_data

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

    async def forward_request(self, target_node_id, request):
        # Route the request through the hypercube
        route_request = {
            "command": "route",
            "target_node_id": target_node_id,
            "original_request": request
        }
        return await self.route_message(target_node_id, json.dumps(route_request))

    async def route_message(self, target_node_id, message):
        if target_node_id == self.node_id:
            # Process the original request
            original_request = json.loads(message).get('original_request')
            # print("Original Request in route message is: ",original_request)
            return await self.process_request(json.dumps(original_request))
        else:
            next_hop_id = self.helper.find_next_hop(target_node_id)
            if next_hop_id in self.offline_nodes:
                return json.dumps({"status": "error", "message": f"Failed to route message to {target_node_id} because it is offline"})
            neighbor_host, neighbor_port = self.helper.get_node_address(next_hop_id)
            try:
                reader, writer = await asyncio.open_connection(neighbor_host, neighbor_port)
                writer.write(message.encode())
                await writer.drain()
                response = await reader.read(1024)
                writer.close()
                await writer.wait_closed()
                return response.decode() if response else json.dumps({"status": "error", "message": "No response from forwarded node"})
            except Exception as e:
                self.helper.log_event(f"Failed to route message to {next_hop_id}: {e}")
                return json.dumps({"status": "error", "message": f"Failed to route message to {target_node_id}"})


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
