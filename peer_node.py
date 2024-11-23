import argparse
import asyncio
import json
import hashlib
from datetime import datetime


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
        

    async def start(self):
        server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        self.log_event(f"Peer node {self.node_id} started on {self.host}:{self.port}")

        async with server:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                server.close()
                await server.wait_closed()
                self.log_event(f"Peer node {self.node_id} shut down.")
                
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
                subscriber_node_id = request.get('subscriber_node_id')
                return await self.handle_subscription(topic, subscriber_port, subscriber_node_id)
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
            return json.dumps({"status": "success", "message": f"Topic '{topic}' created"})
        else:
            # Route the request to the responsible node
            return await self.forward_request(topic_node_id, {"command": "create_topic", "topic": topic})

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
        if topic_node_id == self.node_id:
            if topic in self.topics:
                self.topics[topic].append(message)
                self.log_event(f"Published message on topic '{topic}': {message}")
                await self.forward_message_to_subscribers(topic, message)
                return json.dumps({"status": "success", "message": f"Message published on topic '{topic}'"})
            else:
                return json.dumps({"status": "error", "message": "Topic does not exist"})
        else:
            # Route the request to the responsible node
            return await self.forward_request(topic_node_id, {"command": "publish", "topic": topic, "message": message})

    async def subscribe(self, topic):
        topic_node_id = self.hash_topic(topic)
        if topic_node_id == self.node_id:
            if topic not in self.topics:
                self.topics[topic] = []
                self.subscribers[topic] = set()
            self.subscribers[topic].add((self.host, self.port, self.node_id))
            self.log_event(f"Local subscription to topic '{topic}' by {self.node_id}")
            return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}'"})
        else:
            return await self.forward_request(topic_node_id, {
                "command": "subscribe_to_peer",
                "topic": topic,
                "subscriber_host": self.host,
                "subscriber_port": self.port,
                "subscriber_node_id": self.node_id
            })

    async def handle_subscription(self, topic, subscriber_port, subscriber_node_id):
        if topic in self.topics:
            self.subscribers[topic].add((self.host, subscriber_port, subscriber_node_id))
            self.log_event(f"Peer {subscriber_node_id} subscribed to topic '{topic}'")
            return json.dumps({"status": "success", "message": f"Subscribed to topic '{topic}'"})
        else:
            return json.dumps({"status": "error", "message": "Topic not found"})

    async def pull(self, topic):
        topic_node_id = self.hash_topic(topic)
        if topic_node_id == self.node_id:
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
            print("Original Request in route message is: ",original_request)
            return await self.process_request(json.dumps(original_request))
        else:
            next_hop_id = self.find_next_hop(target_node_id)
            neighbor_host, neighbor_port = self.get_node_address(next_hop_id)
            try:
                reader, writer = await asyncio.open_connection(neighbor_host, neighbor_port)
                writer.write(message.encode())
                await writer.drain()
                response = await reader.read(1024)
                writer.close()
                await writer.wait_closed()
                return response.decode() if response else json.dumps({"status": "error", "message": "No response from forwarded node"})
            except Exception as e:
                self.log_event(f"Failed to route message to {next_hop_id}: {e}")
                return json.dumps({"status": "error", "message": f"Failed to route message to {target_node_id}"})

    def find_next_hop(self, target_node_id):
        # XOR the current node ID with the target node ID
        current_int = int(self.node_id, 2)
        target_int = int(target_node_id, 2)
        differing_bits = current_int ^ target_int
        # Find the most significant differing bit
        for i in range(self.total_bits - 1, -1, -1):
            if differing_bits & (1 << i):
                # Flip the bit to get the next hop
                next_hop_int = current_int ^ (1 << i)
                next_hop_id = format(next_hop_int, f'0{self.total_bits}b')
                if next_hop_id in self.neighbors:
                    return next_hop_id
        return None

    def get_node_address(self, node_id):
        # Map node IDs to host and port numbers
        base_port = 5555
        node_int = int(node_id, 2)
        node_port = base_port + node_int
        return self.host, node_port

    def log_event(self, event):
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
        node.running = False


if __name__ == "__main__":
    main()
