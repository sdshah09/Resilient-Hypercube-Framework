# publisher.py
import argparse
import asyncio
from test_benchmark_analysis.peer_node_interface import PeerNodeInterface

class Publisher:
    def __init__(self, interface, topic):
        self.interface = interface
        self.topic = topic

    async def create_topic(self):
        response = await self.interface.create_topic(self.topic)
        print(f"Create Topic Response: {response}")

    async def publish(self, message):
        response = await self.interface.publish(self.topic, message)
        print(f"Publish Response: {response}")
        
    async def delete_topic(self,topic):
        response = await self.interface.delete_topic(topic)
        print(f"Delete topic Response is: {response}")

async def main():
    parser = argparse.ArgumentParser(description="Publisher Client")
    parser.add_argument('--host', type=str, default='localhost', help='Host of the peer node')
    parser.add_argument('--port', type=int, default=5555, help='Port of the peer node')
    parser.add_argument('--topic', type=str, required=True, help='Topic to publish to or create')
    parser.add_argument('--action', type=str, choices=['create_topic', 'publish','delete_topic'], required=True, help='Action to perform: create_topic or publish')
    parser.add_argument('--message', type=str, help='Message to publish (required if action is publish)')

    args = parser.parse_args()

    # Initialize the PeerNodeInterface
    interface = PeerNodeInterface(args.host, args.port)
    publisher = Publisher(interface, args.topic)

    # Perform the action based on the command line argument
    if args.action == 'create_topic':
        await publisher.create_topic()
    elif args.action == 'publish':
        if not args.message:
            print("Error: --message is required when action is 'publish'")
            return
        await publisher.publish(args.message)
    elif args.action == 'delete_topic':
        await publisher.delete_topic(args.topic)

if __name__ == "__main__":
    asyncio.run(main())
