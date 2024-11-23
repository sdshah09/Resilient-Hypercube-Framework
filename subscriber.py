# subscriber.py
import argparse
import asyncio
from test_benchmark_analysis.peer_node_interface import PeerNodeInterface

class Subscriber:
    def __init__(self, interface, topic):
        self.interface = interface
        self.topic = topic

    async def subscribe(self):
        response = await self.interface.subscribe(self.topic)
        print(f"Subscribe Response: {response}")

    async def pull_messages(self):
        response = await self.interface.pull(self.topic)
        print(f"Pull Response: {response}")  # Log the raw response for debugging
        if response.get("status") == "success":
            messages = response.get("messages", [])
            if messages:
                print(f"Received messages: {messages}")
            else:
                print("No new messages")
        else:
            print(f"Status for pulling messages is : {response.get('message')}")

async def main():
    parser = argparse.ArgumentParser(description="Subscriber Client")
    parser.add_argument('--host', type=str, default='localhost', help='Host of the peer node')
    parser.add_argument('--port', type=int, default=5555, help='Port of the peer node')
    parser.add_argument('--topic', type=str, required=True, help='Topic to subscribe and pull messages from')
    args = parser.parse_args()

    interface = PeerNodeInterface(args.host, args.port)
    subscriber = Subscriber(interface, args.topic)
    await subscriber.subscribe()

    # Periodically pull messages
    while True:
        await subscriber.pull_messages()
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
