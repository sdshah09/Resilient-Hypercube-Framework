import argparse
import asyncio
from test_benchmark_analysis.peer_node_interface import PeerNodeInterface

class Subscriber:
    def __init__(self, interface, topic):
        self.interface = interface
        self.topic = topic

    async def subscribe_and_listen(self):
        # Subscribe to the topic
        subscribe_response = await self.interface.subscribe(self.topic)
        print(f"Subscribe Response: {subscribe_response}")

        if subscribe_response.get("status") != "success":
            print("Failed to subscribe to the topic. Exiting.")
            return

        print(f"Listening for messages on topic: {self.topic}")
        try:
            while True:
                # Pull messages periodically
                messages = await self.interface.pull(self.topic)
                if messages.get("status") == "success" and "messages" in messages:
                    for message in messages["messages"]:
                        print(f"New Message on '{self.topic}': {message}")
                await asyncio.sleep(4)  # Adjust polling interval as needed
        except asyncio.CancelledError:
            print("Subscription listener stopped.")

async def main():
    parser = argparse.ArgumentParser(description="Subscriber Client")
    parser.add_argument('--host', type=str, default='localhost', help='Host of the peer node')
    parser.add_argument('--port', type=int, default=5555, help='Port of the peer node')
    parser.add_argument('--topic', type=str, required=True, help='Topic to subscribe to')

    args = parser.parse_args()

    # Initialize the PeerNodeInterface
    interface = PeerNodeInterface(args.host, args.port)
    subscriber = Subscriber(interface, args.topic)

    # Start the subscription listener
    await subscriber.subscribe_and_listen()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Subscriber stopped.")
