import asyncio
from test_benchmark_analysis.peer_node_interface import PeerNodeInterface
from publisher import Publisher
from subscriber import Subscriber

async def test_publisher_multiple_subscribers():
    # Configuration
    host = 'localhost'
    publisher_port = 5555  # Publisher's peer node port
    subscriber_ports = [5556, 5557]  # Ports for the subscribers' peer nodes
    topic = 'test_topic'
    messages_to_publish = ["Hello, Subscriber 1!", "Hello, Subscriber 2!", "Hello, Everyone!"]

    # Initialize Publisher
    publisher_interface = PeerNodeInterface(host, publisher_port)
    publisher = Publisher(publisher_interface, topic)

    # Initialize Subscribers
    subscribers = []
    for port in subscriber_ports:
        interface = PeerNodeInterface(host, port)
        subscriber = Subscriber(interface, topic)
        subscribers.append(subscriber)

    # Step 1: Create the topic
    print("Creating topic...")
    await publisher.create_topic()

    # Step 2: Subscribe all subscribers to the topic
    tasks = []
    for idx, subscriber in enumerate(subscribers):
        print(f"Subscriber {idx + 1} subscribing on port {subscriber_ports[idx]}...")
        tasks.append(asyncio.create_task(subscriber.subscribe_and_listen()))

    # Allow time for subscriptions to complete
    await asyncio.sleep(2)

    # Step 3: Publish messages
    print("Publishing messages...")
    for message in messages_to_publish:
        await publisher.publish(message)
        await asyncio.sleep(1)  # Allow time between messages for delivery

    # Step 4: Allow subscribers to process messages
    print("Waiting for subscribers to receive messages...")
    await asyncio.sleep(5)  # Adjust this as needed for message delivery

    # Step 5: Clean up (optional)
    print("Deleting topic...")
    await publisher.delete_topic(topic)

    # Cancel subscriber tasks
    for task in tasks:
        task.cancel()

# Run the test
if __name__ == "__main__":
    asyncio.run(test_publisher_multiple_subscribers())
