import asyncio
from test_benchmark_analysis.peer_node_interface import PeerNodeInterface
from publisher import Publisher
from subscriber import Subscriber

async def test_sports_topic_scenario():
    # Configuration
    host = 'localhost'
    publisher_port = 5555  # Port of the node acting as publisher
    subscriber_ports = [5556, 5557, 5558]  # Ports for the subscriber nodes
    topic = 'sports'
    messages_to_publish = ["Team A won!", "Game scheduled for tomorrow.", "Exciting match ahead!"]

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
    print("Creating topic 'sports'...")
    await publisher.create_topic()

    # Step 2: Subscribe all subscribers to the topic
    tasks = []
    for idx, subscriber in enumerate(subscribers):
        print(f"Subscriber {idx + 1} subscribing on port {subscriber_ports[idx]}...")
        tasks.append(asyncio.create_task(subscriber.subscribe_and_listen()))

    # Allow time for subscriptions to complete
    await asyncio.sleep(2)

    # Step 3: Publish messages
    print("Publishing messages to 'sports' topic...")
    for message in messages_to_publish:
        await publisher.publish(message)
        await asyncio.sleep(1)  # Allow time between messages for delivery

    # Step 4: Simulate node failure
    print("Simulating node failure for publisher...")
    failed_node_id = '000'  # Assuming node 000 holds the primary for the topic
    for port in subscriber_ports:
        interface = PeerNodeInterface(host, port)
        await interface.add_offline_node(failed_node_id)

    # Allow time for new primary election
    await asyncio.sleep(5)

    # Step 5: Publish more messages to test new primary
    print("Publishing messages after node failure...")
    new_messages = ["New primary works!", "Match postponed."]
    for message in new_messages:
        await publisher.publish(message)
        await asyncio.sleep(1)

    # Step 6: Allow subscribers to process messages
    print("Waiting for subscribers to process messages...")
    await asyncio.sleep(5)

    # Step 7: Clean up
    print("Deleting topic 'sports'...")
    await publisher.delete_topic(topic)

    # Cancel subscriber tasks
    for task in tasks:
        task.cancel()

if __name__ == "__main__":
    asyncio.run(test_sports_topic_scenario())
