import asyncio
import subprocess
from test_benchmark_analysis.peer_node_interface import PeerNodeInterface
from publisher import Publisher
from subscriber import Subscriber

async def test_failover_and_reclaim():
    """
    Test failover and topic reclamation in a distributed publisher-subscriber network with process termination.
    """
    # Configuration
    primary_node_id = '110'  # Initially, node 6 is the primary
    primary_node_port = 5561  # Port for node 6
    neighbor_node_port = 5562  # Port for node 7, expected to become the primary
    subscriber_ports = [5559, 5560, 5558]  # Ports for subscriber nodes
    topic = 'sports'
    initial_messages = ["Match Update 1", "Match Update 2"]
    failover_messages = ["Failover Test 1", "Failover Test 2"]
    reclaim_messages = ["Reclaim Test 1", "Reclaim Test 2"]

    # Track the primary process
    primary_process = None

    # Step 1: Start the primary node process
    print(f"Starting primary node {primary_node_id} (port {primary_node_port})...")
    primary_process = subprocess.Popen(
        ["python", "peer.py", "--node_id", primary_node_id, "--port", str(primary_node_port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Allow time for the primary node to initialize
    await asyncio.sleep(3)

    # Step 2: Set up publisher and subscribers
    print(f"Setting up publisher on Node {primary_node_id} (port {primary_node_port})...")
    primary_interface = PeerNodeInterface("localhost", primary_node_port)
    publisher = Publisher(primary_interface, topic)

    print("Setting up subscribers...")
    subscribers = []
    for port in subscriber_ports:
        subscriber_interface = PeerNodeInterface("localhost", port)
        subscribers.append(Subscriber(subscriber_interface, topic))

    # Step 3: Create the topic
    print(f"Creating topic '{topic}' on Node {primary_node_id}...")
    create_response = await publisher.create_topic()
    print(f"Create Topic Response: {create_response}")

    # Step 4: Subscribe all subscribers
    subscriber_tasks = []
    for idx, subscriber in enumerate(subscribers):
        print(f"Subscriber {idx + 1} subscribing on port {subscriber_ports[idx]}...")
        subscriber_tasks.append(asyncio.create_task(subscriber.subscribe_and_listen()))

    # Allow time for subscriptions to propagate
    await asyncio.sleep(3)

    # Step 5: Publish initial messages
    print("Publishing initial messages...")
    for message in initial_messages:
        publish_response = await publisher.publish(message)
        print(f"Publish Response: {publish_response}")
        await asyncio.sleep(1)

    # Step 6: Kill the primary node process to simulate failure
    print(f"Killing Node {primary_node_id} (port {primary_node_port}) to simulate failure...")
    if primary_process:
        primary_process.terminate()
        primary_process.wait()
    else:
        print("Error: Primary process is not running.")

    # Allow time for failover
    await asyncio.sleep(20)

    # Step 7: Verify failover and publish through new primary (Node 7)
    print(f"Publishing messages after failover to Node 7 (port {neighbor_node_port})...")
    neighbor_interface = PeerNodeInterface("localhost", neighbor_node_port)
    neighbor_publisher = Publisher(neighbor_interface, topic)
    for message in failover_messages:
        publish_response = await neighbor_publisher.publish(message)
        print(f"Publish Response: {publish_response}")
        await asyncio.sleep(1)

    await asyncio.sleep(30)
    # Step 8: Restart the original primary node process (Node 6)
    print(f"Restarting Node {primary_node_id} (port {primary_node_port})...")
    primary_process = subprocess.Popen(
        ["python", "peer.py", "--node_id", primary_node_id, "--port", str(primary_node_port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Allow time for the node to initialize and reclaim topics
    print(f"Waiting for Node {primary_node_id} to reclaim topics...")
    for _ in range(30):
        # Query Node 6 to check if it has reclaimed the topic
        reclaim_status = await primary_interface.get_topic_status(topic)
        if reclaim_status and reclaim_status.get("primary") == primary_node_id:
            print(f"Node {primary_node_id} has successfully reclaimed the topic '{topic}'.")
            break
        await asyncio.sleep(1)
    else:
        print(f"Node {primary_node_id} did not reclaim the topic '{topic}' within 30 seconds.")

    # Step 9: Verify topic reclamation and publish through reclaimed primary
    print(f"Publishing messages after Node {primary_node_id} reclaims the topic...")
    for message in reclaim_messages:
        publish_response = await publisher.publish(message)
        print(f"Publish Response: {publish_response}")
        await asyncio.sleep(1)

    # Step 10: Clean up and delete the topic
    print(f"Deleting topic '{topic}'...")
    delete_response = await neighbor_publisher.delete_topic(topic)
    print(f"Delete Topic Response: {delete_response}")

    # Cancel all subscriber tasks
    for task in subscriber_tasks:
        task.cancel()

    print("Test complete. All resources cleaned up.")

if __name__ == "__main__":
    asyncio.run(test_failover_and_reclaim())
