import asyncio
import random
from peer_node_interface import PeerNodeInterface

async def test_api(peer_host, peer_port, topic, message):
    """ Test all APIs on a given peer. """
    interface = PeerNodeInterface(peer_host, peer_port)

    # Test create_topic
    create_response = await interface.create_topic(topic)
    print(f"Create Topic Response from {peer_host}:{peer_port}: {create_response}")

    # Test publish
    publish_response = await interface.publish(topic, message)
    print(f"Publish Response from {peer_host}:{peer_port}: {publish_response}")

    # Test subscribe
    subscribe_response = await interface.subscribe(topic)
    print(f"Subscribe Response from {peer_host}:{peer_port}: {subscribe_response}")

    # Test pull
    pull_response = await interface.pull(topic)
    print(f"Pull Response from {peer_host}:{peer_port}: {pull_response}")

async def deploy_and_test_peers():
    peers = [
        ("localhost", 5555),
        ("localhost", 5556),
        ("localhost", 5557),
        ("localhost", 5558),
        ("localhost", 5559),
        ("localhost", 5560),
        ("localhost", 5561),
        ("localhost", 5562)
    ]
    tasks = []
    for host, port in peers:
        topic = f"topic_{random.randint(0, 100)}"
        message = f"Message for {topic}"
        tasks.append(test_api(host, port, topic, message))
    
    await asyncio.gather(*tasks)

# Run the deployment and API tests
asyncio.run(deploy_and_test_peers())
