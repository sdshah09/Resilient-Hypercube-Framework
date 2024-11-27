import time
import asyncio

import sys
print(sys.path)
from peer_node_interface import PeerNodeInterface

# Test if each node can access topics on every other node.
async def test_forwarding_access(peers):
    tasks = []
    for host, port in peers:
        interface = PeerNodeInterface(host, port)
        for target_host, target_port in peers:
            topic = f"forwarding_topic_{target_port}"
            print(f"Testing access from {host}:{port} to topic on {target_host}:{target_port}")
            # Run subscriptions concurrently
            tasks.append(interface.subscribe(topic))
    
    # Run all subscriptions concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Subscription failed: {result}")
        else:
            print(f"Subscription succeeded: {result}")

# Define list of peers
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

# Run forwarding access test
asyncio.run(test_forwarding_access(peers))

# Measure average response time for a given function
async def measure_response_time(interface, func, *args, repeat=50):
    times = []
    for _ in range(repeat):
        start = time.time()
        await func(*args)
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times)

# Measure max throughput within a given duration
async def measure_max_throughput(interface, func, *args, duration=5):
    start_time = time.time()
    request_count = 0
    while time.time() - start_time < duration:
        try:
            await func(*args)
            request_count += 1
        except Exception as e:
            print(f"Error during throughput test: {e}")
    return request_count / duration

# Test forwarding performance on publish requests
async def test_forwarding_performance():
    interface = PeerNodeInterface("localhost", 5555)  # Target a specific peer for testing
    topic = "performance_test_topic"
    message = "Test message"

    # Measure average response time
    avg_response_time = await measure_response_time(interface, interface.publish, topic, message)
    print(f"Average Response Time: {avg_response_time:.4f} seconds")

    # Measure max throughput
    max_throughput = await measure_max_throughput(interface, interface.publish, topic, message)
    print(f"Maximum Throughput: {max_throughput:.2f} requests per second")

# Run forwarding performance tests
asyncio.run(test_forwarding_performance())
