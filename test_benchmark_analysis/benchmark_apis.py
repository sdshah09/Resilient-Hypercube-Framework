import time
import asyncio
from peer_node_interface import PeerNodeInterface

async def benchmark_api(interface, api_func, *args, repeat=100):
    latencies = []
    start_time = time.time()

    for _ in range(repeat):
        start = time.time()
        await api_func(*args)
        end = time.time()
        latencies.append(end - start)

    total_time = time.time() - start_time
    avg_latency = sum(latencies) / len(latencies)
    throughput = repeat / total_time

    return avg_latency, throughput

async def run_benchmarks():
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
    results = []

    for host, port in peers:
        interface = PeerNodeInterface(host, port)
        topic = "benchmark_topic"
        message = "message"

        avg_latency, throughput = await benchmark_api(interface, interface.create_topic, topic)
        results.append((host, port, "create_topic", avg_latency, throughput))

        avg_latency, throughput = await benchmark_api(interface, interface.publish, topic, message)
        results.append((host, port, "publish", avg_latency, throughput))

        avg_latency, throughput = await benchmark_api(interface, interface.subscribe, topic)
        results.append((host, port, "subscribe", avg_latency, throughput))

        avg_latency, throughput = await benchmark_api(interface, interface.pull, topic)
        results.append((host, port, "pull", avg_latency, throughput))

    # Print benchmark results
    for result in results:
        print(f"Peer {result[0]}:{result[1]} - API: {result[2]} - Latency: {result[3]:.4f}s - Throughput: {result[4]:.2f} req/s")

# Run the benchmarks
asyncio.run(run_benchmarks())
