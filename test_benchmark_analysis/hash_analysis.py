import hashlib
import time
from collections import Counter

def hash_topic(topic, total_nodes):
    hash_value = int(hashlib.sha256(topic.encode()).hexdigest(), 16)
    return hash_value % total_nodes

def measure_hashing_time(topics, total_nodes):
    times = []
    for topic in topics:
        start = time.time()
        hash_topic(topic, total_nodes)
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times)

def measure_hash_distribution(topics, total_nodes):
    node_distribution = Counter()
    for topic in topics:
        node_id = hash_topic(topic, total_nodes)
        node_distribution[node_id] += 1
    return node_distribution



# Generate random topics and measure hashing time
topics = [f"topic_{i}" for i in range(10000)]
average_time = measure_hashing_time(topics, 8)
print(f"Average Hashing Time: {average_time:.8f} seconds")
distribution = measure_hash_distribution(topics, 8)
print("Topic Distribution Across Nodes:", distribution)
