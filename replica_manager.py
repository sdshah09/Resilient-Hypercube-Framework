import argparse
import asyncio
import json
import hashlib
from datetime import datetime

class ReplicaManager:
    def __init__(self, node_id, neighbors, forward_request, hash_topic):
        """
        Handles replica-related tasks such as synchronization and recovery.
        """
        self.node_id = node_id
        self.neighbors = neighbors
        self.replica_table = {}  # {topic: [list of replica node_ids]}
        self.forward_request = forward_request  # Method to send requests to other nodes
        self.hash_topic = hash_topic  # Hash function to determine topic ownership

    def get_replicas(self, topic):
        """
        Get the list of replicas for a given topic.
        """
        return self.replica_table.get(topic, [])

    def assign_replicas(self, topic, num_replicas=2):
        """
        Assign a fixed number of replicas for a topic.
        """
        self.replica_table[topic] = self.neighbors[:num_replicas]

    async def sync_replicas(self, topic, messages):
        replicas = self.get_replicas(topic)
        tasks = []
        for replica_id in replicas:
            try:
                tasks.append(self.forward_request(replica_id, {
                    "command": "sync",
                    "topic": topic,
                    "messages": messages
                }))
            except Exception as e:
                print(f"Error syncing replica {replica_id}: {e}")
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
    async def recover_node(self, node_id):
        recovered_topics = []
        for topic, replicas in self.replica_table.items():
            if self.hash_topic(topic) == node_id:
                for replica_id in replicas:
                    try:
                        response = await self.forward_request(replica_id, {
                            "command": "pull",
                            "topic": topic
                        })
                        messages = json.loads(response).get("messages", [])
                        if messages:
                            recovered_topics.append((topic, messages))
                            break
                    except Exception as e:
                        print(f"Error recovering topic '{topic}' from replica {replica_id}: {e}")
        return recovered_topics

    async def redistribute_topics(self, new_node_id, topics):
        """
        Redistribute topics to a newly joined node while retaining replicas.
        """
        for topic in topics:
            if self.hash_topic(topic) == new_node_id:
                await self.forward_request(new_node_id, {
                    "command": "create_topic",
                    "topic": topic
                })
                # Retain replicas but remove ownership
                self.replica_table.pop(topic, None)
