# peer_node_interface.py
from .client import Client

class PeerNodeInterface:
    def __init__(self, host, port):
        self.client = Client(host, port)

    async def create_topic(self, topic):
        request = {"command": "create_topic", "topic": topic}
        return await self.client.send_request(request)

    async def delete_topic(self, topic):
        request = {"command": "delete_topic", "topic": topic}
        return await self.client.send_request(request)

    async def publish(self, topic, message):
        request = {"command": "publish", "topic": topic, "message": message}
        return await self.client.send_request(request)

    async def subscribe(self, topic):
        request = {"command": "subscribe", "topic": topic}
        return await self.client.send_request(request)

    async def pull(self, topic):
        request = {"command": "pull", "topic": topic}
        return await self.client.send_request(request)
