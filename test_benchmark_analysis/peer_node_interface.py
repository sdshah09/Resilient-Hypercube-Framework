from .client import Client

class PeerNodeInterface:
    def __init__(self, host, port):
        self.client = Client(host, port)

    async def create_topic(self, topic):
        """
        Sends a request to create a new topic.
        """
        request = {"command": "create_topic", "topic": topic}
        return await self.client.send_request(request)

    async def delete_topic(self, topic):
        """
        Sends a request to delete an existing topic.
        """
        request = {"command": "delete_topic", "topic": topic}
        return await self.client.send_request(request)

    async def publish(self, topic, message):
        """
        Sends a request to publish a message to a topic.
        """
        request = {"command": "publish", "topic": topic, "message": message}
        return await self.client.send_request(request)

    async def subscribe(self, topic):
        """
        Sends a request to subscribe to a topic.
        """
        request = {"command": "subscribe", "topic": topic}
        return await self.client.send_request(request)

    async def pull(self, topic):
        """
        Sends a request to pull messages from a topic.
        """
        request = {"command": "pull", "topic": topic}
        return await self.client.send_request(request)

    async def add_offline_node(self, failed_node_id):
        """
        Sends a request to mark a node as offline.
        """
        request = {"command": "add_offline_node", "failed_node": failed_node_id}
        return await self.client.send_request(request)

    async def remove_offline_node(self, online_node_id):
        """
        Sends a request to mark a previously offline node as online.
        """
        request = {"command": "remove_offline_node", "online_node": online_node_id}
        return await self.client.send_request(request)

    async def find_primary(self, topic):
        """
        Sends a request to find the primary holder for a topic.
        """
        request = {"command": "find_primary", "topic": topic}
        return await self.client.send_request(request)
