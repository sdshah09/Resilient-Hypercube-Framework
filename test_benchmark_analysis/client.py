# client.py
import asyncio
import json

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    async def send_request(self, request):
        """
        Sends a JSON request to the PeerNode and receives a JSON response.
        """
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            writer.write(json.dumps(request).encode())
            await writer.drain()

            response = await reader.read(1024)
            writer.close()
            await writer.wait_closed()

            if response:
                return json.loads(response.decode())
            else:
                print("Received empty response from server.")
                return {"status": "error", "message": "Empty response from server"}
        
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print("Raw response was:", response.decode() if response else "None")
            return {"status": "error", "message": "Invalid JSON format received from server"}
        except Exception as e:
            print(f"An error occurred: {e}")
            return {"status": "error", "message": str(e)}
