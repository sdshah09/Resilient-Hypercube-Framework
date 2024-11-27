import hashlib
from datetime import datetime
import platform
class PeerNodeHelper:
    """
    Helper class for utility methods used in the PeerNode class.
    """
    def __init__(self, host, node_id, port, total_bits):
        self.host = host
        self.node_id = node_id
        self.port = port
        self.total_bits = total_bits
        self.total_nodes = 2 ** total_bits
        self.log_file = f"peer_node_{self.port}.log"

    def hash_topic(self, topic):
        """
        Hashes a topic to determine its responsible node.
        """
        hash_value = int(hashlib.sha256(topic.encode()).hexdigest(), 16)
        node_id_int = hash_value % self.total_nodes
        node_id = format(node_id_int, f'0{self.total_bits}b')
        return node_id

    def get_neighbors(self):
        """
        Determines the neighbors of the current node in the hypercube.
        """
        neighbors = []
        node_int = int(self.node_id, 2)
        for i in range(self.total_bits):
            neighbor_int = node_int ^ (1 << i)
            neighbor_id = format(neighbor_int, f'0{self.total_bits}b')
            neighbors.append(neighbor_id)
        return neighbors

    def find_next_hop(self, target_node_id):
        """
        Finds the next hop in the hypercube for routing to a target node.
        """
        current_int = int(self.node_id, 2)
        target_int = int(target_node_id, 2)
        differing_bits = current_int ^ target_int
        for i in range(self.total_bits - 1, -1, -1):
            if differing_bits & (1 << i):
                next_hop_int = current_int ^ (1 << i)
                next_hop_id = format(next_hop_int, f'0{self.total_bits}b')
                return next_hop_id
        return None

    def get_node_address(self, node_id):
        """
        Maps a node ID to its host and port number.
        """
        base_port = 5555
        node_int = int(node_id, 2)
        node_port = base_port + node_int
        return self.host, node_port

    def log_event(self, event):
        """
        Logs events to a file and prints them to the console.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a") as log:
            log.write(f"[{timestamp}] {event}\n")
        print(f"[LOG {self.node_id}] {event}")
        
    def get_ping_command(self):
        """Return the appropriate command for ncat/netcat based on the OS."""
        system = platform.system()
        if system == "Windows":
            # Use ncat on Windows
            return "ncat -zv {host} {port}"
        elif system in ("Linux", "Darwin"):
            # Use netcat/nc on Linux/MacOS
            return "nc -zv {host} {port}"
        else:
            raise RuntimeError(f"Unsupported operating system: {system}")
