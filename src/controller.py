import grpc
from concurrent import futures
import threading
import time
from proto.src import marketplace_pb2 as pb2
from proto.src import marketplace_pb2_grpc as pb2_grpc
from src.utils.config import CONTROLLER_PORT

class Controller(pb2_grpc.ControllerServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.nodes = {} 
        self.primary_address = None

    def RegisterNode(self, request, context):
        with self.lock:
            addr = request.address
            self.nodes[addr] = time.time()
            if not self.primary_address:
                self.primary_address = addr
                print(f"Primary assigned: {addr}")
            print(f"Registered node: {addr}")
            return pb2.RegisterResponse(success=True, is_primary=(addr == self.primary_address))

    def GetPrimary(self, request, context):
        with self.lock:
            if not self.primary_address:
                return pb2.GetPrimaryResponse(success=False, message="No primary available")
            return pb2.GetPrimaryResponse(success=True, primary_address=self.primary_address)

    def HeartbeatMonitor(self):
        """Background thread to actively check Storage Node health."""
        while True:
            time.sleep(5)
            with self.lock:
                for addr in list(self.nodes.keys()):
                    try:
                        with grpc.insecure_channel(addr) as channel:
                            stub = pb2_grpc.StorageReplicaStub(channel)
                            response = stub.Heartbeat(pb2.HealthCheckRequest(request_source="CONTROLLER"), timeout=2.0)
                            if response.alive:
                                print(f"Node {addr} is healthy ({response.role})")
                            else:
                                raise Exception("Node unhealthy")
                    except Exception:
                        print(f"Node {addr} failed heartbeat! Removing from cluster.")
                        del self.nodes[addr]
                        if self.primary_address == addr:
                            self.primary_address = None
                            self.ElectNewPrimary()
    
    def ElectNewPrimary(self):
        """Pick a new leader. Caller must hold self.lock!"""
        if not self.nodes:
            print("CRITICAL: All storage nodes are dead.")
            return

        new_primary = list(self.nodes.keys())[0]
        self.primary_address = new_primary
        print(f"ELECTED NEW PRIMARY: {self.primary_address}")
        
        # call in a separate thread/after loop to avoid holding lock
        threading.Thread(target=self.NotifyPromotion, args=(new_primary,)).start()

    def GetClusterInfo(self, request, context):
        """Returns all healthy nodes so Service Nodes can load balance reads."""
        with self.lock:
            # list(self.nodes.keys()) returns all currently tracked addresses
            return pb2.ClusterInfoResponse(
                success=True,
                node_addresses=list(self.nodes.keys())
            )

    def NotifyPromotion(self, address):
        try:
            with grpc.insecure_channel(address) as channel:
                stub = pb2_grpc.StorageReplicaStub(channel)
                stub.PromoteToPrimary(pb2.PromotionRequest(new_role="primary"))
                print(f"Node {address} acknowledged promotion.")
        except Exception as e:
            print(f"Failed to promote {address}: {e}")

def serve():
    # Instantiate
    controller_instance = Controller()

    # Start background monitor
    monitor_thread = threading.Thread(target=controller_instance.HeartbeatMonitor, daemon=True)
    monitor_thread.start()

    # Setup gRPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ControllerServicer_to_server(controller_instance, server)
    server.add_insecure_port(f"[::]:{CONTROLLER_PORT}")
    print(f"Controller starting on port {CONTROLLER_PORT}...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()