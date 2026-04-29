import os
import random
from concurrent import futures

import grpc

from proto.src import marketplace_pb2 as pb2
from proto.src import marketplace_pb2_grpc as pb2_grpc
from src.utils.config import NODE_PORT

CONTROLLER_ADDRESS = os.getenv("CONTROLLER_ADDRESS", "localhost:50050")
SERVICE_PORT = os.getenv("SERVICE_PORT", NODE_PORT)


class ServiceNode(pb2_grpc.MarketplaceServicer):
    """
    Stateless service pod — sits behind the Kubernetes load balancer and
    brokers requests between clients and the storage layer.

    Write path:  ask the controller for the current primary, forward there.
    Read path:   ask the controller for any live replica, read from it.
                 Falls back to the primary if no backup is available.

    The node holds no persistent state of its own, so any number of replicas
    can run behind the same Kubernetes Service without coordination.
    """


    def _get_primary_address(self) -> str | None:
        """Ask the controller which storage node is the current primary."""
        try:
            with grpc.insecure_channel(CONTROLLER_ADDRESS) as ch:
                stub = pb2_grpc.ControllerStub(ch)
                resp = stub.GetPrimary(pb2.GetPrimaryRequest(), timeout=3.0)
                if resp.success:
                    return resp.primary_address
        except Exception as e:
            print(f"[ServiceNode] Could not reach controller: {e}")
        return None

    def _get_all_storage_addresses(self) -> list[str]:
        """
        Ask the controller for the full cluster view so we can route reads
        to any healthy replica.
        """
        try:
            with grpc.insecure_channel(CONTROLLER_ADDRESS) as ch:
                stub = pb2_grpc.ControllerStub(ch)
                resp = stub.GetClusterInfo(pb2.ClusterInfoRequest(), timeout=3.0)
                if resp.success:
                    return list(resp.node_addresses)
        except Exception as e:
            print(f"[ServiceNode] Could not fetch cluster info: {e}")
        return []

    def _storage_stub(self, address: str):
        """Open an insecure channel to a storage node and return its stub."""
        channel = grpc.insecure_channel(address)
        return pb2_grpc.StorageReplicaStub(channel), channel


    def PutItem(self, request: pb2.PutRequest, context) -> pb2.PutResponse:
        """
        Forward a write exclusively to the primary storage node.
        The primary is responsible for replicating the write to all backups
        (active replication) before acknowledging success.
        """
        primary = self._get_primary_address()
        if not primary:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("No primary storage node available")
            return pb2.PutResponse(success=False, message="No primary available")

        try:
            stub, channel = self._storage_stub(primary)
            with channel:
                response = stub.PutItem(request, timeout=5.0)
                return response
        except grpc.RpcError as e:
            print(f"[ServiceNode] PutItem failed on primary {primary}: {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Primary unreachable: {e}")
            return pb2.PutResponse(success=False, message="Primary unreachable")

    def QueryItems(self, request: pb2.QueryRequest, context) -> pb2.QueryResponse:
        """
        Forward a read to any available storage replica.
        Selecting a random replica distributes read load across the storage
        layer (matching the multi-arrow fan-out shown in the diagram).
        Falls back to the primary if no replica list is available.
        """
        candidates = self._get_all_storage_addresses()

        # spread the work load
        if candidates:
            random.shuffle(candidates)
        else:
            primary = self._get_primary_address()
            if not primary:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("No storage nodes available")
                return pb2.QueryResponse(ok=False, items=[], items_found=0)
            candidates = [primary]

        last_error = None
        for addr in candidates:
            try:
                stub, channel = self._storage_stub(addr)
                with channel:
                    response = stub.QueryItems(request, timeout=5.0)
                    return response
            except grpc.RpcError as e:
                print(f"[ServiceNode] QueryItems failed on {addr}: {e}")
                last_error = e
                continue  # try next replica

        context.set_code(grpc.StatusCode.UNAVAILABLE)
        context.set_details(f"All replicas unreachable. Last error: {last_error}")
        return pb2.QueryResponse(ok=False, items=[], items_found=0)



def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_MarketplaceServicer_to_server(ServiceNode(), server)
    server.add_insecure_port(f"[::]:{SERVICE_PORT}")
    print(f"Service node gRPC server starting on port {SERVICE_PORT}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()