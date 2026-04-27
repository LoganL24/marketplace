import threading
from concurrent import futures

import grpc

import os

from proto.src import marketplace_pb2, marketplace_pb2_grpc
from proto.src.marketplace_pb2 import Item
from src.utils.config import NODE_PORT


class StorageNode(marketplace_pb2_grpc.StorageReplicaServicer):
    def __init__(self) -> None:
        self.cv = threading.Condition()
        self.items_by_id: dict[str, Item] = {}

        self.role = os.getenv("NODE_ROLE", "backup")
        self.peer_addresses = os.getenv("PEER_ADDRESSES", "").split(",")

    def PutItem(self, request: marketplace_pb2.PutRequest, context) -> marketplace_pb2.PutResponse:
        with self.cv:
            item_id = request.item.item_id
            existing = self.items_by_id.get(item_id)

            # Prevent older versions from overwriting newer data
            if existing and not request.skip_consistency_check:
                if request.item.version <= existing.version:
                    return marketplace_pb2.PutResponse(
                        success=False,
                        current_version=existing.version,
                        message="Stale write rejected",
                    )

            # Local Save
            self.items_by_id[item_id] = request.item
            print(f"[{self.role.upper()}] Saved item: {item_id} (v{request.item.version})")

            # Active Replication 
            # If we are the primary, we must push this to all backups
            if self.role == "primary":
                replication_success = self._propagate_to_backups(request.item)
                if not replication_success:
                    # delete the local copy if replication fails to maintain perfect sync
                    return marketplace_pb2.PutResponse(
                        success=False,
                        current_version=request.item.version,
                        message="Failed to replicate to backups",
                    )

            return marketplace_pb2.PutResponse(
                success=True,
                current_version=request.item.version,
                message=f"Item stored and replicated via {self.role}",
            )

    def _propagate_to_backups(self, item: Item) -> bool:
        """Helper to send the item to all peers listed in PEER_ADDRESSES"""
        all_acks = True
        for addr in self.peer_addresses:
            if not addr.strip(): continue
            try:
                # short timeout so one slow backup doesn't hang the UI
                with grpc.insecure_channel(addr) as channel:
                    stub = marketplace_pb2_grpc.StorageReplicaStub(channel)
                    repl_req = marketplace_pb2.ReplicationRequest(item=item)
                    
                    response = stub.ReplicateLog(repl_req, timeout=2.0)
                    if not response.success:
                        print(f"Backup {addr} rejected replication.")
                        all_acks = False
            except Exception as e:
                print(f"Connection failed to backup {addr}: {e}")
                all_acks = False
        return all_acks

    def QueryItems(self, request: marketplace_pb2.QueryRequest, context) -> marketplace_pb2.QueryResponse:
        with self.cv:
            filter_text = request.filter.strip().lower()
            all_items = list(self.items_by_id.values())

            if not filter_text:
                matches = all_items
            else:
                matches = [
                    item
                    for item in all_items
                    if filter_text in item.title.lower()
                    or filter_text in item.category.lower()
                    or filter_text in item.description.lower()
                ]

            return marketplace_pb2.QueryResponse(
                ok=True,
                items=matches,
                items_found=len(matches),
            )

    def SyncFullState(
        self, request: marketplace_pb2.StateRequest, context
    ) -> marketplace_pb2.StateResponse:
        with self.cv:
            items = list(self.items_by_id.values())
            last_version = max((item.version for item in items), default=0)
            return marketplace_pb2.StateResponse(
                ok=True,
                items=items,
                last_included_version=last_version,
            )

    def ReplicateLog(
        self, request: marketplace_pb2.ReplicationRequest, context
    ) -> marketplace_pb2.ReplicationResponse:
        with self.cv:
            self.items_by_id[request.item.item_id] = request.item
            return marketplace_pb2.ReplicationResponse(
                success=True,
                ack_version=request.item.version,
            )

    def Heartbeat(
        self, request: marketplace_pb2.HealthCheckRequest, context
    ) -> marketplace_pb2.HealthCheckResponse:
        with self.cv:
            return marketplace_pb2.HealthCheckResponse(
                alive=True,
                item_count=len(self.items_by_id),
                role="replica",
            )


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    marketplace_pb2_grpc.add_StorageReplicaServicer_to_server(StorageNode(), server)
    server.add_insecure_port(f"[::]:{NODE_PORT}")
    print(f"Storage node gRPC server starting on port {NODE_PORT}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()