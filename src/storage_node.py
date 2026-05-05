import threading
from concurrent import futures

import grpc

import os

from proto.src import marketplace_pb2, marketplace_pb2_grpc
from proto.src.marketplace_pb2 import Item
from src.utils.config import CONTROLLER_ADDRESS, MY_ADDRESS, NODE_PORT


class StorageNode(marketplace_pb2_grpc.StorageReplicaServicer):
    def __init__(self) -> None:
        self.cv = threading.Condition()
        self.items_by_id: dict[str, Item] = {}

        self.port = os.getenv("NODE_PORT", "50051")
        self.role = os.getenv("NODE_ROLE", "backup") 
        raw_peers = os.getenv("PEER_ADDRESSES", "")
        self.peer_addresses = [p.strip() for p in raw_peers.split(",") if p.strip()]
        raw_address = os.getenv("POD_IP", "localhost")

        # Handle address formatting
        if "storage-" in raw_address and ".storage-service" not in raw_address:
            self.my_full_address = f"{raw_address}.storage-service:{NODE_PORT}"
        else:
            self.my_full_address = raw_address if ":" in raw_address else f"{raw_address}:{NODE_PORT}"

        try:
            with grpc.insecure_channel(CONTROLLER_ADDRESS) as channel:
                stub = marketplace_pb2_grpc.ControllerStub(channel)
                
                # 1. Register with the controller
                resp = stub.RegisterNode(marketplace_pb2.RegisterRequest(address=self.my_full_address))
                self.role = "primary" if resp.is_primary else "backup"
                print(f"Registered with Controller. Role assigned: {self.role.upper()}")

                # 2. IF BACKUP: Ask Controller for the current Primary to sync state
                if self.role == "backup":
                    primary_resp = stub.GetPrimary(marketplace_pb2.Empty())
                    if primary_resp.success and primary_resp.primary_address != self.my_full_address:
                        self.sync_from_primary(primary_resp.primary_address)
                    else:
                        print("No primary found or I am the first node. Skipping initial sync.")

        except Exception as e:
            print(f"Could not connect to Controller: {e}. Defaulting to {self.role}")

    def PutItem(self, request: marketplace_pb2.PutRequest, context) -> marketplace_pb2.PutResponse:
        with self.cv:
            item_id = request.item.item_id
            existing = self.items_by_id.get(item_id)

            # Logic for NEW items
            if not existing:
                # Force starting version to 1 if not set
                if request.item.version == 0:
                    request.item.version = 1
            
            # Logic for UPDATES (Consistency Check)
            elif not request.skip_consistency_check:
                # The client must provide the version they CURRENTLY see
                if request.item.version != existing.version:
                    return marketplace_pb2.PutResponse(
                        success=False,
                        current_version=existing.version,
                        message=f"Stale write rejected. Storage has v{existing.version}, you sent v{request.item.version}",
                    )
                
                # Increment version for the successful update
                request.item.version = existing.version + 1

            # --- Local Save ---
            self.items_by_id[item_id] = request.item
            print(f"[{self.role.upper()}] Saved item: {item_id} (v{request.item.version})")

            # --- Active Replication ---
            if self.role == "primary":
                replication_success = self.PropagateToBackups(request.item)
                if not replication_success:
                    # Optional: Rollback local save if replication fails
                    del self.items_by_id[item_id]
                    return marketplace_pb2.PutResponse(
                        success=False,
                        current_version=existing.version if existing else 0,
                        message="Failed to replicate to backups",
                    )

            return marketplace_pb2.PutResponse(
                success=True,
                current_version=request.item.version,
                message=f"Item stored and replicated via {self.role}",
            )

    def PropagateToBackups(self, item: Item) -> bool:
        all_acks = True
        for addr in self.peer_addresses:
            # EXACT match only
            if addr == self.my_full_address:
                print(f"Skipping replication to self ({addr})")
                continue
            
            print(f"Attempting replication to backup: {addr}")
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = marketplace_pb2_grpc.StorageReplicaStub(channel)
                    response = stub.ReplicateLog(marketplace_pb2.ReplicationRequest(item=item), timeout=2.0)
                    if not response.success:
                        all_acks = False
            except Exception as e:
                print(f"Replication to {addr} failed: {e}")
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
            print(f"[BACKUP] Received replication for {request.item.item_id} (v{request.item.version})")
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
                role=self.role,
            )
        
    def PromoteToPrimary(self, request, context):
        with self.cv:
            self.role = "primary"
            print("I have been promoted to PRIMARY!")
            return marketplace_pb2.PromotionResponse(success=True)
        
    def GetSnapshot(self, request, context):
        with self.cv:
            print(f"[PRIMARY] Providing snapshot to a joining backup...")
            items_list = list(self.items_by_id.values())
            return marketplace_pb2.SnapshotResponse(items=items_list)
    
    def sync_from_primary(self, primary_addr):
        print(f"[BACKUP] Attempting to sync state from {primary_addr}...")
        try:
            # Use a longer timeout for the full snapshot than a single write
            with grpc.insecure_channel(primary_addr) as channel:
                stub = marketplace_pb2_grpc.StorageReplicaStub(channel)
                response = stub.GetSnapshot(marketplace_pb2.Empty(), timeout=10.0)
                
                for item in response.items:
                    self.items_by_id[item.item_id] = item
                    
                print(f"[BACKUP] Sync complete. Loaded {len(response.items)} items.")
        except Exception as e:
            print(f"[BACKUP] Sync failed: {e}")


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    marketplace_pb2_grpc.add_StorageReplicaServicer_to_server(StorageNode(), server)
    server.add_insecure_port(f"[::]:{NODE_PORT}")
    print(f"Storage node gRPC server starting on port {NODE_PORT}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()