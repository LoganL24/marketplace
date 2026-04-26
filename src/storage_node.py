import threading
from concurrent import futures

import grpc

from proto.src import marketplace_pb2, marketplace_pb2_grpc
from proto.src.marketplace_pb2 import Item
from utils.config import NODE_PORT


class StorageNode(marketplace_pb2_grpc.StorageReplicaServicer):
    def __init__(self) -> None:
        self.cv = threading.Condition()
        self.items_by_id: dict[str, Item] = {}

    def PutItem(self, request: marketplace_pb2.PutRequest, context) -> marketplace_pb2.PutResponse:
        with self.cv:
            item_id = request.item.item_id
            existing = self.items_by_id.get(item_id)

            if existing and not request.skip_consistency_check:
                if request.item.version <= existing.version:
                    return marketplace_pb2.PutResponse(
                        success=False,
                        current_version=existing.version,
                        message="Stale write rejected",
                    )

            self.items_by_id[item_id] = request.item
            return marketplace_pb2.PutResponse(
                success=True,
                current_version=request.item.version,
                message="Item stored",
            )

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