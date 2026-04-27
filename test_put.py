import grpc
from proto.src import marketplace_pb2, marketplace_pb2_grpc

def run():
    # Connect to the PRIMARY (50051)
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = marketplace_pb2_grpc.StorageReplicaStub(channel)
        
        # Create a dummy item
        item = marketplace_pb2.Item(
            item_id="1", title="MacBook Pro", category="Tech", 
            description="M3 Chip", starting_price=1999.99, quantity=1, version=1
        )
        
        print("Sending PutItem to Primary...")
        response = stub.PutItem(marketplace_pb2.PutRequest(item=item))
        print(f"Response from Primary: {response.message}")

if __name__ == "__main__":
    run()