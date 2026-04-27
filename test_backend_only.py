import grpc
from proto.src import marketplace_pb2 as pb2
from proto.src import marketplace_pb2_grpc as pb2_grpc

def test_flow():
    print("🧠 Step 1: Asking Controller for the Primary...")
    with grpc.insecure_channel('localhost:50050') as channel:
        stub = pb2_grpc.ControllerStub(channel)
        res = stub.GetPrimary(pb2.GetPrimaryRequest())
        if not res.success:
            print("Controller has no Primary!")
            return
        primary_addr = res.primary_address
        print(f"Controller says Primary is at: {primary_addr}")

    print(f"\n💾 Step 2: Sending data to {primary_addr}...")
    with grpc.insecure_channel(primary_addr) as channel:
        stub = pb2_grpc.StorageReplicaStub(channel)
        item = pb2.Item(title="Backend Test Item", current_price=99.0)
        put_res = stub.PutItem(pb2.PutRequest(item=item))
        if put_res.success:
            print("Storage Node accepted the item!")
        else:
            print(f"Storage failed: {put_res.message}")

if __name__ == "__main__":
    test_flow()