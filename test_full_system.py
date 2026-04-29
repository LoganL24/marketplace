import grpc
from proto.src import marketplace_pb2 as pb2
from proto.src import marketplace_pb2_grpc as pb2_grpc

def run_test():
    # Connect to the SERVICE NODE (Port 50053)
    channel = grpc.insecure_channel('localhost:50053')
    stub = pb2_grpc.MarketplaceStub(channel)

    print("🚀 Starting Full System Integration Test...")

    # 1. Test CreateItem
    print("\n📝 Phase 1: Creating Item...")
    item = pb2.Item(
        item_id="item_001",
        seller_id="user_A",
        title="Gaming Laptop",
        description="High performance",
        starting_price=1200.0,
        current_price=1200.0,
        quantity=1
    )
    create_res = stub.CreateItem(pb2.CreateItemRequest(item=item))
    print(f"Result: {create_res.message} (OK: {create_res.ok})")

    # 2. Test UpdateItem (Success Case)
    print("\n🆙 Phase 2: Updating Item (Correct Version)...")
    update_res = stub.UpdateItem(pb2.UpdateItemRequest(
        item_id="item_001",
        seller_id="user_A",
        description="Updated: High performance + Free Mouse",
        quantity=1,
        status="ACTIVE",
        expected_version=1 # Initial version is usually 0
    ))
    print(f"Result: {update_res.message} (New Version: {update_res.new_version})")

    # 3. Test UpdateItem (Conflict Case)
    print("\n⚠️ Phase 3: Testing Optimistic Locking (Wrong Version)...")
    fail_res = stub.UpdateItem(pb2.UpdateItemRequest(
        item_id="item_001",
        seller_id="user_A",
        description="This should fail",
        expected_version=0 # We already updated it to 1, so 0 is now old
    ))
    print(f"Result: {fail_res.message} (OK: {fail_res.ok})")

if __name__ == "__main__":
    run_test()