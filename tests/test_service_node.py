"""
Unit tests for ServiceNode.

gRPC channels are mocked so no live servers are required.
"""
import os
from unittest.mock import MagicMock, patch, call

import grpc
import pytest

from proto.src import marketplace_pb2 as pb2
from proto.src import marketplace_pb2_grpc as pb2_grpc
from src.service_node import ServiceNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _item(item_id="item-1", title="Phone", category="Electronics",
          description="Latest model", starting_price=800.0, current_price=750.0,
          quantity=5, version=1):
    return pb2.Item(
        item_id=item_id, title=title, category=category,
        description=description, starting_price=starting_price,
        current_price=current_price, quantity=quantity, version=version,
    )


def _mock_grpc_error():
    """Return a grpc.RpcError instance suitable as a side_effect."""
    return grpc.RpcError()


def _controller_channel(primary_address="storage-primary:50051",
                         success=True,
                         cluster_addrs=None):
    """Return a mocked channel whose ControllerStub returns canned responses."""
    primary_resp = MagicMock()
    primary_resp.success = success
    primary_resp.primary_address = primary_address

    cluster_resp = MagicMock()
    cluster_resp.success = success
    cluster_resp.node_addresses = cluster_addrs or []

    mock_stub = MagicMock()
    mock_stub.GetPrimary.return_value = primary_resp
    mock_stub.GetClusterInfo.return_value = cluster_resp

    mock_ch = MagicMock()
    mock_ch.__enter__ = MagicMock(return_value=mock_ch)
    mock_ch.__exit__ = MagicMock(return_value=False)
    return mock_ch, mock_stub


# ---------------------------------------------------------------------------
# _get_primary_address
# ---------------------------------------------------------------------------

class TestGetPrimaryAddress:
    def test_returns_primary_on_success(self):
        svc = ServiceNode()
        mock_ch, mock_stub = _controller_channel("storage:50051")

        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=mock_stub):
                addr = svc._get_primary_address()

        assert addr == "storage:50051"

    def test_returns_none_when_controller_says_not_success(self):
        svc = ServiceNode()
        mock_ch, mock_stub = _controller_channel(success=False)

        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=mock_stub):
                addr = svc._get_primary_address()

        assert addr is None

    def test_returns_none_on_network_exception(self):
        svc = ServiceNode()
        with patch("grpc.insecure_channel", side_effect=Exception("timeout")):
            addr = svc._get_primary_address()
        assert addr is None


# ---------------------------------------------------------------------------
# _get_all_storage_addresses
# ---------------------------------------------------------------------------

class TestGetAllStorageAddresses:
    def test_returns_list_from_cluster_info(self):
        svc = ServiceNode()
        addrs = ["node1:50051", "node2:50052"]
        mock_ch, mock_stub = _controller_channel(cluster_addrs=addrs)

        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=mock_stub):
                result = svc._get_all_storage_addresses()

        assert result == addrs

    def test_returns_empty_on_controller_failure(self):
        svc = ServiceNode()
        mock_ch, mock_stub = _controller_channel(success=False)

        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=mock_stub):
                result = svc._get_all_storage_addresses()

        assert result == []

    def test_returns_empty_on_network_exception(self):
        svc = ServiceNode()
        with patch("grpc.insecure_channel", side_effect=Exception("no conn")):
            result = svc._get_all_storage_addresses()
        assert result == []


# ---------------------------------------------------------------------------
# PutItem
# ---------------------------------------------------------------------------

class TestPutItem:
    def _storage_channel(self, put_success=True):
        put_resp = MagicMock(spec=pb2.PutResponse)
        put_resp.success = put_success
        put_resp.message = "ok" if put_success else "fail"

        mock_stub = MagicMock()
        mock_stub.PutItem.return_value = put_resp

        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)
        return mock_ch, mock_stub

    def test_put_item_forwarded_to_primary(self):
        svc = ServiceNode()
        ctx = MagicMock()
        item = _item()

        ctrl_ch, ctrl_stub = _controller_channel("primary:50051")
        stor_ch, stor_stub = self._storage_channel(put_success=True)

        def channel_factory(addr):
            if "50050" in addr or addr == os.getenv("CONTROLLER_ADDRESS",
                                                     "localhost:50050"):
                return ctrl_ch
            return stor_ch

        with patch("grpc.insecure_channel", side_effect=channel_factory):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=ctrl_stub):
                with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                           return_value=stor_stub):
                    resp = svc.CreateItem(pb2.CreateItemRequest(item=item), ctx)

        assert resp.ok is True
        stor_stub.PutItem.assert_called_once()

    def test_put_item_returns_unavailable_when_no_primary(self):
        svc = ServiceNode()
        ctx = MagicMock()

        ctrl_ch, ctrl_stub = _controller_channel(success=False)

        with patch("grpc.insecure_channel", return_value=ctrl_ch):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=ctrl_stub):
                resp = svc.CreateItem(pb2.CreateItemRequest(item=_item()), ctx)

        assert resp.ok is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNAVAILABLE)

    def test_put_item_returns_unavailable_when_primary_unreachable(self):
        svc = ServiceNode()
        ctx = MagicMock()

        ctrl_ch, ctrl_stub = _controller_channel("primary:50051")

        rpc_err = _mock_grpc_error()

        mock_stor_stub = MagicMock()
        mock_stor_stub.PutItem.side_effect = rpc_err

        dead_ch = MagicMock()
        dead_ch.__enter__ = MagicMock(return_value=dead_ch)
        dead_ch.__exit__ = MagicMock(return_value=False)

        def channel_factory(addr):
            return ctrl_ch if "50050" in addr else dead_ch

        with patch("grpc.insecure_channel", side_effect=channel_factory):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=ctrl_stub):
                with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                           return_value=mock_stor_stub):
                    resp = svc.CreateItem(pb2.CreateItemRequest(item=_item()), ctx)

        assert resp.ok is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNAVAILABLE)


# ---------------------------------------------------------------------------
# QueryItems
# ---------------------------------------------------------------------------

class TestQueryItems:
    def _storage_channel(self, items=None):
        qresp = MagicMock(spec=pb2.QueryResponse)
        qresp.ok = True
        qresp.items = items or []
        qresp.items_found = len(items or [])

        mock_stub = MagicMock()
        mock_stub.QueryItems.return_value = qresp

        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)
        return mock_ch, mock_stub

    def test_query_reads_from_any_replica(self):
        svc = ServiceNode()
        ctx = MagicMock()
        expected_items = [_item()]

        cluster_addrs = ["replica1:50051", "replica2:50052"]
        ctrl_ch, ctrl_stub = _controller_channel(
            cluster_addrs=cluster_addrs)
        stor_ch, stor_stub = self._storage_channel(items=expected_items)

        def channel_factory(addr):
            return ctrl_ch if "50050" in addr else stor_ch

        with patch("grpc.insecure_channel", side_effect=channel_factory):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=ctrl_stub):
                with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                           return_value=stor_stub):
                    resp = svc.QueryItems(pb2.QueryRequest(filter="phone"), ctx)

        assert resp.ok is True

    def test_query_falls_back_to_primary_when_no_cluster_info(self):
        svc = ServiceNode()
        ctx = MagicMock()

        # cluster info returns empty, primary returns an address
        cluster_resp = MagicMock()
        cluster_resp.success = False
        cluster_resp.node_addresses = []

        primary_resp = MagicMock()
        primary_resp.success = True
        primary_resp.primary_address = "primary:50051"

        ctrl_stub = MagicMock()
        ctrl_stub.GetClusterInfo.return_value = cluster_resp
        ctrl_stub.GetPrimary.return_value = primary_resp

        ctrl_ch = MagicMock()
        ctrl_ch.__enter__ = MagicMock(return_value=ctrl_ch)
        ctrl_ch.__exit__ = MagicMock(return_value=False)

        stor_ch, stor_stub = self._storage_channel(items=[_item()])

        def channel_factory(addr):
            return ctrl_ch if "50050" in addr else stor_ch

        with patch("grpc.insecure_channel", side_effect=channel_factory):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=ctrl_stub):
                with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                           return_value=stor_stub):
                    resp = svc.QueryItems(pb2.QueryRequest(filter=""), ctx)

        assert resp.ok is True

    def test_query_returns_unavailable_when_all_replicas_fail(self):
        svc = ServiceNode()
        ctx = MagicMock()

        cluster_addrs = ["replica1:50051"]
        ctrl_ch, ctrl_stub = _controller_channel(cluster_addrs=cluster_addrs)

        rpc_err = _mock_grpc_error()
        dead_stub = MagicMock()
        dead_stub.QueryItems.side_effect = rpc_err

        dead_ch = MagicMock()
        dead_ch.__enter__ = MagicMock(return_value=dead_ch)
        dead_ch.__exit__ = MagicMock(return_value=False)

        def channel_factory(addr):
            return ctrl_ch if "50050" in addr else dead_ch

        with patch("grpc.insecure_channel", side_effect=channel_factory):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=ctrl_stub):
                with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                           return_value=dead_stub):
                    resp = svc.QueryItems(pb2.QueryRequest(filter=""), ctx)

        assert resp.ok is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNAVAILABLE)

    def test_query_returns_unavailable_when_no_nodes_at_all(self):
        svc = ServiceNode()
        ctx = MagicMock()

        # Both cluster info and primary return failure
        ctrl_stub = MagicMock()
        ctrl_stub.GetClusterInfo.return_value = MagicMock(
            success=False, node_addresses=[])
        ctrl_stub.GetPrimary.return_value = MagicMock(
            success=False, primary_address="")

        ctrl_ch = MagicMock()
        ctrl_ch.__enter__ = MagicMock(return_value=ctrl_ch)
        ctrl_ch.__exit__ = MagicMock(return_value=False)

        with patch("grpc.insecure_channel", return_value=ctrl_ch):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=ctrl_stub):
                resp = svc.QueryItems(pb2.QueryRequest(filter=""), ctx)

        assert resp.ok is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNAVAILABLE)

    def test_query_tries_next_replica_on_failure(self):
        """ServiceNode tries all replicas before giving up."""
        svc = ServiceNode()
        ctx = MagicMock()

        cluster_addrs = ["dead:50051", "alive:50052"]
        ctrl_ch, ctrl_stub = _controller_channel(cluster_addrs=cluster_addrs)

        rpc_err = _mock_grpc_error()
        call_count = {"n": 0}
        expected_items = [_item()]

        def make_stub(_ch):
            call_count["n"] += 1
            stub = MagicMock()
            if call_count["n"] == 1:
                stub.QueryItems.side_effect = rpc_err
            else:
                qresp = MagicMock(spec=pb2.QueryResponse)
                qresp.ok = True
                qresp.items = expected_items
                qresp.items_found = 1
                stub.QueryItems.return_value = qresp
            return stub

        stor_ch = MagicMock()
        stor_ch.__enter__ = MagicMock(return_value=stor_ch)
        stor_ch.__exit__ = MagicMock(return_value=False)

        def channel_factory(addr):
            return ctrl_ch if "50050" in addr else stor_ch

        with patch("grpc.insecure_channel", side_effect=channel_factory):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=ctrl_stub):
                with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                           side_effect=make_stub):
                    resp = svc.QueryItems(pb2.QueryRequest(filter=""), ctx)

        assert resp.ok is True
        assert call_count["n"] == 2  # tried both replicas
