"""
Unit tests for StorageNode.

All gRPC network calls are mocked so the tests run without any live servers.
"""
import os
import threading
from unittest.mock import MagicMock, patch, call

import pytest

from proto.src import marketplace_pb2 as pb2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _item(item_id="item-1", title="Laptop", category="Tech",
          description="Fast", starting_price=500.0, current_price=450.0,
          quantity=3, version=1):
    """Return a populated Item proto."""
    return pb2.Item(
        item_id=item_id,
        title=title,
        category=category,
        description=description,
        starting_price=starting_price,
        current_price=current_price,
        quantity=quantity,
        version=version,
    )


def _make_node(role="primary", peer_addresses="", port="50051",
               register_as_primary=True):
    """
    Instantiate a StorageNode with the controller registration mocked out.

    Parameters
    ----------
    role              : default env-var role if the mock assigns backup
    peer_addresses    : comma-separated peer list (env-var)
    port              : NODE_PORT env-var value
    register_as_primary : what is_primary the fake controller returns
    """
    mock_resp = MagicMock()
    mock_resp.is_primary = register_as_primary

    mock_stub = MagicMock()
    mock_stub.RegisterNode.return_value = mock_resp

    mock_channel = MagicMock()
    mock_channel.__enter__ = MagicMock(return_value=mock_channel)
    mock_channel.__exit__ = MagicMock(return_value=False)

    env_patch = {
        "NODE_PORT": port,
        "NODE_ROLE": role,
        "PEER_ADDRESSES": peer_addresses,
    }

    with patch.dict(os.environ, env_patch):
        with patch("grpc.insecure_channel", return_value=mock_channel):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=mock_stub):
                from src.storage_node import StorageNode
                node = StorageNode()

    return node


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_registers_as_primary_when_controller_says_so(self):
        node = _make_node(register_as_primary=True)
        assert node.role == "primary"

    def test_registers_as_backup_when_controller_says_so(self):
        node = _make_node(register_as_primary=False)
        assert node.role == "backup"

    def test_falls_back_to_env_role_on_connection_error(self):
        with patch.dict(os.environ, {"NODE_PORT": "50051", "NODE_ROLE": "backup",
                                     "PEER_ADDRESSES": ""}):
            with patch("grpc.insecure_channel", side_effect=Exception("conn refused")):
                from src.storage_node import StorageNode
                node = StorageNode()
        assert node.role == "backup"


# ---------------------------------------------------------------------------
# PutItem
# ---------------------------------------------------------------------------

class TestPutItem:
    def test_put_new_item_succeeds(self):
        node = _make_node(role="backup", peer_addresses="")
        ctx = MagicMock()
        item = _item()
        resp = node.PutItem(pb2.PutRequest(item=item), ctx)
        assert resp.success is True
        assert item.item_id in node.items_by_id

    def test_put_item_with_higher_version_overwrites(self):
        node = _make_node(role="backup")
        ctx = MagicMock()
        node.items_by_id["item-1"] = _item(version=1)

        resp = node.PutItem(pb2.PutRequest(item=_item(version=2)), ctx)
        assert resp.success is True
        assert node.items_by_id["item-1"].version == 2

    def test_stale_write_rejected(self):
        node = _make_node(role="backup")
        ctx = MagicMock()
        node.items_by_id["item-1"] = _item(version=5)

        resp = node.PutItem(pb2.PutRequest(item=_item(version=3)), ctx)
        assert resp.success is False
        assert "Stale" in resp.message
        # Store should still hold the newer version
        assert node.items_by_id["item-1"].version == 5

    def test_equal_version_is_also_stale(self):
        node = _make_node(role="backup")
        ctx = MagicMock()
        node.items_by_id["item-1"] = _item(version=2)

        resp = node.PutItem(pb2.PutRequest(item=_item(version=2)), ctx)
        assert resp.success is False

    def test_skip_consistency_check_allows_stale_version(self):
        node = _make_node(role="backup")
        ctx = MagicMock()
        node.items_by_id["item-1"] = _item(version=10)

        resp = node.PutItem(
            pb2.PutRequest(item=_item(version=1), skip_consistency_check=True), ctx
        )
        assert resp.success is True

    def test_primary_replicates_to_backups_on_put(self):
        node = _make_node(role="primary", register_as_primary=True,
                          peer_addresses="backup1:50052")

        mock_resp = MagicMock()
        mock_resp.success = True

        mock_stub = MagicMock()
        mock_stub.ReplicateLog.return_value = mock_resp

        mock_channel = MagicMock()
        mock_channel.__enter__ = MagicMock(return_value=mock_channel)
        mock_channel.__exit__ = MagicMock(return_value=False)

        ctx = MagicMock()
        with patch("grpc.insecure_channel", return_value=mock_channel):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                resp = node.PutItem(pb2.PutRequest(item=_item()), ctx)

        assert resp.success is True
        mock_stub.ReplicateLog.assert_called_once()

    def test_primary_put_fails_when_backup_replication_fails(self):
        node = _make_node(role="primary", register_as_primary=True,
                          peer_addresses="backup1:50052")

        mock_stub = MagicMock()
        mock_stub.ReplicateLog.side_effect = Exception("backup down")

        mock_channel = MagicMock()
        mock_channel.__enter__ = MagicMock(return_value=mock_channel)
        mock_channel.__exit__ = MagicMock(return_value=False)

        ctx = MagicMock()
        with patch("grpc.insecure_channel", return_value=mock_channel):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                resp = node.PutItem(pb2.PutRequest(item=_item()), ctx)

        assert resp.success is False
        assert "replicate" in resp.message.lower()

    def test_backup_does_not_propagate(self):
        node = _make_node(role="backup", register_as_primary=False,
                          peer_addresses="peer:50052")
        ctx = MagicMock()

        with patch.object(node, "PropagateToBackups") as mock_prop:
            node.PutItem(pb2.PutRequest(item=_item()), ctx)
            mock_prop.assert_not_called()


# ---------------------------------------------------------------------------
# QueryItems
# ---------------------------------------------------------------------------

class TestQueryItems:
    def setup_method(self):
        self.node = _make_node(role="backup")

    def _populate(self):
        self.node.items_by_id["item-1"] = _item(
            item_id="item-1", title="Laptop", category="Tech",
            description="Fast machine")
        self.node.items_by_id["item-2"] = _item(
            item_id="item-2", title="Headphones", category="Audio",
            description="Noise cancelling")

    def test_query_all_items_no_filter(self):
        self._populate()
        resp = self.node.QueryItems(pb2.QueryRequest(filter=""), MagicMock())
        assert resp.ok is True
        assert resp.items_found == 2

    def test_query_by_title(self):
        self._populate()
        resp = self.node.QueryItems(pb2.QueryRequest(filter="laptop"), MagicMock())
        assert resp.ok is True
        assert resp.items_found == 1
        assert resp.items[0].item_id == "item-1"

    def test_query_by_category(self):
        self._populate()
        resp = self.node.QueryItems(pb2.QueryRequest(filter="audio"), MagicMock())
        assert resp.ok is True
        assert resp.items_found == 1

    def test_query_by_description(self):
        self._populate()
        resp = self.node.QueryItems(pb2.QueryRequest(filter="noise"), MagicMock())
        assert resp.ok is True
        assert resp.items_found == 1

    def test_query_case_insensitive(self):
        self._populate()
        resp = self.node.QueryItems(pb2.QueryRequest(filter="LAPTOP"), MagicMock())
        assert resp.ok is True
        assert resp.items_found == 1

    def test_query_no_match(self):
        self._populate()
        resp = self.node.QueryItems(pb2.QueryRequest(filter="zzznomatch"), MagicMock())
        assert resp.ok is True
        assert resp.items_found == 0

    def test_query_empty_store(self):
        resp = self.node.QueryItems(pb2.QueryRequest(filter=""), MagicMock())
        assert resp.ok is True
        assert resp.items_found == 0

    def test_query_whitespace_filter_treated_as_empty(self):
        self._populate()
        resp = self.node.QueryItems(pb2.QueryRequest(filter="   "), MagicMock())
        # strip() makes it empty → return all
        assert resp.items_found == 2


# ---------------------------------------------------------------------------
# ReplicateLog
# ---------------------------------------------------------------------------

class TestReplicateLog:
    def test_replicatelog_stores_item(self):
        node = _make_node(role="backup")
        item = _item(item_id="item-99", version=7)
        resp = node.ReplicateLog(pb2.ReplicationRequest(item=item), MagicMock())
        assert resp.success is True
        assert resp.ack_version == 7
        assert node.items_by_id["item-99"].version == 7

    def test_replicatelog_overwrites_existing_item(self):
        node = _make_node(role="backup")
        node.items_by_id["item-1"] = _item(version=1)
        new_item = _item(version=3)
        node.ReplicateLog(pb2.ReplicationRequest(item=new_item), MagicMock())
        assert node.items_by_id["item-1"].version == 3


# ---------------------------------------------------------------------------
# SyncFullState
# ---------------------------------------------------------------------------

class TestSyncFullState:
    def test_sync_empty_store(self):
        node = _make_node(role="primary")
        resp = node.SyncFullState(pb2.StateRequest(requester_id="new-node"), MagicMock())
        assert resp.ok is True
        assert len(resp.items) == 0
        assert resp.last_included_version == 0

    def test_sync_returns_all_items(self):
        node = _make_node(role="primary")
        node.items_by_id["item-1"] = _item(item_id="item-1", version=3)
        node.items_by_id["item-2"] = _item(item_id="item-2", version=7)
        resp = node.SyncFullState(pb2.StateRequest(), MagicMock())
        assert resp.ok is True
        assert len(resp.items) == 2
        assert resp.last_included_version == 7


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

class TestHeartbeat:
    def test_heartbeat_returns_alive(self):
        node = _make_node(role="primary")
        node.items_by_id["item-1"] = _item()
        resp = node.Heartbeat(pb2.HealthCheckRequest(request_source="CONTROLLER"),
                              MagicMock())
        assert resp.alive is True
        assert resp.item_count == 1
        assert resp.role == "primary"

    def test_heartbeat_backup_role_reported(self):
        node = _make_node(role="backup", register_as_primary=False)
        resp = node.Heartbeat(pb2.HealthCheckRequest(), MagicMock())
        assert resp.alive is True
        assert resp.role == "backup"


# ---------------------------------------------------------------------------
# PromoteToPrimary
# ---------------------------------------------------------------------------

class TestPromoteToPrimary:
    def test_promotion_changes_role(self):
        node = _make_node(role="backup", register_as_primary=False)
        assert node.role == "backup"
        resp = node.PromoteToPrimary(pb2.PromotionRequest(new_role="primary"),
                                     MagicMock())
        assert resp.success is True
        assert node.role == "primary"


# ---------------------------------------------------------------------------
# PropagateToBackups
# ---------------------------------------------------------------------------

class TestPropagateToBackups:
    def _node_with_peers(self, peers: str):
        return _make_node(role="primary", register_as_primary=True,
                          peer_addresses=peers)

    def _make_channel_mock(self, repl_success=True):
        mock_resp = MagicMock()
        mock_resp.success = repl_success

        mock_stub = MagicMock()
        mock_stub.ReplicateLog.return_value = mock_resp

        mock_channel = MagicMock()
        mock_channel.__enter__ = MagicMock(return_value=mock_channel)
        mock_channel.__exit__ = MagicMock(return_value=False)
        return mock_channel, mock_stub

    def test_no_peers_returns_true(self):
        node = self._node_with_peers("")
        item = _item()
        result = node.PropagateToBackups(item)
        assert result is True

    def test_single_peer_success(self):
        node = self._node_with_peers("backup1:50052")
        mock_channel, mock_stub = self._make_channel_mock(repl_success=True)
        with patch("grpc.insecure_channel", return_value=mock_channel):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                result = node.PropagateToBackups(_item())
        assert result is True

    def test_single_peer_rejection_returns_false(self):
        node = self._node_with_peers("backup1:50052")
        mock_channel, mock_stub = self._make_channel_mock(repl_success=False)
        with patch("grpc.insecure_channel", return_value=mock_channel):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                result = node.PropagateToBackups(_item())
        assert result is False

    def test_single_peer_connection_error_returns_false(self):
        node = self._node_with_peers("backup1:50052")
        with patch("grpc.insecure_channel", side_effect=Exception("conn refused")):
            result = node.PropagateToBackups(_item())
        assert result is False

    def test_two_peers_both_succeed(self):
        node = self._node_with_peers("backup1:50052,backup2:50053")
        mock_channel, mock_stub = self._make_channel_mock(repl_success=True)
        with patch("grpc.insecure_channel", return_value=mock_channel):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                result = node.PropagateToBackups(_item())
        assert result is True

    def test_two_peers_one_fails_returns_false(self):
        node = self._node_with_peers("backup1:50052,backup2:50053")

        # Simulate mixed success: first peer succeeds, second fails
        responses = [True, False]
        idx = {"i": 0}

        def make_stub(_ch):
            mock_resp = MagicMock()
            mock_resp.success = responses[idx["i"]]
            idx["i"] += 1
            mock_s = MagicMock()
            mock_s.ReplicateLog.return_value = mock_resp
            return mock_s

        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)

        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       side_effect=make_stub):
                result = node.PropagateToBackups(_item())
        assert result is False
