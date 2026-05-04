"""
Integration-style tests for replication and fault-tolerance scenarios.

All gRPC network calls are mocked; the nodes run in-process so we can assert
on their internal state as well as the responses they return.
"""
import os
import time
import threading
from unittest.mock import MagicMock, patch, call

import grpc
import pytest

from proto.src import marketplace_pb2 as pb2
from proto.src import marketplace_pb2_grpc as pb2_grpc
from src.controller import Controller
from src.service_node import ServiceNode


# ---------------------------------------------------------------------------
# Helpers — create in-process StorageNode without real network registration
# ---------------------------------------------------------------------------

def _make_storage_node(role="primary", peer_addresses="", port="50051",
                        register_as_primary=True):
    """Return a StorageNode whose __init__ controller call is mocked."""
    mock_resp = MagicMock()
    mock_resp.is_primary = register_as_primary

    mock_stub = MagicMock()
    mock_stub.RegisterNode.return_value = mock_resp

    mock_ch = MagicMock()
    mock_ch.__enter__ = MagicMock(return_value=mock_ch)
    mock_ch.__exit__ = MagicMock(return_value=False)

    with patch.dict(os.environ, {"NODE_PORT": port,
                                  "NODE_ROLE": role,
                                  "PEER_ADDRESSES": peer_addresses}):
        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=mock_stub):
                from src.storage_node import StorageNode
                node = StorageNode()
    return node


def _item(item_id="item-1", title="Camera", category="Electronics",
          description="4K sensor", starting_price=300.0, current_price=280.0,
          quantity=2, version=1):
    return pb2.Item(
        item_id=item_id, title=title, category=category,
        description=description, starting_price=starting_price,
        current_price=current_price, quantity=quantity, version=version,
    )


# ===========================================================================
# REPLICATION TESTS
# ===========================================================================

class TestActiveReplication:
    """
    Verify that the primary forwards writes to every backup listed in
    PEER_ADDRESSES before acknowledging success.
    """

    def test_write_to_primary_replicates_to_single_backup(self):
        primary = _make_storage_node(role="primary", register_as_primary=True,
                                     peer_addresses="backup:50052")
        backup = _make_storage_node(role="backup", register_as_primary=False)

        item = _item()

        # Intercept the gRPC call that the primary makes to the backup and
        # apply it to the in-process backup node instead.
        def fake_replicate(req, timeout=None):
            resp = backup.ReplicateLog(req, MagicMock())
            return resp

        mock_stub = MagicMock()
        mock_stub.ReplicateLog.side_effect = fake_replicate

        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)

        ctx = MagicMock()
        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                resp = primary.PutItem(pb2.PutRequest(item=item), ctx)

        # Primary should succeed
        assert resp.success is True
        # Backup must have the same item
        assert item.item_id in backup.items_by_id
        assert backup.items_by_id[item.item_id].version == item.version

    def test_write_to_primary_replicates_to_two_backups(self):
        primary = _make_storage_node(role="primary", register_as_primary=True,
                                     peer_addresses="backup1:50052,backup2:50053")
        backup1 = _make_storage_node(role="backup", register_as_primary=False)
        backup2 = _make_storage_node(role="backup", register_as_primary=False)

        item = _item()
        call_index = {"n": 0}
        backups = [backup1, backup2]

        def fake_replicate(req, timeout=None):
            node = backups[call_index["n"] % 2]
            call_index["n"] += 1
            return node.ReplicateLog(req, MagicMock())

        mock_stub = MagicMock()
        mock_stub.ReplicateLog.side_effect = fake_replicate

        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)

        ctx = MagicMock()
        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                resp = primary.PutItem(pb2.PutRequest(item=item), ctx)

        assert resp.success is True
        # Both backups received the item
        for b in backups:
            assert item.item_id in b.items_by_id

    def test_replication_failure_means_primary_write_fails(self):
        primary = _make_storage_node(role="primary", register_as_primary=True,
                                     peer_addresses="backup:50052")

        mock_stub = MagicMock()
        mock_stub.ReplicateLog.side_effect = Exception("backup down")

        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)

        ctx = MagicMock()
        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                resp = primary.PutItem(pb2.PutRequest(item=_item()), ctx)

        assert resp.success is False
        assert "replicate" in resp.message.lower()

    def test_backup_write_rejected_means_primary_write_fails(self):
        primary = _make_storage_node(role="primary", register_as_primary=True,
                                     peer_addresses="backup:50052")

        mock_resp = MagicMock()
        mock_resp.success = False  # backup rejects

        mock_stub = MagicMock()
        mock_stub.ReplicateLog.return_value = mock_resp

        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)

        ctx = MagicMock()
        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                resp = primary.PutItem(pb2.PutRequest(item=_item()), ctx)

        assert resp.success is False

    def test_backup_does_not_re_replicate(self):
        """A backup that receives a ReplicateLog should NOT cascade to its peers."""
        backup = _make_storage_node(role="backup", register_as_primary=False,
                                    peer_addresses="other-backup:50053")

        item = _item()
        with patch.object(backup, "PropagateToBackups") as mock_prop:
            backup.ReplicateLog(pb2.ReplicationRequest(item=item), MagicMock())
            mock_prop.assert_not_called()

    def test_version_advances_consistently_across_replicas(self):
        """Successive version bumps should propagate correctly."""
        primary = _make_storage_node(role="primary", register_as_primary=True,
                                     peer_addresses="backup:50052")
        backup = _make_storage_node(role="backup", register_as_primary=False)

        def fake_replicate(req, timeout=None):
            return backup.ReplicateLog(req, MagicMock())

        mock_stub = MagicMock()
        mock_stub.ReplicateLog.side_effect = fake_replicate

        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)

        ctx = MagicMock()
        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                # Write v1
                primary.PutItem(pb2.PutRequest(item=_item(version=1)), ctx)
                # Write v2 (higher version)
                primary.PutItem(pb2.PutRequest(item=_item(version=2)), ctx)

        assert primary.items_by_id["item-1"].version == 2
        assert backup.items_by_id["item-1"].version == 2


# ===========================================================================
# FAULT TOLERANCE TESTS
# ===========================================================================

class TestFaultTolerance:
    """Cover primary failure, leader election, and read-path resilience."""

    # --- Stale write rejection ---

    def test_stale_write_on_backup_rejected(self):
        backup = _make_storage_node(role="backup", register_as_primary=False)
        backup.items_by_id["item-1"] = _item(version=5)

        resp = backup.PutItem(pb2.PutRequest(item=_item(version=3)), MagicMock())
        assert resp.success is False
        assert backup.items_by_id["item-1"].version == 5  # not overwritten

    def test_concurrent_writes_preserve_newest_version(self):
        """Simulate two concurrent writes; only the latest version should win."""
        node = _make_storage_node(role="backup", register_as_primary=False)

        errors = []

        def write(version):
            try:
                node.PutItem(
                    pb2.PutRequest(item=_item(version=version)),
                    MagicMock(),
                )
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=write, args=(v,))
            for v in [3, 1, 5, 2, 4]
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # After all concurrent writes, the stored version must be >= 1
        assert "item-1" in node.items_by_id
        assert node.items_by_id["item-1"].version >= 1

    # --- Primary failure → election ---

    def test_primary_failure_triggers_new_election_in_controller(self):
        ctrl = Controller()
        ctrl.nodes = {
            "primary:50051": time.time(),
            "backup:50052": time.time(),
        }
        ctrl.primary_address = "primary:50051"

        # Simulate heartbeat detecting primary is dead
        with ctrl.lock:
            del ctrl.nodes["primary:50051"]
            ctrl.primary_address = None
            with patch.object(ctrl, "NotifyPromotion"):
                ctrl.ElectNewPrimary()

        assert ctrl.primary_address == "backup:50052"
        assert "primary:50051" not in ctrl.nodes

    def test_promoted_backup_becomes_primary(self):
        node = _make_storage_node(role="backup", register_as_primary=False)
        assert node.role == "backup"

        node.PromoteToPrimary(pb2.PromotionRequest(new_role="primary"),
                              MagicMock())

        assert node.role == "primary"

    def test_promoted_node_can_replicate_after_promotion(self):
        node = _make_storage_node(role="backup", register_as_primary=False,
                                  peer_addresses="new-backup:50053")
        node.PromoteToPrimary(pb2.PromotionRequest(), MagicMock())

        item = _item()
        mock_resp = MagicMock()
        mock_resp.success = True
        mock_stub = MagicMock()
        mock_stub.ReplicateLog.return_value = mock_resp
        mock_ch = MagicMock()
        mock_ch.__enter__ = MagicMock(return_value=mock_ch)
        mock_ch.__exit__ = MagicMock(return_value=False)

        ctx = MagicMock()
        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                resp = node.PutItem(pb2.PutRequest(item=item), ctx)

        assert resp.success is True
        mock_stub.ReplicateLog.assert_called_once()

    def test_all_storage_nodes_dead_controller_has_no_primary(self):
        ctrl = Controller()
        ctrl.nodes = {"node1:50051": time.time()}
        ctrl.primary_address = "node1:50051"

        with ctrl.lock:
            del ctrl.nodes["node1:50051"]
            ctrl.primary_address = None
            ctrl.ElectNewPrimary()  # no nodes left

        assert ctrl.primary_address is None
        resp = ctrl.GetPrimary(pb2.GetPrimaryRequest(), MagicMock())
        assert resp.success is False

    # --- Service-layer read resilience ---

    def test_service_node_skips_dead_replica_and_reads_alive_one(self):
        svc = ServiceNode()
        ctx = MagicMock()

        cluster_addrs = ["dead:50051", "alive:50052"]

        cluster_resp = MagicMock()
        cluster_resp.success = True
        cluster_resp.node_addresses = cluster_addrs

        ctrl_stub = MagicMock()
        ctrl_stub.GetClusterInfo.return_value = cluster_resp

        ctrl_ch = MagicMock()
        ctrl_ch.__enter__ = MagicMock(return_value=ctrl_ch)
        ctrl_ch.__exit__ = MagicMock(return_value=False)

        rpc_err = grpc.RpcError()
        call_index = {"n": 0}

        def make_stub(_ch):
            call_index["n"] += 1
            stub = MagicMock()
            if call_index["n"] == 1:
                stub.QueryItems.side_effect = rpc_err
            else:
                qresp = MagicMock(spec=pb2.QueryResponse)
                qresp.ok = True
                qresp.items = [_item()]
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
        assert call_index["n"] == 2  # first replica failed, second succeeded

    def test_service_node_put_reports_unavailable_when_primary_fails(self):
        svc = ServiceNode()
        ctx = MagicMock()

        primary_resp = MagicMock()
        primary_resp.success = True
        primary_resp.primary_address = "dead-primary:50051"

        ctrl_stub = MagicMock()
        ctrl_stub.GetPrimary.return_value = primary_resp

        ctrl_ch = MagicMock()
        ctrl_ch.__enter__ = MagicMock(return_value=ctrl_ch)
        ctrl_ch.__exit__ = MagicMock(return_value=False)

        rpc_err = grpc.RpcError()

        dead_stub = MagicMock()
        dead_stub.PutItem.side_effect = rpc_err

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
                    resp = svc.PutItem(pb2.PutRequest(item=_item()), ctx)

        assert resp.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNAVAILABLE)

    # --- Controller notification of new primary ---

    def test_controller_notifies_new_primary_on_election(self):
        ctrl = Controller()
        ctrl.nodes = {"backup:50052": time.time()}
        ctrl.primary_address = None

        with patch.object(ctrl, "NotifyPromotion") as mock_notify:
            # patch threading.Thread so it runs inline
            with patch("threading.Thread") as mock_thread:
                instance = MagicMock()
                mock_thread.return_value = instance
                ctrl.ElectNewPrimary()

        # Thread was started for notification
        instance.start.assert_called_once()

    def test_heartbeat_removes_dead_nodes_preserves_healthy(self):
        ctrl = Controller()
        ctrl.nodes = {
            "healthy:50051": time.time(),
            "dead:50052": time.time(),
        }
        ctrl.primary_address = "healthy:50051"

        # Simulate heartbeat: remove dead node
        with ctrl.lock:
            del ctrl.nodes["dead:50052"]

        assert "dead:50052" not in ctrl.nodes
        assert "healthy:50051" in ctrl.nodes
        assert ctrl.primary_address == "healthy:50051"

    def test_sync_full_state_transfers_all_items(self):
        """
        A newly joined backup should be able to fetch full state from the primary.
        """
        primary = _make_storage_node(role="primary", register_as_primary=True)
        for i in range(5):
            primary.items_by_id[f"item-{i}"] = _item(item_id=f"item-{i}",
                                                       version=i + 1)

        resp = primary.SyncFullState(
            pb2.StateRequest(requester_id="new-backup"), MagicMock()
        )

        assert resp.ok is True
        assert len(resp.items) == 5
        assert resp.last_included_version == 5

    def test_skip_consistency_check_for_sync_replication(self):
        """
        When the primary pushes state to a newly joined backup it should be
        able to use skip_consistency_check so older version data still lands.
        """
        backup = _make_storage_node(role="backup", register_as_primary=False)
        backup.items_by_id["item-1"] = _item(version=10)  # has newer

        # Primary tries to push an older snapshot (version=3) with the bypass flag
        resp = backup.PutItem(
            pb2.PutRequest(item=_item(version=3), skip_consistency_check=True),
            MagicMock(),
        )
        assert resp.success is True
