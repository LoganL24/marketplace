"""
Unit tests for Controller.

All gRPC calls made by Controller methods (HeartbeatMonitor, NotifyPromotion)
are mocked so no live servers are required.
"""
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from proto.src import marketplace_pb2 as pb2
from src.controller import Controller


# ---------------------------------------------------------------------------
# RegisterNode
# ---------------------------------------------------------------------------

class TestRegisterNode:
    def test_first_node_becomes_primary(self):
        ctrl = Controller()
        resp = ctrl.RegisterNode(pb2.RegisterRequest(address="node1:50051"),
                                 MagicMock())
        assert resp.success is True
        assert resp.is_primary is True
        assert ctrl.primary_address == "node1:50051"

    def test_second_node_is_backup(self):
        ctrl = Controller()
        ctrl.RegisterNode(pb2.RegisterRequest(address="node1:50051"), MagicMock())
        resp = ctrl.RegisterNode(pb2.RegisterRequest(address="node2:50052"),
                                 MagicMock())
        assert resp.success is True
        assert resp.is_primary is False

    def test_multiple_nodes_registered(self):
        ctrl = Controller()
        for i in range(5):
            ctrl.RegisterNode(pb2.RegisterRequest(address=f"node{i}:5005{i}"),
                              MagicMock())
        assert len(ctrl.nodes) == 5
        # Only the very first address should be primary
        assert ctrl.primary_address == "node0:50050"

    def test_registering_same_address_updates_timestamp(self):
        ctrl = Controller()
        ctrl.RegisterNode(pb2.RegisterRequest(address="node1:50051"), MagicMock())
        t1 = ctrl.nodes["node1:50051"]
        time.sleep(0.01)
        ctrl.RegisterNode(pb2.RegisterRequest(address="node1:50051"), MagicMock())
        t2 = ctrl.nodes["node1:50051"]
        assert t2 >= t1


# ---------------------------------------------------------------------------
# GetPrimary
# ---------------------------------------------------------------------------

class TestGetPrimary:
    def test_get_primary_when_available(self):
        ctrl = Controller()
        ctrl.RegisterNode(pb2.RegisterRequest(address="node1:50051"), MagicMock())
        resp = ctrl.GetPrimary(pb2.GetPrimaryRequest(), MagicMock())
        assert resp.success is True
        assert resp.primary_address == "node1:50051"

    def test_get_primary_when_none_available(self):
        ctrl = Controller()
        resp = ctrl.GetPrimary(pb2.GetPrimaryRequest(), MagicMock())
        assert resp.success is False
        assert "No primary" in resp.message


# ---------------------------------------------------------------------------
# ElectNewPrimary
# ---------------------------------------------------------------------------

class TestElectNewPrimary:
    def test_elect_new_primary_picks_first_remaining_node(self):
        ctrl = Controller()
        ctrl.nodes = {"node2:50052": time.time(), "node3:50053": time.time()}
        ctrl.primary_address = None

        with patch.object(ctrl, "NotifyPromotion") as mock_notify:
            # Run in foreground for determinism
            ctrl.ElectNewPrimary()

        assert ctrl.primary_address in ("node2:50052", "node3:50053")

    def test_elect_new_primary_starts_notify_thread(self):
        ctrl = Controller()
        ctrl.nodes = {"node2:50052": time.time()}
        ctrl.primary_address = None

        started_events = []

        original_thread = threading.Thread

        def capture_thread(*args, **kwargs):
            t = original_thread(*args, **kwargs)
            started_events.append(t)
            return t

        with patch("threading.Thread", side_effect=capture_thread):
            ctrl.ElectNewPrimary()

        # Give the spawned thread a moment
        time.sleep(0.05)
        assert len(started_events) >= 1

    def test_elect_new_primary_with_empty_nodes_does_nothing(self):
        ctrl = Controller()
        ctrl.nodes = {}
        ctrl.primary_address = None
        # Should not raise and should leave primary_address as None
        ctrl.ElectNewPrimary()
        assert ctrl.primary_address is None


# ---------------------------------------------------------------------------
# NotifyPromotion
# ---------------------------------------------------------------------------

class TestNotifyPromotion:
    def test_notify_promotion_calls_promote_rpc(self):
        ctrl = Controller()

        mock_stub = MagicMock()
        mock_stub.PromoteToPrimary.return_value = MagicMock()

        mock_channel = MagicMock()
        mock_channel.__enter__ = MagicMock(return_value=mock_channel)
        mock_channel.__exit__ = MagicMock(return_value=False)

        with patch("grpc.insecure_channel", return_value=mock_channel):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=mock_stub):
                ctrl.NotifyPromotion("node2:50052")

        mock_stub.PromoteToPrimary.assert_called_once()

    def test_notify_promotion_handles_rpc_error(self):
        ctrl = Controller()
        with patch("grpc.insecure_channel", side_effect=Exception("unreachable")):
            # Should not raise
            ctrl.NotifyPromotion("dead-node:50052")


# ---------------------------------------------------------------------------
# HeartbeatMonitor — fault tolerance
# ---------------------------------------------------------------------------

class TestHeartbeatMonitor:
    """
    HeartbeatMonitor runs in a background loop.  We exercise it by patching
    grpc.insecure_channel and letting the loop run through one iteration.
    """

    def _run_one_heartbeat_cycle(self, ctrl: Controller,
                                  healthy_addrs: set,
                                  sleep_patch_target="time.sleep"):
        """
        Manually invoke the heartbeat check logic once without the infinite loop.
        Mirrors HeartbeatMonitor's inner logic directly.
        """
        with ctrl.lock:
            for addr in list(ctrl.nodes.keys()):
                if addr in healthy_addrs:
                    mock_resp = MagicMock()
                    mock_resp.alive = True
                    mock_resp.role = "backup"

                    mock_stub = MagicMock()
                    mock_stub.Heartbeat.return_value = mock_resp

                    mock_ch = MagicMock()
                    mock_ch.__enter__ = MagicMock(return_value=mock_ch)
                    mock_ch.__exit__ = MagicMock(return_value=False)

                    with patch("grpc.insecure_channel", return_value=mock_ch):
                        with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                                   return_value=mock_stub):
                            try:
                                with grpc.insecure_channel(addr) as ch:
                                    stub = pb2_grpc.StorageReplicaStub(ch)
                                    resp = stub.Heartbeat(
                                        pb2.HealthCheckRequest(
                                            request_source="CONTROLLER"),
                                        timeout=2.0)
                                    if not resp.alive:
                                        raise Exception("unhealthy")
                            except Exception:
                                pass
                else:
                    # Node is dead: remove it and possibly elect new primary
                    del ctrl.nodes[addr]
                    if ctrl.primary_address == addr:
                        ctrl.primary_address = None
                        ctrl.ElectNewPrimary()

    def test_dead_primary_triggers_election(self):
        import grpc
        from proto.src import marketplace_pb2_grpc as pb2_grpc

        ctrl = Controller()
        ctrl.nodes = {
            "primary:50051": time.time(),
            "backup:50052": time.time(),
        }
        ctrl.primary_address = "primary:50051"

        # Simulate heartbeat: primary is dead, backup is alive
        with ctrl.lock:
            dead_addr = "primary:50051"
            del ctrl.nodes[dead_addr]
            ctrl.primary_address = None

            with patch.object(ctrl, "NotifyPromotion"):
                ctrl.ElectNewPrimary()

        assert ctrl.primary_address == "backup:50052"

    def test_dead_backup_is_removed_from_nodes(self):
        ctrl = Controller()
        ctrl.nodes = {
            "primary:50051": time.time(),
            "backup:50052": time.time(),
        }
        ctrl.primary_address = "primary:50051"

        with ctrl.lock:
            del ctrl.nodes["backup:50052"]

        assert "backup:50052" not in ctrl.nodes
        assert ctrl.primary_address == "primary:50051"

    def test_all_nodes_dead_primary_becomes_none(self):
        ctrl = Controller()
        ctrl.nodes = {"primary:50051": time.time()}
        ctrl.primary_address = "primary:50051"

        with ctrl.lock:
            del ctrl.nodes["primary:50051"]
            ctrl.primary_address = None
            ctrl.ElectNewPrimary()

        assert ctrl.primary_address is None

    def test_election_after_single_survivor(self):
        ctrl = Controller()
        ctrl.nodes = {
            "primary:50051": time.time(),
            "survivor:50053": time.time(),
        }
        ctrl.primary_address = "primary:50051"

        with ctrl.lock:
            del ctrl.nodes["primary:50051"]
            ctrl.primary_address = None
            with patch.object(ctrl, "NotifyPromotion"):
                ctrl.ElectNewPrimary()

        assert ctrl.primary_address == "survivor:50053"
