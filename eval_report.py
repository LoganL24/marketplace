"""
Full Statistical Evaluation Report
===================================
Covers:
  1. Unit-test suite results
  2. Service-node operation latency benchmarks
  3. Storage-node PutItem / ReplicateLog / QueryItems benchmarks
  4. Concurrent-write stress test (throughput & latency percentiles)

Run:  python eval_report.py
"""

import os
import subprocess
import sys
import threading
import time
from statistics import mean, median, stdev
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Ensure proto stubs exist
# ---------------------------------------------------------------------------
_proto_dir = os.path.join(os.path.dirname(__file__), "proto", "src")
if not os.path.exists(os.path.join(_proto_dir, "marketplace_pb2.py")):
    subprocess.run(
        [
            sys.executable, "-m", "grpc_tools.protoc",
            f"-I{_proto_dir}",
            f"--python_out={_proto_dir}",
            f"--pyi_out={_proto_dir}",
            f"--grpc_python_out={_proto_dir}",
            os.path.join(_proto_dir, "marketplace.proto"),
        ],
        check=True,
    )
    grpc_file = os.path.join(_proto_dir, "marketplace_pb2_grpc.py")
    with open(grpc_file) as f:
        content = f.read()
    with open(grpc_file, "w") as f:
        f.write(content.replace("import marketplace_pb2", "from . import marketplace_pb2"))

import contextlib
import io

import grpc
from proto.src import marketplace_pb2 as pb2
from proto.src import marketplace_pb2_grpc as pb2_grpc
from src.controller import Controller
from src.service_node import ServiceNode


@contextlib.contextmanager
def _silent():
    """Suppress stdout/stderr from noisy node print statements during benchmarks."""
    with open(os.devnull, "w") as devnull:
        with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
            yield


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _sep(widths):
    return "+" + "+".join("-" * (w + 2) for w in widths) + "+"


def _row(cells, widths):
    return "|" + "|".join(f" {str(c):<{widths[i]}} " for i, c in enumerate(cells)) + "|"


def print_table(title, headers, rows):
    widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
              for i, h in enumerate(headers)]
    sep = _sep(widths)
    print(f"\n{'='*len(sep)}")
    print(f" {title}")
    print('='*len(sep))
    print(sep)
    print(_row(headers, widths))
    print(_sep(widths))
    for row in rows:
        print(_row(row, widths))
    print(sep)


# ---------------------------------------------------------------------------
# Shared mock factories
# ---------------------------------------------------------------------------

def _make_storage_node(role="primary", peer_addresses="", register_as_primary=True):
    mock_resp = MagicMock()
    mock_resp.is_primary = register_as_primary
    mock_stub = MagicMock()
    mock_stub.RegisterNode.return_value = mock_resp
    mock_ch = MagicMock()
    mock_ch.__enter__ = MagicMock(return_value=mock_ch)
    mock_ch.__exit__ = MagicMock(return_value=False)
    with patch.dict(os.environ, {"NODE_PORT": "50051", "NODE_ROLE": role,
                                  "PEER_ADDRESSES": peer_addresses}):
        with patch("grpc.insecure_channel", return_value=mock_ch):
            with patch("proto.src.marketplace_pb2_grpc.ControllerStub",
                       return_value=mock_stub):
                from src.storage_node import StorageNode
                node = StorageNode()
    return node


def _item(item_id="item-1", version=1):
    return pb2.Item(
        item_id=item_id, title="Laptop", category="Tech",
        description="Fast machine", starting_price=500.0,
        current_price=450.0, quantity=3, version=version,
    )


def _mock_channel(put_success=True, query_items=None):
    put_resp = MagicMock()
    put_resp.success = put_success
    put_resp.message = "ok"
    put_resp.current_version = 1

    q_resp = MagicMock()
    q_resp.ok = True
    q_resp.items = query_items or []
    q_resp.items_found = len(query_items or [])

    stub = MagicMock()
    stub.PutItem.return_value = put_resp
    stub.QueryItems.return_value = q_resp

    ch = MagicMock()
    ch.__enter__ = MagicMock(return_value=ch)
    ch.__exit__ = MagicMock(return_value=False)
    return ch, stub


def _ctrl_channel(primary="primary:50051", cluster_addrs=None, success=True):
    pr = MagicMock()
    pr.success = success
    pr.primary_address = primary

    cr = MagicMock()
    cr.success = success
    cr.node_addresses = cluster_addrs or [primary]

    stub = MagicMock()
    stub.GetPrimary.return_value = pr
    stub.GetClusterInfo.return_value = cr

    ch = MagicMock()
    ch.__enter__ = MagicMock(return_value=ch)
    ch.__exit__ = MagicMock(return_value=False)
    return ch, stub


# ---------------------------------------------------------------------------
# 1. Unit-test suite
# ---------------------------------------------------------------------------

def run_test_suite():
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=no", "-q"],
        capture_output=True, text=True,
        cwd=os.path.dirname(__file__),
    )
    output = result.stdout + result.stderr
    passed = failed = errors = skipped = 0
    duration = 0.0
    for line in output.splitlines():
        if " passed" in line:
            parts = line.split()
            for i, p in enumerate(parts):
                if p == "passed":
                    try:
                        passed = int(parts[i - 1])
                    except (ValueError, IndexError):
                        pass
                elif p == "failed":
                    try:
                        failed = int(parts[i - 1])
                    except (ValueError, IndexError):
                        pass
                elif p == "error" or p == "errors":
                    try:
                        errors = int(parts[i - 1])
                    except (ValueError, IndexError):
                        pass
                elif p == "skipped":
                    try:
                        skipped = int(parts[i - 1])
                    except (ValueError, IndexError):
                        pass
        if "passed in" in line or "failed in" in line or "error in" in line:
            try:
                duration = float(line.split("in")[1].strip().rstrip("s").strip())
            except Exception:
                pass
    total = passed + failed + errors + skipped
    pass_rate = f"{passed / total * 100:.1f}%" if total else "N/A"
    return [
        ("Controller tests",                "test_controller.py",                  _count_tests("tests/test_controller.py")),
        ("Storage-node tests",              "test_storage_node.py",                _count_tests("tests/test_storage_node.py")),
        ("Service-node tests",              "test_service_node.py",                _count_tests("tests/test_service_node.py")),
        ("Replication & fault-tolerance",   "test_replication_and_fault_tolerance.py",
                                                                                    _count_tests("tests/test_replication_and_fault_tolerance.py")),
    ], passed, failed, errors, skipped, duration, pass_rate


def _count_tests(filepath):
    result = subprocess.run(
        [sys.executable, "-m", "pytest", filepath, "--collect-only", "-q", "--tb=no"],
        capture_output=True, text=True,
        cwd=os.path.dirname(__file__),
    )
    lines = result.stdout.strip().splitlines()
    for line in reversed(lines):
        if "selected" in line or "test" in line:
            try:
                return int(line.split()[0])
            except (ValueError, IndexError):
                pass
    return "?"


# ---------------------------------------------------------------------------
# 2. Latency benchmark helper
# ---------------------------------------------------------------------------

BENCH_ITERATIONS = 500


def _benchmark(fn, n=BENCH_ITERATIONS):
    latencies = []
    success = 0
    for _ in range(n):
        t0 = time.perf_counter()
        ok = fn()
        elapsed_us = (time.perf_counter() - t0) * 1_000_000  # microseconds
        latencies.append(elapsed_us)
        if ok:
            success += 1
    latencies.sort()
    p50 = latencies[int(0.50 * n) - 1]
    p95 = latencies[int(0.95 * n) - 1]
    p99 = latencies[int(0.99 * n) - 1]
    return {
        "n": n,
        "success": success,
        "success_rate": f"{success / n * 100:.1f}%",
        "mean_us": f"{mean(latencies):.1f}",
        "median_us": f"{median(latencies):.1f}",
        "stdev_us": f"{stdev(latencies):.1f}" if n > 1 else "0.0",
        "p50_us": f"{p50:.1f}",
        "p95_us": f"{p95:.1f}",
        "p99_us": f"{p99:.1f}",
        "min_us": f"{min(latencies):.1f}",
        "max_us": f"{max(latencies):.1f}",
    }


# ---------------------------------------------------------------------------
# 3. Service-node benchmarks
# ---------------------------------------------------------------------------

def bench_service_node():
    svc = ServiceNode()
    ctx = MagicMock()
    ctrl_ch, ctrl_stub = _ctrl_channel("primary:50051", cluster_addrs=["primary:50051"])
    stor_ch, stor_stub = _mock_channel(put_success=True, query_items=[_item()])

    def channel_factory(addr):
        return ctrl_ch if "50050" in addr else stor_ch

    results = {}
    with patch("grpc.insecure_channel", side_effect=channel_factory):
        with patch("proto.src.marketplace_pb2_grpc.ControllerStub", return_value=ctrl_stub):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub", return_value=stor_stub):

                def do_create():
                    r = svc.CreateItem(pb2.CreateItemRequest(item=_item()), ctx)
                    return r.ok

                def do_update():
                    r = svc.UpdateItem(pb2.UpdateItemRequest(
                        item_id="item-1", seller_id="user_A",
                        description="Updated", quantity=1,
                        status="ACTIVE", expected_version=1), ctx)
                    return r.ok

                def do_query():
                    r = svc.QueryItems(pb2.QueryRequest(filter="laptop"), ctx)
                    return r.ok

                results["CreateItem"] = _benchmark(do_create)
                results["UpdateItem"] = _benchmark(do_update)
                results["QueryItems"] = _benchmark(do_query)

    return results


# ---------------------------------------------------------------------------
# 4. Storage-node benchmarks
# ---------------------------------------------------------------------------

def bench_storage_node():
    results = {}
    ctx = MagicMock()

    # --- PutItem (no replication, backup role) ---
    node_b = _make_storage_node(role="backup", peer_addresses="", register_as_primary=False)

    counter = {"v": 0}

    def do_put():
        counter["v"] += 1
        iid = f"item-{counter['v']}"
        r = node_b.PutItem(pb2.PutRequest(item=_item(item_id=iid, version=1)), ctx)
        return r.success

    results["PutItem (backup, no repl)"] = _benchmark(do_put)

    # --- PutItem with replication (primary → 1 backup) ---
    node_p = _make_storage_node(role="primary", peer_addresses="backup:50052",
                                 register_as_primary=True)
    repl_resp = MagicMock()
    repl_resp.success = True
    repl_stub = MagicMock()
    repl_stub.ReplicateLog.return_value = repl_resp
    repl_ch = MagicMock()
    repl_ch.__enter__ = MagicMock(return_value=repl_ch)
    repl_ch.__exit__ = MagicMock(return_value=False)

    pcounter = {"v": 0}

    def do_put_primary():
        pcounter["v"] += 1
        iid = f"pitem-{pcounter['v']}"
        with patch("grpc.insecure_channel", return_value=repl_ch):
            with patch("proto.src.marketplace_pb2_grpc.StorageReplicaStub",
                       return_value=repl_stub):
                r = node_p.PutItem(pb2.PutRequest(item=_item(item_id=iid, version=1)), ctx)
        return r.success

    results["PutItem (primary + replicate)"] = _benchmark(do_put_primary)

    # --- ReplicateLog ---
    node_r = _make_storage_node(role="backup", register_as_primary=False)
    rcounter = {"v": 0}

    def do_replicate():
        rcounter["v"] += 1
        iid = f"ritem-{rcounter['v']}"
        r = node_r.ReplicateLog(pb2.ReplicationRequest(item=_item(item_id=iid, version=1)), ctx)
        return r.success

    results["ReplicateLog"] = _benchmark(do_replicate)

    # --- QueryItems ---
    node_q = _make_storage_node(role="backup", register_as_primary=False)
    for i in range(100):
        node_q.items_by_id[f"qitem-{i}"] = _item(item_id=f"qitem-{i}", version=1)

    def do_query():
        r = node_q.QueryItems(pb2.QueryRequest(filter="laptop"), ctx)
        return r.ok

    results["QueryItems (100 items)"] = _benchmark(do_query)

    # --- Heartbeat ---
    node_h = _make_storage_node(role="primary", register_as_primary=True)

    def do_heartbeat():
        r = node_h.Heartbeat(pb2.HealthCheckRequest(request_source="CONTROLLER"), ctx)
        return r.alive

    results["Heartbeat"] = _benchmark(do_heartbeat)

    return results


# ---------------------------------------------------------------------------
# 5. Controller benchmarks
# ---------------------------------------------------------------------------

def bench_controller():
    results = {}
    ctx = MagicMock()

    # --- RegisterNode ---
    ctrl = Controller()

    def do_register():
        addr = f"node-{time.time_ns()}:50051"
        r = ctrl.RegisterNode(pb2.RegisterRequest(address=addr), ctx)
        return r.success

    results["RegisterNode"] = _benchmark(do_register)

    # --- GetPrimary ---
    ctrl2 = Controller()
    ctrl2.RegisterNode(pb2.RegisterRequest(address="node:50051"), ctx)

    def do_get_primary():
        r = ctrl2.GetPrimary(pb2.GetPrimaryRequest(), ctx)
        return r.success

    results["GetPrimary"] = _benchmark(do_get_primary)

    # --- GetClusterInfo ---
    ctrl3 = Controller()
    for i in range(3):
        ctrl3.RegisterNode(pb2.RegisterRequest(address=f"node{i}:5005{i}"), ctx)

    def do_cluster_info():
        r = ctrl3.GetClusterInfo(pb2.ClusterInfoRequest(), ctx)
        return r.success

    results["GetClusterInfo"] = _benchmark(do_cluster_info)

    return results


# ---------------------------------------------------------------------------
# 6. Stress test — concurrent puts
# ---------------------------------------------------------------------------

STRESS_WORKERS = 20
STRESS_OPS_PER_WORKER = 50


def stress_test():
    node = _make_storage_node(role="backup", register_as_primary=False)
    ctx = MagicMock()

    latencies = []
    errors = []
    lock = threading.Lock()
    counter = {"v": 0}

    def worker():
        for _ in range(STRESS_OPS_PER_WORKER):
            with lock:
                counter["v"] += 1
                vid = counter["v"]
            iid = f"stress-{vid}"
            t0 = time.perf_counter()
            try:
                r = node.PutItem(pb2.PutRequest(item=_item(item_id=iid, version=1)), ctx)
                elapsed_us = (time.perf_counter() - t0) * 1_000_000
                with lock:
                    latencies.append((elapsed_us, r.success))
            except Exception as e:
                with lock:
                    errors.append(str(e))

    threads = [threading.Thread(target=worker) for _ in range(STRESS_WORKERS)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_time_s = time.perf_counter() - t_start

    total_ops = STRESS_WORKERS * STRESS_OPS_PER_WORKER
    successes = sum(1 for _, ok in latencies if ok)
    lat_vals = sorted(v for v, _ in latencies)
    n = len(lat_vals)

    return {
        "workers": STRESS_WORKERS,
        "ops_per_worker": STRESS_OPS_PER_WORKER,
        "total_ops": total_ops,
        "successes": successes,
        "errors": len(errors),
        "success_rate": f"{successes / total_ops * 100:.1f}%",
        "total_time_s": f"{total_time_s:.3f}",
        "throughput_ops_s": f"{total_ops / total_time_s:.1f}",
        "mean_us": f"{mean(lat_vals):.1f}",
        "stdev_us": f"{stdev(lat_vals):.1f}" if n > 1 else "0.0",
        "p50_us": f"{lat_vals[int(0.50 * n) - 1]:.1f}",
        "p95_us": f"{lat_vals[int(0.95 * n) - 1]:.1f}",
        "p99_us": f"{lat_vals[int(0.99 * n) - 1]:.1f}",
        "min_us": f"{min(lat_vals):.1f}",
        "max_us": f"{max(lat_vals):.1f}",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n" + "=" * 70)
    print("  MARKETPLACE SYSTEM — FULL STATISTICAL EVALUATION REPORT")
    print("=" * 70)
    print(f"  Benchmark iterations per operation : {BENCH_ITERATIONS}")
    print(f"  Stress test workers                : {STRESS_WORKERS}")
    print(f"  Stress test ops/worker             : {STRESS_OPS_PER_WORKER}")
    print(f"  Total stress-test operations       : {STRESS_WORKERS * STRESS_OPS_PER_WORKER}")

    # ── 1. Unit-test suite ─────────────────────────────────────────────────
    print("\n[1/5] Running unit-test suite …", flush=True)
    suite_rows, passed, failed, errors, skipped, duration, pass_rate = run_test_suite()
    total_tests = passed + failed + errors + skipped

    print_table(
        "TABLE 1 — Unit-Test Suite Results",
        ["Test Module", "File", "# Tests"],
        suite_rows,
    )
    print_table(
        "TABLE 1b — Suite Summary",
        ["Total", "Passed", "Failed", "Errors", "Skipped", "Pass Rate", "Duration (s)"],
        [[total_tests, passed, failed, errors, skipped, pass_rate, f"{duration:.2f}"]],
    )

    # ── 2. Service-node benchmarks ─────────────────────────────────────────
    print("\n[2/5] Benchmarking ServiceNode operations …", flush=True)
    with _silent():
        sn_results = bench_service_node()
    sn_rows = [
        [op,
         r["n"],
         r["success_rate"],
         r["mean_us"],
         r["stdev_us"],
         r["p50_us"],
         r["p95_us"],
         r["p99_us"],
         r["min_us"],
         r["max_us"]]
        for op, r in sn_results.items()
    ]
    print_table(
        "TABLE 2 — ServiceNode Latency Benchmarks (all times in µs)",
        ["Operation", "Iters", "Success%", "Mean", "StdDev", "p50", "p95", "p99", "Min", "Max"],
        sn_rows,
    )

    # ── 3. Storage-node benchmarks ─────────────────────────────────────────
    print("\n[3/5] Benchmarking StorageNode operations …", flush=True)
    with _silent():
        stor_results = bench_storage_node()
    stor_rows = [
        [op,
         r["n"],
         r["success_rate"],
         r["mean_us"],
         r["stdev_us"],
         r["p50_us"],
         r["p95_us"],
         r["p99_us"],
         r["min_us"],
         r["max_us"]]
        for op, r in stor_results.items()
    ]
    print_table(
        "TABLE 3 — StorageNode Latency Benchmarks (all times in µs)",
        ["Operation", "Iters", "Success%", "Mean", "StdDev", "p50", "p95", "p99", "Min", "Max"],
        stor_rows,
    )

    # ── 4. Controller benchmarks ───────────────────────────────────────────
    print("\n[4/5] Benchmarking Controller operations …", flush=True)
    with _silent():
        ctrl_results = bench_controller()
    ctrl_rows = [
        [op,
         r["n"],
         r["success_rate"],
         r["mean_us"],
         r["stdev_us"],
         r["p50_us"],
         r["p95_us"],
         r["p99_us"],
         r["min_us"],
         r["max_us"]]
        for op, r in ctrl_results.items()
    ]
    print_table(
        "TABLE 4 — Controller Latency Benchmarks (all times in µs)",
        ["Operation", "Iters", "Success%", "Mean", "StdDev", "p50", "p95", "p99", "Min", "Max"],
        ctrl_rows,
    )

    # ── 5. Stress test ─────────────────────────────────────────────────────
    print(f"\n[5/5] Running stress test ({STRESS_WORKERS} workers × {STRESS_OPS_PER_WORKER} ops) …",
          flush=True)
    with _silent():
        st = stress_test()
    print_table(
        "TABLE 5 — Stress Test: Concurrent PutItem (StorageNode backup, in-process)",
        ["Metric", "Value"],
        [
            ["Workers", st["workers"]],
            ["Ops / worker", st["ops_per_worker"]],
            ["Total operations", st["total_ops"]],
            ["Successful writes", st["successes"]],
            ["Errors", st["errors"]],
            ["Success rate", st["success_rate"]],
            ["Total wall time (s)", st["total_time_s"]],
            ["Throughput (ops/s)", st["throughput_ops_s"]],
            ["Mean latency (µs)", st["mean_us"]],
            ["StdDev latency (µs)", st["stdev_us"]],
            ["p50 latency (µs)", st["p50_us"]],
            ["p95 latency (µs)", st["p95_us"]],
            ["p99 latency (µs)", st["p99_us"]],
            ["Min latency (µs)", st["min_us"]],
            ["Max latency (µs)", st["max_us"]],
        ],
    )

    print("\n" + "=" * 70)
    print("  EVALUATION COMPLETE")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
