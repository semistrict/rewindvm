#!/usr/bin/env python3
"""Deterministic 3-node Apache Cassandra 6.0 (CEP-15 Accord) protocol stress test.

Boots three real Apache Cassandra 6.0-alpha2 VMs with Accord enabled
(`accord.enabled: true`, `storage_compatibility_mode: NONE`, `RF=3`) and slight
inter-node clock skew (`0ms`, `+15ms`, `-10ms`) + clock drift (`0`, `+200ppm`,
`-200ppm`), plus a zero-dependency multi-threaded Python CQLv4 driver that
exercises strict-serializable, conflicting, 3-partition Accord transactions
across two tables (`bank.accounts` + `bank.txlog`):

  Setup (paid once, then snapshotted):
      Creates `bank.accounts` and `bank.txlog` with `transactional_mode = 'full'`,
      seeds alice/bob/carol = 1000 each, and captures a whole-cluster snapshot
      (`cluster-ready`) of all 3 JVMs, the driver VM, and the virtual L2 switch.

  Concurrent Conflicting Accord Storm (3 worker threads + linearizable auditor):
      Three concurrent workers pinned to `node1`, `node2`, and `node3`
      simultaneously submit overlapping 3-partition conditional transfers
      (`IF seen IS NULL AND s.balance >= amt THEN INSERT txlog ... UPDATE +/-`)
      while a 4th thread runs continuous multi-partition Accord reads asserting
      strict conservation (`total == 3000`, zero overdrafts) on every read.

  Branch A (from `cluster-ready`) — Mid-flight minority partition + lossy link + heal:
      Isolates `node3` and injects 5% packet loss, 2% duplication, and 3 ms
      jitter on `node1 <-> node2` *while transactions are actively in flight*,
      verifies the 2-node slow-path quorum keeps committing, heals mid-storm,
      and verifies linearizable catch-up directly on `node3` (`nodes=3`).

  Branch B (`c.restore("cluster-ready")`, zero JVM reboot) — Mid-flight
      asymmetric partition -> hard coordinator crash:
      Rewinds all 3 JVMs and the virtual switch back to `cluster-ready`,
      injects an asymmetric partition (`node1 -> node2` blocked while
      `node2 -> node1` stays open) mid-storm, then hard-crashes `node3`
      mid-flight and verifies the surviving 2-node Accord quorum completes all
      transactions with exact conservation (`total == 3000`, `nodes=2`).

Requires `nix` on PATH (for `.#cassandra6` and `nixpkgs#python3`), or prebuilt
EROFS images via `REWIND_CASSANDRA_EROFS` and `REWIND_PYTHON_EROFS`. Run with:
    python3 examples/cluster/cassandra_accord.py

Or explore perturbed schedules/nemesis faults automatically:
    rewind cluster check --schedules 4 --nemesis -- python3 examples/cluster/cassandra_accord.py
"""

import os
import sys
import textwrap
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from rewind_cluster import Cluster

REPO_ROOT = Path(__file__).resolve().parents[2]

NODES = [
    ("node1", "10.0.0.1", "-9223372036854775808", 0, 0),
    ("node2", "10.0.0.2", "-3074457345618258603", 15, 200),
    ("node3", "10.0.0.3", "3074457345618258602", -10, -200),
]

LOGBACK_XML = """\
<configuration scan="false">
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%-5level [%thread] %logger{24} - %msg%n</pattern>
    </encoder>
  </appender>
  <root level="INFO">
    <appender-ref ref="STDOUT" />
  </root>
  <logger name="org.apache.cassandra.db.compaction" level="WARN"/>
  <logger name="org.apache.cassandra.gms" level="INFO"/>
  <logger name="org.apache.cassandra.service.accord" level="INFO"/>
</configuration>
"""


def cassandra_yaml(ip: str, token: str, seeds: str) -> str:
    return textwrap.dedent(
        f"""\
        cluster_name: 'RewindAccordCluster'
        num_tokens: 1
        initial_token: {token}
        partitioner: org.apache.cassandra.dht.Murmur3Partitioner

        data_file_directories:
          - /build/data/data
        commitlog_directory: /build/data/commitlog
        saved_caches_directory: /build/data/saved_caches
        hints_directory: /build/data/hints

        storage_compatibility_mode: NONE
        accord:
          enabled: true
          journal_directory: /build/data/accord_journal
          queue_shard_count: 1
          command_store_shard_count: 1
          fast_path_update_delay: 1s

        seed_provider:
          - class_name: org.apache.cassandra.locator.SimpleSeedProvider
            parameters:
              - seeds: "{seeds}"

        listen_address: {ip}
        rpc_address: {ip}
        broadcast_rpc_address: {ip}
        storage_port: 7000
        ssl_storage_port: 7001
        start_native_transport: true
        native_transport_port: 9042
        native_transport_allow_older_protocols: true

        endpoint_snitch: SimpleSnitch
        authenticator:
          class_name: AllowAllAuthenticator
        authorizer:
          class_name: AllowAllAuthorizer
        role_manager:
          class_name: CassandraRoleManager
        network_authorizer:
          class_name: AllowAllNetworkAuthorizer
        cidr_authorizer:
          class_name: AllowAllCIDRAuthorizer

        disk_access_mode: standard
        commitlog_sync: periodic
        commitlog_sync_period: 10000ms
        commitlog_segment_size: 4MiB
        commitlog_disk_access_mode: legacy
        memtable_allocation_type: heap_buffers
        concurrent_reads: 4
        concurrent_writes: 4
        concurrent_counter_writes: 2
        concurrent_materialized_view_writes: 2
        concurrent_compactors: 1
        auto_snapshot: false
        hinted_handoff_enabled: true
        read_request_timeout: 5000ms
        write_request_timeout: 5000ms
        cas_contention_timeout: 5000ms
        request_timeout: 10000ms
        """
    )


DRIVER_SCRIPT = Path(__file__).with_name("cassandra_accord_driver.py").read_text(
    encoding="utf-8"
)


def main() -> None:
    cassandra_pkg = os.environ.get("REWIND_CASSANDRA_PKG", f"{REPO_ROOT}#cassandra6")
    cassandra_erofs = os.environ.get("REWIND_CASSANDRA_EROFS")
    python_erofs = os.environ.get("REWIND_PYTHON_EROFS")
    seeds = "10.0.0.1:7000"

    t0 = time.perf_counter()
    with Cluster(
        "cassandra6-accord-3node",
        latency_ms=2,
        jitter_us=300,
        mem_mib=1024,
        stop_when="driver",
        max_virtual_ms=300_000,
        slice_us=250,
    ) as c:
        node_handles = []
        for idx, (name, ip, token, skew_ms, drift_ppm) in enumerate(NODES):
            # Stagger joins cleanly (Cassandra CEP-21 TCM forbids concurrent
            # bootstraps): node2 waits for node1:9042, node3 waits for node2:9042.
            if idx == 0:
                cmd = ["cassandra-node", "/build", "/build/data"]
            else:
                prev_ip = NODES[idx - 1][1]
                cmd = [
                    "/bin/sh",
                    "-c",
                    f"until (nc -z {prev_ip} 9042 2>/dev/null || "
                    f"bash -c 'exec 3<>/dev/tcp/{prev_ip}/9042' 2>/dev/null); "
                    "do sleep 0.5; done; "
                    "exec cassandra-node /build /build/data",
                ]
            node_kwargs = {}
            if cassandra_erofs:
                node_kwargs["root"] = cassandra_erofs
            else:
                node_kwargs["packages"] = [cassandra_pkg]
            h = c.add_node(
                name,
                cmd,
                ip=ip,
                role="service",
                mem_mib=1024,
                clock_skew_ms=skew_ms,
                clock_drift_ppm=drift_ppm,
                files={
                    "/build/cassandra.yaml": cassandra_yaml(ip, token, seeds),
                    "/build/logback.xml": LOGBACK_XML,
                },
                **node_kwargs,
            )
            node_handles.append(h)

        client_kwargs = {}
        if python_erofs:
            client_kwargs["root"] = python_erofs
        else:
            client_kwargs["packages"] = ["nixpkgs#python3"]
        client = c.add_node(
            "client",
            ["python3", "-u", "/build/driver.py"],
            ip="10.0.0.4",
            role="driver",
            mem_mib=256,
            files={"/build/driver.py": DRIVER_SCRIPT},
            **client_kwargs,
        )

        c.start()
        try:
            for h in node_handles:
                h.wait_for(
                    "Starting listening for CQL clients",
                    from_start=True,
                    timeout_ms=120_000,
                )

            client.wait_for("CLUSTER_3_NODES_JOINED", from_start=True, timeout_ms=60_000)
            client.wait_for("CLUSTER_READY", from_start=True, timeout_ms=60_000)
            t_boot = time.perf_counter()

            # Snapshot the warm 3-node Accord cluster (VMs + full virtual switch)
            # so we can branch into multiple mid-flight fault timelines without rebooting!
            snap_ready = c.snapshot("cluster-ready")
            t_snap = time.perf_counter()

            # === Branch A: Mid-flight minority partition + lossy/jittery quorum link + heal ===
            client.wait_for("STORM_IN_FLIGHT", from_start=True, timeout_ms=60_000)
            c.partition(["node3"], ["node1", "node2", "client"])
            c.set_link(
                "node1",
                "node2",
                latency_ms=8,
                jitter_ms=3,
                drop_ppm=50_000,
                duplicate_ppm=20_000,
            )

            client.wait_for("STORM_MID", from_start=True, timeout_ms=90_000)
            c.heal()
            c.set_link(
                "node1",
                "node2",
                latency_ms=2,
                jitter_us=300,
                drop_ppm=0,
                duplicate_ppm=0,
            )

            conv_a = client.wait_for("CONVERGENCE", from_start=True, timeout_ms=90_000)
            assert "nodes=3 " in (conv_a.line or ""), f"Branch A expected 3 converged nodes: {conv_a.line}"
            client.wait_for("ACCORD_ALL_OK", from_start=True, timeout_ms=30_000)
            t_branch_a = time.perf_counter()
            print(f"[Branch A] {conv_a.line} ({t_branch_a - t_snap:.2f}s wall)", flush=True)

            # === Rewind to `cluster-ready` (zero JVM reboot!) ===
            c.restore(snap_ready)
            t_restore = time.perf_counter()

            # === Branch B: Mid-flight asymmetric partition -> hard crash of node3 ===
            client.wait_for("STORM_IN_FLIGHT", from_start=True, timeout_ms=60_000)
            c.partition(["node1"], ["node2"], symmetric=False)

            client.wait_for("STORM_MID", from_start=True, timeout_ms=90_000)
            c.heal()
            node_handles[2].crash()

            conv_b = client.wait_for("CONVERGENCE", from_start=True, timeout_ms=90_000)
            assert "nodes=2 " in (conv_b.line or ""), f"Branch B expected 2 surviving nodes: {conv_b.line}"
            client.wait_for("ACCORD_ALL_OK", from_start=True, timeout_ms=30_000)
            res = client.wait_exit(timeout_ms=10_000)
            assert res["exited"] and res["exit_code"] == 0, res
            t_branch_b = time.perf_counter()
            print(
                f"[Branch B] {conv_b.line} ({t_branch_b - t_restore:.2f}s wall; "
                f"restore={t_restore - t_branch_a:.2f}s)",
                flush=True,
            )

            fin = c.finish()
            print(
                f"recorded cluster {fin['id']} ({fin['verdict']}) "
                f"at {res['cluster_ns'] / 1e6:.1f}ms virtual time "
                f"(boot={t_boot - t0:.1f}s, branch_a={t_branch_a - t_snap:.1f}s, "
                f"branch_b={t_branch_b - t_restore:.1f}s)"
            )
        except Exception:
            for h in [*node_handles, client]:
                all_out = h.logs(stream="output")
                errs = [l for l in all_out if "ERROR" in l or "Exception" in l or "WARN" in l][-15:]
                out = all_out[-25:]
                print(f"\n=== {h.name} errors/warns ===", file=sys.stderr)
                for line in errs:
                    print(line, file=sys.stderr)
                print(f"=== {h.name} output (tail) ===", file=sys.stderr)
                for line in out:
                    print(line, file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
