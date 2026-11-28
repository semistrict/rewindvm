#!/usr/bin/env python3
"""In-guest multi-threaded CQLv4 client + concurrent Cassandra 6.0 Accord driver.

Injected into the `client` VM (`/build/driver.py`) by `cassandra_accord.py`.
Uses only the Python standard library so the driver VM only needs `python3`.
"""

import socket
import struct
import sys
import threading
import time

NODES = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
ACCOUNTS = ["alice", "bob", "carol"]
TXS_PER_WORKER = 10  # 3 workers * 10 = 30 conflicting 3-partition Accord txns

# CQL Native Protocol v4 opcodes
OP_ERROR = 0x00
OP_STARTUP = 0x01
OP_READY = 0x02
OP_QUERY = 0x07
OP_RESULT = 0x08

CL_ONE = 0x0001
CL_QUORUM = 0x0004


class CqlError(RuntimeError):
    def __init__(self, code: int, msg: str):
        super().__init__(f"CQL error 0x{code:04x}: {msg}")
        self.code = code
        self.msg = msg


class CqlConn:
    def __init__(self, host: str, port: int = 9042, timeout: float = 10.0):
        self.host = host
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self._stream = 1
        self._startup()

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError(f"EOF from {self.host}")
            buf.extend(chunk)
        return bytes(buf)

    def _roundtrip(self, opcode: int, body: bytes) -> tuple[int, bytes]:
        stream = self._stream
        self._stream = (self._stream % 30000) + 1
        hdr = struct.pack("!BBhBi", 0x04, 0x00, stream, opcode, len(body))
        self.sock.sendall(hdr + body)
        rhdr = self._recv_exact(9)
        ver, _flags, _rstream, rop, rlen = struct.unpack("!BBhBi", rhdr)
        assert ver == 0x84, f"unexpected protocol response version {ver:#x}"
        rbody = self._recv_exact(rlen)
        if rop == OP_ERROR:
            code = struct.unpack_from("!i", rbody, 0)[0]
            mlen = struct.unpack_from("!H", rbody, 4)[0]
            msg = rbody[6 : 6 + mlen].decode("utf-8", "replace")
            raise CqlError(code, msg)
        return rop, rbody

    def _startup(self) -> None:
        body = (
            struct.pack("!H", 1)
            + struct.pack("!H", 11)
            + b"CQL_VERSION"
            + struct.pack("!H", 5)
            + b"3.0.0"
        )
        rop, _ = self._roundtrip(OP_STARTUP, body)
        if rop != OP_READY:
            raise RuntimeError(f"expected READY, got opcode {rop:#x}")

    def query(self, cql: str, consistency: int = CL_QUORUM) -> list[list[bytes | None]]:
        raw = cql.encode("utf-8")
        body = struct.pack("!i", len(raw)) + raw + struct.pack("!HB", consistency, 0)
        rop, rbody = self._roundtrip(OP_QUERY, body)
        if rop != OP_RESULT:
            raise RuntimeError(f"expected RESULT, got opcode {rop:#x}")
        kind = struct.unpack_from("!i", rbody, 0)[0]
        if kind != 2:  # Not ROWS
            return []
        return self._parse_rows(rbody, 4)

    def _read_short_str(self, buf: bytes, off: int) -> tuple[str, int]:
        n = struct.unpack_from("!H", buf, off)[0]
        off += 2
        return buf[off : off + n].decode("utf-8"), off + n

    def _skip_type(self, buf: bytes, off: int) -> int:
        tid = struct.unpack_from("!H", buf, off)[0]
        off += 2
        if tid == 0x0000:
            _, off = self._read_short_str(buf, off)
        elif tid in (0x0020, 0x0022):
            off = self._skip_type(buf, off)
        elif tid == 0x0021:
            off = self._skip_type(buf, off)
            off = self._skip_type(buf, off)
        elif tid == 0x0030:
            _, off = self._read_short_str(buf, off)
            _, off = self._read_short_str(buf, off)
            n = struct.unpack_from("!H", buf, off)[0]
            off += 2
            for _ in range(n):
                _, off = self._read_short_str(buf, off)
                off = self._skip_type(buf, off)
        elif tid == 0x0031:
            n = struct.unpack_from("!H", buf, off)[0]
            off += 2
            for _ in range(n):
                off = self._skip_type(buf, off)
        return off

    def _parse_rows(self, buf: bytes, off: int) -> list[list[bytes | None]]:
        flags, col_count = struct.unpack_from("!ii", buf, off)
        off += 8
        if flags & 0x0002:
            plen = struct.unpack_from("!i", buf, off)[0]
            off += 4 + max(plen, 0)
        if not (flags & 0x0004):
            global_spec = bool(flags & 0x0001)
            if global_spec:
                _, off = self._read_short_str(buf, off)
                _, off = self._read_short_str(buf, off)
            for _ in range(col_count):
                if not global_spec:
                    _, off = self._read_short_str(buf, off)
                    _, off = self._read_short_str(buf, off)
                _, off = self._read_short_str(buf, off)
                off = self._skip_type(buf, off)
        row_count = struct.unpack_from("!i", buf, off)[0]
        off += 4
        rows: list[list[bytes | None]] = []
        for _ in range(row_count):
            row: list[bytes | None] = []
            for _ in range(col_count):
                vlen = struct.unpack_from("!i", buf, off)[0]
                off += 4
                if vlen < 0:
                    row.append(None)
                else:
                    row.append(buf[off : off + vlen])
                    off += vlen
            rows.append(row)
        return rows


def decode_int(cell: bytes | None) -> int:
    assert cell is not None and len(cell) == 4, f"bad int cell: {cell!r}"
    return struct.unpack("!i", cell)[0]


def connect_with_retry(host: str, deadline: float, timeout: float = 15.0) -> CqlConn:
    last_err = None
    while time.monotonic() < deadline:
        try:
            return CqlConn(host, timeout=timeout)
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    raise RuntimeError(f"failed to connect to {host}: {last_err}")


def wait_cluster_ready(conns: list[CqlConn], deadline: float) -> None:
    while time.monotonic() < deadline:
        ok = True
        for c in conns:
            try:
                rows = c.query("SELECT peer FROM system.peers_v2", CL_ONE)
                if len(rows) < 2:
                    ok = False
                    break
            except Exception:
                ok = False
                break
        if ok:
            return
        time.sleep(0.5)
    raise RuntimeError("timed out waiting for 3-node cluster membership")


def accord_read_all(conn: CqlConn) -> dict[str, int]:
    """Strictly serializable multi-partition Accord read across all 3 accounts."""
    cql = """
    BEGIN TRANSACTION
      LET a = (SELECT balance FROM bank.accounts WHERE id = 'alice');
      LET b = (SELECT balance FROM bank.accounts WHERE id = 'bob');
      LET c = (SELECT balance FROM bank.accounts WHERE id = 'carol');
      SELECT a.balance, b.balance, c.balance;
    COMMIT TRANSACTION
    """
    rows = conn.query(cql, CL_QUORUM)
    assert len(rows) == 1, f"expected 1 row from Accord read, got {rows!r}"
    return {
        "alice": decode_int(rows[0][0]),
        "bob": decode_int(rows[0][1]),
        "carol": decode_int(rows[0][2]),
    }


def accord_idempotent_transfer(
    conn: CqlConn, tx_id: str, src: str, dst: str, amount: int
) -> None:
    """Strictly serializable, idempotent 3-partition Accord transfer across 2 tables.

    Guards on `bank.txlog[tx_id]` and `s.balance >= amount` inside a single
    Accord transaction so concurrent conflicting writers and retries after
    ambiguous timeouts/partitions/crashes have exact-once semantics.
    """
    cql = f"""
    BEGIN TRANSACTION
      LET seen = (SELECT applied FROM bank.txlog WHERE tx_id = '{tx_id}');
      LET s = (SELECT balance FROM bank.accounts WHERE id = '{src}');
      IF seen IS NULL AND s.balance >= {amount} THEN
        INSERT INTO bank.txlog (tx_id, applied) VALUES ('{tx_id}', 1);
        UPDATE bank.accounts SET balance -= {amount} WHERE id = '{src}';
        UPDATE bank.accounts SET balance += {amount} WHERE id = '{dst}';
      END IF
    COMMIT TRANSACTION
    """
    conn.query(cql, CL_QUORUM)


def check_invariant(label: str, balances: dict[str, int], log: bool = True) -> None:
    total = sum(balances.values())
    if log:
        print(f"{label}: balances={balances} total={total}", flush=True)
    for acct, bal in balances.items():
        if bal < 0:
            raise AssertionError(f"OVERDRAFT on {acct}: {balances}")
    if total != 3000:
        raise AssertionError(f"CONSERVATION VIOLATION: total={total} balances={balances}")


def run_with_retry(fn, attempts: int = 10, delay: float = 0.5):
    last_err = None
    for _ in range(attempts):
        try:
            return fn()
        except Exception as e:
            last_err = e
            time.sleep(delay)
    raise RuntimeError(f"operation failed after {attempts} attempts: {last_err}")


def rewind_signal(name: str, payload: str = "") -> None:
    import urllib.request

    data = payload.encode("utf-8")
    req = urllib.request.Request(
        f"http://rewind/signal/{name}",
        data=data,
        method="POST",
        headers={"Content-Type": "application/json" if payload.startswith("{") else "text/plain"},
    )
    with urllib.request.urlopen(req, timeout=30.0):
        pass


def rewind_barrier(name: str) -> str:
    import urllib.request

    with urllib.request.urlopen(f"http://rewind/barrier/{name}", timeout=120.0) as resp:
        return resp.read().decode("utf-8", errors="replace")


def main() -> None:
    deadline = time.monotonic() + 240.0
    print("DRIVER_CONNECTING", flush=True)
    conns = [connect_with_retry(ip, deadline) for ip in NODES]
    wait_cluster_ready(conns, deadline)
    rewind_signal("cluster_3_nodes_joined")

    c1, c2, c3 = conns
    c1.query(
        "CREATE KEYSPACE IF NOT EXISTS bank "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        CL_QUORUM,
    )
    c1.query(
        "CREATE TABLE IF NOT EXISTS bank.accounts ("
        "  id text PRIMARY KEY,"
        "  balance int"
        ") WITH transactional_mode = 'full'",
        CL_QUORUM,
    )
    c1.query(
        "CREATE TABLE IF NOT EXISTS bank.txlog ("
        "  tx_id text PRIMARY KEY,"
        "  applied int"
        ") WITH transactional_mode = 'full'",
        CL_QUORUM,
    )
    run_with_retry(
        lambda: c1.query(
            "BEGIN TRANSACTION "
            "  INSERT INTO bank.accounts (id, balance) VALUES ('alice', 1000); "
            "  INSERT INTO bank.accounts (id, balance) VALUES ('bob', 1000); "
            "  INSERT INTO bank.accounts (id, balance) VALUES ('carol', 1000); "
            "COMMIT TRANSACTION",
            CL_QUORUM,
        )
    )
    check_invariant("INIT", run_with_retry(lambda: accord_read_all(c2)))
    for c in conns:
        c.close()

    # Park at barrier so the host snapshots the warm 3-node Accord cluster cleanly.
    rewind_barrier("cluster_ready")

    # Concurrent conflicting multi-partition Accord storm:
    # 3 workers targeting node1, node2, node3 simultaneously + 1 linearizable auditor.
    lock = threading.Lock()
    committed_count = 0
    in_flight_emitted = False
    mid_emitted = False
    audit_count = 0
    errors: list[BaseException] = []
    stop_audit = threading.Event()

    def worker(wid: int) -> None:
        nonlocal committed_count, in_flight_emitted, mid_emitted
        # Each worker prefers its own coordinator but fails over round-robin
        # if that coordinator is partitioned or hard-crashed mid-storm.
        hosts = [NODES[(wid + k) % 3] for k in range(3)]
        conn: CqlConn | None = None
        host_idx = 0
        try:
            for seq in range(TXS_PER_WORKER):
                tx_id = f"w{wid}-{seq}"
                src = ACCOUNTS[(wid + seq) % 3]
                dst = ACCOUNTS[(wid + seq + 1) % 3]
                amt = 15 + ((wid * 7 + seq * 11) % 45)
                ok = False
                last_err: Exception | None = None
                for attempt in range(24):
                    if conn is None:
                        target = hosts[host_idx % len(hosts)]
                        try:
                            conn = CqlConn(target, timeout=3.5)
                        except Exception as e:
                            last_err = e
                            host_idx += 1
                            time.sleep(0.15)
                            continue
                    try:
                        accord_idempotent_transfer(conn, tx_id, src, dst, amt)
                        ok = True
                        break
                    except Exception as e:
                        last_err = e
                        conn.close()
                        conn = None
                        host_idx += 1
                        time.sleep(0.2)
                if not ok:
                    raise RuntimeError(f"worker {wid} tx {tx_id} failed: {last_err}")

                with lock:
                    committed_count += 1
                    cur = committed_count
                    if cur >= 3 and not in_flight_emitted:
                        in_flight_emitted = True
                        rewind_signal("storm_in_flight", str(cur))
                    if cur >= 15 and not mid_emitted:
                        mid_emitted = True
                        rewind_signal("storm_mid", str(cur))
        except BaseException as e:
            with lock:
                errors.append(e)
        finally:
            if conn is not None:
                conn.close()

    def auditor() -> None:
        nonlocal audit_count
        idx = 0
        while not stop_audit.is_set():
            host = NODES[idx % 3]
            idx += 1
            conn: CqlConn | None = None
            try:
                conn = CqlConn(host, timeout=3.0)
                bal = accord_read_all(conn)
                check_invariant("AUDIT", bal, log=False)
                with lock:
                    audit_count += 1
            except AssertionError as e:
                with lock:
                    errors.append(e)
                return
            except Exception:
                # Expected when probing a partitioned or crashed coordinator.
                pass
            finally:
                if conn is not None:
                    conn.close()
            time.sleep(0.15)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    aud = threading.Thread(target=auditor)
    aud.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop_audit.set()
    aud.join()

    if errors:
        raise errors[0]

    print(f"STORM_DONE committed={committed_count} audits={audit_count}", flush=True)

    # Give any healed node a short catch-up window, then verify linearizable
    # convergence across every reachable node.
    time.sleep(2.0)
    observed: dict[str, dict[str, int]] = {}
    ledger_count = None
    for ip in NODES:
        conn = None
        try:
            conn = CqlConn(ip, timeout=4.0)
            bal = run_with_retry(lambda c=conn: accord_read_all(c), attempts=6, delay=0.4)
            check_invariant(f"FINAL_{ip}", bal)
            observed[ip] = bal
            if ledger_count is None:
                rows = conn.query("SELECT tx_id FROM bank.txlog", CL_QUORUM)
                ledger_count = len(rows)
        except Exception as e:
            print(f"UNREACHABLE {ip}: {e}", flush=True)
        finally:
            if conn is not None:
                conn.close()

    assert len(observed) >= 2, f"expected >=2 reachable nodes, got {observed!r}"
    vals = list(observed.values())
    for ip, bal in observed.items():
        assert bal == vals[0], f"DIVERGENCE on {ip}: {bal} != {vals[0]}"
    assert ledger_count == TXS_PER_WORKER * 3, (
        f"expected {TXS_PER_WORKER * 3} applied txns in txlog, got {ledger_count}"
    )
    import json

    rewind_signal(
        "convergence",
        json.dumps(
            {
                "nodes": len(observed),
                "applied": ledger_count,
                "audits": audit_count,
                "balances": vals[0],
            }
        ),
    )
    rewind_signal("accord_all_ok")


if __name__ == "__main__":
    main()
