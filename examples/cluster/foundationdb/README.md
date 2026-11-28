# FoundationDB 8.0.1 Deterministic Cluster Examples

Runs unmodified official FoundationDB 8.0.1 binaries (`fdbserver`, `fdbcli`, and
`libfdb_c.so`) inside multi-VM Rewind clusters on Apple Silicon HVF or Linux KVM.

## 1. Build the Guest Root Image

Build the EROFS image once (requires Docker / OrbStack):

```bash
examples/cluster/foundationdb/build_image.sh
```

This writes `target/<arch>-erofs/fdb.erofs` (auto-detected by `foundationdb_sim.py`,
or override with `REWIND_FDB_EROFS=/path/to/fdb.erofs`).

## 2. Run the Scenarios

```bash
# Default: reproduce FDB commit eb5639a90e (PR #13912 / #14050) stale-peer leak
python3 examples/cluster/foundationdb_sim.py --scenario stale-peer

# Reproduce FDB commit f8e1e90a40 (PR #14196) empty \xff/globalConfig/v crash
python3 examples/cluster/foundationdb_sim.py --scenario globalconfig-crash

# Reproduce FDB commit 1915da14fc (PR #14328) stale coordinator DNS hang
python3 examples/cluster/foundationdb_sim.py --scenario stale-coordinator-dns

# Reproduce FDB commit 446079cc6a (PR #13248) TLog peek timeout under clogging
python3 examples/cluster/foundationdb_sim.py --scenario peek-timeout-clog

# Reproduce FDB commit be737227dc invalid --trusted-subnet CIDR prefix bypass
python3 examples/cluster/foundationdb_sim.py --scenario subnet-prefix-bypass

# Multi-AZ fault injection (clogging, pause/resume, clock step, same-IP TLog crash+restart)
python3 examples/cluster/foundationdb_sim.py --scenario reboot-chaos

# Run all six scenarios and verify deterministic bit-for-bit replay
python3 examples/cluster/foundationdb_sim.py --scenario all --replay
```

## Scenarios

1. **`stale-peer`** — FDB `eb5639a90e` (PR #13912 / #14050) (7 VMs: `fdb1..fdb6` across `az-1..az-6` + `libfdb_c.so` client):
   Compares `location_cache_peer_evictor_enabled=0` (`buggy`) against `1` (`fixed`)
   after permanently crashing the active Storage Server. Rewind's switch trace
   (`c.net_events()`) catches the buggy client spamming TCP SYN/ARP reconnects to
   the dead peer indefinitely, while the fixed client evicts the dead peer and
   drops new TCP SYN/ARP reconnects to zero.
2. **`globalconfig-crash`** — FDB `f8e1e90a40` (PR #14196) (4 VMs: `fdb1..fdb3` + `client`):
   Writes an empty value to `\xff/globalConfig/v` via `ACCESS_SYSTEM_KEYS` and catches
   the heap OOB read and `Assertion !globalConfigHistory.empty() failed` crash across
   ClusterController/GRV proxies in unmodified FoundationDB 8.0.1.
3. **`stale-coordinator-dns`** — FDB `1915da14fc` (PR #14328) (4 VMs: `fdb1..fdb3` + `libfdb_c.so` multi-version client):
   Opens databases using a connection string mixing the 3 live coordinators with
   stale `.invalid` coordinator hostnames. In unmodified FDB 8.0.1,
   `getClusterProtocolImpl()` awaits `Hostname::resolveWithRetry()` before watching
   `coordinator->onChange()`, wedging database activation until `error 1031`
   (`transaction_timed_out`) even though the cluster is healthy.
4. **`peek-timeout-clog`** — FDB `446079cc6a` (PR #13248) (6 VMs: `fdb1..fdb5` + `client`):
   Runs non-streaming peeks (`--knob_peek_using_streaming=0`), clogs both TLogs <->
   Storage Server `fdb4` via `c.clog_link()`, and compares `--knob_peek_reply_timeout=0`
   (pre-fix wait-forever) against `--knob_peek_reply_timeout=2` (post-fix sender-side
   peek retries observed directly on Rewind's virtual L2 switch).
5. **`subnet-prefix-bypass`** — FDB `be737227dc` (4 VMs: `fdb1..fdb3` + `client`):
   Starts `fdbserver` with invalid `--trusted-subnet-1` CIDR prefixes (`10.0.0.1/-1`,
   `10.0.0.1/33`, `10.0.0.1/8junk`) and proves unmodified FDB 8.0.1 accepts them
   instead of rejecting with `invalid_option` (silently turning `/-1` into `0.0.0.0/0`
   and overflowing the 4-byte mask array on `/33`).
6. **`reboot-chaos`** (6 VMs: `fdb1..fdb5` + `client`):
   Injects link clogging (`clog_link` + `unclog`), Storage Server vCPU freeze (`pause` +
   `resume`), forward clock step + drift (`step_clock` + `set_clock_drift`), and a same-IP
   TLog crash + restart (`crash` + `restart`), then verifies post-fault transactions and
   multi-boot trace recording/replay.

## Simulation Primitives Demo (No FDB Image Required)

To exercise all six FoundationDB-inspired Rewind simulation primitives (clogging,
pause/resume, dynamic clock faults, quiet/settle nemesis, fault-domain locality,
and `http://rewind` `BUGGIFY` + `CODE_PROBE`) using only Python:

```bash
python3 examples/cluster/fdb_sim_primitives.py
```
