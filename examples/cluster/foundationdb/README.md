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

# Multi-AZ fault injection (clogging, pause/resume, clock step, same-IP TLog crash+restart)
python3 examples/cluster/foundationdb_sim.py --scenario reboot-chaos

# Run all three scenarios and verify deterministic bit-for-bit replay
python3 examples/cluster/foundationdb_sim.py --scenario all --replay
```

## Scenarios

1. **`stale-peer`** (7 VMs: `fdb1..fdb6` across `az-1..az-6` + real `libfdb_c.so` client):
   Compares `location_cache_peer_evictor_enabled=0` (`buggy`, pre-`eb5639a90e`) against
   `location_cache_peer_evictor_enabled=1` (`fixed`, post-`eb5639a90e`) after permanently
   crashing the active Storage Server. Rewind's switch trace (`c.net_events()`) catches
   the buggy client spamming TCP SYN/ARP reconnects to the dead peer indefinitely,
   while the fixed client evicts the dead peer and drops new TCP SYN/ARP reconnects to zero.
2. **`globalconfig-crash`** (4 VMs: `fdb1..fdb3` + `client`):
   Writes an empty value to `\xff/globalConfig/v` via `ACCESS_SYSTEM_KEYS` and catches
   the heap OOB read and `Assertion !globalConfigHistory.empty() failed` crash across
   GRV proxies in unmodified FoundationDB 8.0.1.
3. **`reboot-chaos`** (6 VMs: `fdb1..fdb5` + `client`):
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
