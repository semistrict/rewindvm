# Running Real Jepsen on Rewind VM — NATS 2.12.1 (December 2025 Analysis)

Runs the **real Jepsen Clojure framework (`jepsen 0.3.10`)** and the JetStream
queue workload/checker from Kyle Kingsbury's **December 8, 2025 Jepsen analysis
of NATS 2.12.1** (`jepsen-io/nats`) inside a 4-VM deterministic Rewind cluster:

- `n1`, `n2`, `n3` (`10.0.0.1..3`): unmodified official **NATS 2.12.1** (`nats-server`)
  JetStream cluster (`replicas=3`, default `sync_interval: 2m`).
- `control` (`10.0.0.10`): OpenJDK 21 running the pre-built AOT Jepsen uberjar
  (`jepsen 0.3.10` + `io.nats/jnats 2.24.1` + `jepsen.rewind`).

## How `jepsen.rewind` Works

Instead of SSHing `iptables` / `tc` / `SIGKILL` / `adjtimex` into DB nodes,
[`src/jepsen/rewind.clj`](src/jepsen/rewind.clj) plugs directly into Jepsen's
standard Clojure protocols:

- `jepsen.net.proto/Net` & `PartitionAll` → Rewind's L2 virtual switch
  (`asymmetric_partition`, `heal`, `clog_link`, `set_link`)
- `jepsen.db/Process` & `Pause` → Rewind's hypervisor `crash`/`restart` and `pause`/`resume`
- `rewind/clock-package` → Rewind's hypervisor `step_clock` / `set_clock_drift`

Every Jepsen nemesis action sends a JSON request to Rewind's partition-immune
`POST http://rewind/barrier/jepsen-fault` endpoint, the host driver executes the
exact hypervisor fault, and releases the barrier. Because Jepsen's control JVM
itself runs inside a Rewind VM, **every Jepsen counterexample replays bit-for-bit**
via `rewind cluster replay`.

## 1. Build the Guest Images

```bash
examples/cluster/jepsen_nats/build_images.sh
```

This creates `target/<arch>-erofs/nats-2.12.1.erofs` and
`target/<arch>-erofs/jepsen-nats.erofs` (or override with `REWIND_NATS_EROFS`
and `REWIND_JEPSEN_NATS_EROFS`).

## 2. Run the Scenarios

```bash
# Default A/B proof on persistent crash-consistent ext4 (/dev/rwd0 -> /data):
#   - sync_interval: "2m"     -> :valid? false (acknowledged writes lost on power loss)
#   - sync_interval: "always" -> :valid? true  (0 lost; ~3x REQ_OP_FLUSH / REQ_FUA barriers)
python3 examples/cluster/jepsen_nats.py --scenario ab --replay

# Healthy baseline (no faults): Jepsen queue checker passes (:valid? true, 0 lost)
python3 examples/cluster/jepsen_nats.py --scenario healthy

# Full Jepsen nemesis (partition + pause + clock + kill) driven by jepsen.rewind
python3 examples/cluster/jepsen_nats.py --scenario chaos

# Run all scenarios and verify deterministic bit-for-bit replay
python3 examples/cluster/jepsen_nats.py --scenario all --replay
```

## 3. Inspect in `rewind-app` (Cluster View)

```bash
rewind open <cluster-id>
```

Each NATS node (`n1`, `n2`, `n3`) is backed by a deterministic 64 MiB paravirtual
`rewind-blk` (`/dev/rwd0`) disk formatted as `ext4` and mounted at `/data`.
`c.crash()` models sudden power loss by discarding uncommitted sectors in the
volatile write cache while preserving `fsync`ed (`REQ_OP_FLUSH` / `REQ_FUA`)
blocks across `c.restart()` and ext4 journal replay.
