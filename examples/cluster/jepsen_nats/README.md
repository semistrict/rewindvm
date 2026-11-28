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
# Healthy baseline (no faults): Jepsen queue checker passes (:valid? true, 0 lost)
python3 examples/cluster/jepsen_nats.py --scenario healthy

# Reproduce Jepsen's Dec 2025 NATS 2.12.1 crash data-loss finding (:valid? false)
python3 examples/cluster/jepsen_nats.py --scenario crash-loss

# Full Jepsen nemesis (partition + pause + clock + kill) driven by jepsen.rewind
python3 examples/cluster/jepsen_nats.py --scenario chaos

# Run all scenarios and verify deterministic bit-for-bit replay
python3 examples/cluster/jepsen_nats.py --scenario all --replay
```

> **Storage note:** Rewind VMs currently mount `/tmp/nats-data` on ephemeral `tmpfs`, so
> `c.crash()` + `c.restart()` on a node models sudden power loss before `fsync` (matching
> NATS 2.12.1's default `sync_interval: 2m` lazy-fsync window where acknowledged JetStream
> writes are not yet synced to durable storage). When a surviving replica stays up across
> single/minority crashes, NATS replicates state back to restarted nodes; when all replicas
> lose unsynced state (`crash-loss` / `chaos`), Jepsen catches total loss of all
> acknowledged messages (`:valid? false`).
