# Rewind VM Agent Conventions

## CLI-to-SDK Parity

Every programmatic capability exposed through the CLI (`crates/rewind/src/cmd/cluster.rs` and `--node` cluster commands) must also be exposed through the cluster JSON-RPC server (`crates/rewind-core/src/cluster_rpc.rs`) and the Python SDK (`sdk/python/rewind/`).

- **Zero gaps from CLI to SDK**: when adding or changing a CLI command, subcommand, or flag that makes sense programmatically, wire the matching RPC method/parameter and Python SDK method (`Cluster` and `NodeHandle`, where node-scoped) in the same change.
- **SDK-only features are fine**: interactive/reactive orchestration primitives (signals, barriers, live snapshots, fan-out, live screenshots) may live in the RPC server and SDK without a CLI equivalent.
- **CLI-only exceptions**: interactive TTY/GUI/host-setup/packager commands (`shell`, `gdb`, `open`, `doctor`, `pmu`, `generate`, `bugreport`, `build`) stay CLI-only.

## File-Path Bulk I/O

Transfer binary artifacts (screenshots, videos, `.pcap` captures, snapshots, large guest file exports) by accepting host file paths and writing directly to disk in `rewind cluster serve`, returning only compact JSON metadata over RPC. Keep JSON-RPC payloads small and structured instead of piping bulk base64 blobs.

## Python SDK Architecture

- Keep `sdk/python/rewind/` **async-only** (`asyncio`, pure Python stdlib, zero third-party dependencies).
- Keep the package modular by concern (`cluster.py`, `node.py`, `rpc.py`, `hid.py`, `types.py`, `guest.py`): host callers import `from rewind import Cluster`; guest code inside cluster VMs imports `from rewind.guest import meta, signal, barrier`.
- Before finishing any CLI or SDK change, verify Rust clippy (`cargo clippy --workspace --lib --bins --tests -- -D warnings`) and exercise the affected SDK methods end-to-end against `rewind cluster serve`.
