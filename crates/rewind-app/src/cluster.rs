//! Recorded multi-node cluster metadata, L2 network timeline (`network.bin`),
//! and virtual-time synchronization between cluster nodes in the desktop app.

use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context as _, Result};
use rewind_trace::Trace;
use rewind_trace::cluster::{
    CLUSTER_MANIFEST, CLUSTER_NETWORK_LOG, CLUSTER_NODES_DIR, ClusterManifest, ClusterVerdict,
};
use rewind_trace::net_log::{DropReason, NetEvent, NetLogReader};

/// Lightweight summary of a recorded cluster under `$REWIND_HOME/clusters/`,
/// read from `cluster.json` alone for the empty state's Recent Clusters list.
#[derive(Clone, Debug)]
pub struct ClusterEntry {
    pub dir: PathBuf,
    pub id: String,
    pub name: String,
    pub verdict: ClusterVerdict,
    pub node_count: usize,
    pub virtual_ns: u64,
    pub wall_ms: u64,
    pub tx_frames: u64,
    pub dropped_frames: u64,
    pub fault_count: u64,
    pub modified: SystemTime,
}

impl ClusterEntry {
    pub fn read(dir: &Path) -> Option<ClusterEntry> {
        let manifest_path = dir.join(CLUSTER_MANIFEST);
        let modified = fs::metadata(&manifest_path).ok()?.modified().ok()?;
        let bytes = fs::read(&manifest_path).ok()?;
        let manifest: ClusterManifest = serde_json::from_slice(&bytes).ok()?;
        let (virtual_ns, wall_ms, tx_frames, dropped_frames, fault_count) = match &manifest.outcome
        {
            Some(o) => (
                o.cluster_ns,
                o.wall_ms,
                o.tx_frames,
                o.dropped_frames,
                o.partitions + o.crashes + o.restarts,
            ),
            None => (0, 0, 0, 0, 0),
        };
        Some(ClusterEntry {
            dir: dir.to_path_buf(),
            id: manifest.id.as_str().to_string(),
            name: manifest.name,
            verdict: manifest.verdict,
            node_count: manifest.nodes.len(),
            virtual_ns,
            wall_ms,
            tx_frames,
            dropped_frames,
            fault_count,
            modified,
        })
    }

    pub fn matches(&self, query: &str) -> bool {
        let q = query.trim().to_ascii_lowercase();
        if q.is_empty() {
            return true;
        }
        self.name.to_ascii_lowercase().contains(&q) || self.id.to_ascii_lowercase().contains(&q)
    }

    pub fn headline(&self) -> (&'static str, bool) {
        match &self.verdict {
            ClusterVerdict::Passed => ("passed", false),
            ClusterVerdict::Failed { .. } => ("failed", true),
            ClusterVerdict::Unfinished => ("unfinished", true),
        }
    }
}

/// Scans `$REWIND_HOME/clusters/` and returns clusters ordered newest-first.
pub fn scan_clusters(dir: &Path) -> Vec<ClusterEntry> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<ClusterEntry> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter_map(|p| ClusterEntry::read(&p))
        .collect();
    out.sort_by_key(|a| std::cmp::Reverse(a.modified));
    out
}

/// Resolved per-node info for a cluster loaded in the desktop app.
#[derive(Clone, Debug)]
pub struct ClusterNodeInfo {
    pub idx: usize,
    pub name: String,
    pub ip: String,
    pub role: String,
    pub zone: Option<String>,
    pub rack: Option<String>,
    pub dc: Option<String>,
    pub steps: u64,
    pub local_ns: u64,
    pub boot_count: u32,
    pub status: Option<i32>,
    pub run_dir: PathBuf,
}

/// State of a node at a given virtual timestamp on the cluster swimlane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeSpanKind {
    Running,
    Crashed,
    Paused,
    Clogged,
}

/// Contiguous virtual-time interval of a node's state, for swimlane rendering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeSpan {
    pub start_ns: u64,
    pub end_ns: u64,
    pub kind: NodeSpanKind,
}

/// Tone for coloring a formatted cluster network/fault row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetEventTone {
    Packet,
    Drop,
    FaultRed,
    FaultAmber,
    FaultBlue,
    FaultGreen,
}

/// Pre-formatted view of one `NetEvent` row.
#[derive(Clone, Debug)]
pub struct FormattedNetEvent {
    pub ns: u64,
    pub badge: &'static str,
    pub tone: NetEventTone,
    pub node_idx: Option<usize>,
    pub summary: String,
}

/// Which subset of `network.bin` events the Cluster tab lists.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClusterEventFilter {
    #[default]
    All,
    Faults,
    Drops,
    ActiveNode,
}

/// A full recorded multi-node cluster loaded for the desktop app.
#[derive(Clone, Debug)]
pub struct ClusterData {
    pub dir: PathBuf,
    pub manifest: ClusterManifest,
    pub nodes: Vec<ClusterNodeInfo>,
    pub total_ns: u64,
    pub events: Vec<NetEvent>,
    /// Indices into `events` of all fault/control events.
    pub fault_indices: Vec<usize>,
    /// Indices into `events` of all dropped-frame + fault events.
    pub drop_indices: Vec<usize>,
    /// Indices into `events` of `Tx`, `Drop`, `Duplicate`, and faults (omits
    /// redundant `Deliver` rows so the stream stays dense and readable).
    pub visible_indices: Vec<usize>,
    /// Precomputed per-node state spans for the swimlane timeline.
    pub spans: Vec<Vec<NodeSpan>>,
    /// Precomputed cluster-wide partition intervals `[start_ns, end_ns]`.
    pub partition_spans: Vec<(u64, u64)>,
}

impl ClusterData {
    /// Detects whether `path` is a cluster directory (`cluster.json`), a file
    /// inside one, or a node run directory (`clusters/<id>/nodes/<name>`).
    /// Returns `(cluster_dir, optional_node_name)`.
    pub fn detect(path: &Path) -> Option<(PathBuf, Option<String>)> {
        if path.is_dir() && path.join(CLUSTER_MANIFEST).is_file() {
            return Some((path.to_path_buf(), None));
        }
        if path.is_file()
            && path
                .file_name()
                .is_some_and(|n| n == CLUSTER_MANIFEST || n == CLUSTER_NETWORK_LOG)
            && let Some(parent) = path.parent()
            && parent.join(CLUSTER_MANIFEST).is_file()
        {
            return Some((parent.to_path_buf(), None));
        }
        let dir = if path.is_dir() { path } else { path.parent()? };
        let nodes_parent = dir.parent()?;
        if nodes_parent.file_name().is_some_and(|n| n == CLUSTER_NODES_DIR)
            && let Some(cluster_dir) = nodes_parent.parent()
            && cluster_dir.join(CLUSTER_MANIFEST).is_file()
        {
            let node_name = dir.file_name()?.to_string_lossy().into_owned();
            return Some((cluster_dir.to_path_buf(), Some(node_name)));
        }
        None
    }

    /// Loads `cluster.json` and `network.bin` from `dir`.
    pub fn open(dir: &Path) -> Result<ClusterData> {
        let manifest_path = dir.join(CLUSTER_MANIFEST);
        let bytes = fs::read(&manifest_path)
            .with_context(|| format!("reading {}", manifest_path.display()))?;
        let manifest: ClusterManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", manifest_path.display()))?;

        let mut nodes = Vec::with_capacity(manifest.nodes.len());
        for (idx, nm) in manifest.nodes.iter().enumerate() {
            let outcome = manifest
                .outcome
                .as_ref()
                .and_then(|o| o.nodes.iter().find(|r| r.name == nm.name));
            nodes.push(ClusterNodeInfo {
                idx,
                name: nm.name.clone(),
                ip: nm.ip.clone(),
                role: nm.role.clone(),
                zone: nm.zone.clone(),
                rack: nm.rack.clone(),
                dc: nm.dc.clone(),
                steps: outcome.map_or(0, |o| o.steps),
                local_ns: outcome.map_or(0, |o| o.local_ns),
                boot_count: outcome.map_or(1, |o| o.boot_count),
                status: outcome.and_then(|o| o.status),
                run_dir: dir.join(CLUSTER_NODES_DIR).join(&nm.name),
            });
        }

        let net_path = dir.join(CLUSTER_NETWORK_LOG);
        let events = if net_path.is_file() {
            let f = File::open(&net_path)
                .with_context(|| format!("opening {}", net_path.display()))?;
            let reader = NetLogReader::new(BufReader::new(f))
                .with_context(|| format!("reading header of {}", net_path.display()))?;
            let mut out = Vec::new();
            for item in reader {
                out.push(item.with_context(|| format!("decoding {}", net_path.display()))?);
            }
            out
        } else {
            Vec::new()
        };

        let mut total_ns = manifest.outcome.as_ref().map_or(0, |o| o.cluster_ns);
        if let Some(last) = events.last() {
            total_ns = total_ns.max(last.ns());
        }
        for n in &nodes {
            total_ns = total_ns.max(n.local_ns);
        }

        let mut fault_indices = Vec::new();
        let mut drop_indices = Vec::new();
        let mut visible_indices = Vec::new();
        for (i, ev) in events.iter().enumerate() {
            if ev.is_fault() {
                fault_indices.push(i);
                drop_indices.push(i);
                visible_indices.push(i);
            } else if matches!(ev, NetEvent::Drop { .. }) {
                drop_indices.push(i);
                visible_indices.push(i);
            } else if !matches!(ev, NetEvent::Deliver { .. }) {
                visible_indices.push(i);
            }
        }

        let (spans, partition_spans) = build_spans(nodes.len(), total_ns, &events, &fault_indices);

        Ok(ClusterData {
            dir: dir.to_path_buf(),
            manifest,
            nodes,
            total_ns,
            events,
            fault_indices,
            drop_indices,
            visible_indices,
            spans,
            partition_spans,
        })
    }

    /// Picks the best default node when opening a cluster directory directly:
    /// the first `"driver"` node if present (e.g. `control` / `client`), else `0`.
    pub fn default_node_idx(&self) -> usize {
        self.nodes
            .iter()
            .position(|n| n.role == "driver")
            .unwrap_or(0)
    }

    pub fn node_idx_by_name(&self, name: &str) -> Option<usize> {
        self.nodes.iter().position(|n| n.name == name)
    }

    pub fn node_idx_for_path(&self, path: &Path) -> Option<usize> {
        self.nodes
            .iter()
            .position(|n| n.run_dir == path)
            .or_else(|| {
                let name = path.file_name()?.to_str()?;
                self.node_idx_by_name(name)
            })
    }

    pub fn node_name(&self, idx: usize) -> &str {
        self.nodes
            .get(idx)
            .map_or("?", |n| n.name.as_str())
    }

    /// Node state at virtual time `ns`.
    pub fn node_state_at(&self, node_idx: usize, ns: u64) -> NodeSpanKind {
        let Some(node_spans) = self.spans.get(node_idx) else {
            return NodeSpanKind::Running;
        };
        let pos = node_spans.partition_point(|s| s.end_ns < ns);
        node_spans
            .get(pos)
            .filter(|s| s.start_ns <= ns && ns <= s.end_ns)
            .map_or(NodeSpanKind::Running, |s| s.kind)
    }

    /// Whether any network partition is active at virtual time `ns`.
    pub fn partitioned_at(&self, ns: u64) -> bool {
        self.partition_spans
            .iter()
            .any(|&(s, e)| s <= ns && ns <= e)
    }

    /// Maps a node's local scrubber `step` to approximate cluster virtual time (`ns`).
    pub fn ns_for_step(&self, node_idx: usize, step: u64, trace: &Trace) -> u64 {
        let last_step = trace.last_step().max(1);
        let is_multiboot = self.nodes.get(node_idx).is_some_and(|n| n.boot_count > 1);
        if !is_multiboot && !trace.events.is_empty() {
            let idx = trace.events.partition_point(|e| e.step < step);
            let ev = trace
                .events
                .get(idx)
                .or_else(|| trace.events.last());
            if let Some(ev) = ev {
                return ev.ns.min(self.total_ns.max(ev.ns));
            }
        }
        // Multi-boot or empty-events fallback: monotonic proportional mapping over cluster_ns.
        let ratio = (step.min(last_step) as f64) / (last_step as f64);
        (ratio * self.total_ns as f64).round() as u64
    }

    /// Inverse mapping: finds the step on `trace` closest to cluster virtual time `ns`.
    pub fn step_for_ns(&self, node_idx: usize, ns: u64, trace: &Trace) -> u64 {
        let last_step = trace.last_step();
        if last_step == 0 {
            return 0;
        }
        let is_multiboot = self.nodes.get(node_idx).is_some_and(|n| n.boot_count > 1);
        if !is_multiboot && !trace.events.is_empty() {
            let idx = trace.events.partition_point(|e| e.ns < ns);
            return trace
                .events
                .get(idx)
                .or_else(|| trace.events.last())
                .map_or(last_step, |e| e.step);
        }
        if self.total_ns == 0 {
            return 0;
        }
        let ratio = (ns.min(self.total_ns) as f64) / (self.total_ns as f64);
        (ratio * last_step as f64).round() as u64
    }

    /// Returns event indices matching `filter` (and `active_node` when filtering by node).
    pub fn filtered_indices(
        &self,
        filter: ClusterEventFilter,
        active_node: Option<usize>,
    ) -> Vec<usize> {
        match filter {
            ClusterEventFilter::All => self.visible_indices.clone(),
            ClusterEventFilter::Faults => self.fault_indices.clone(),
            ClusterEventFilter::Drops => self.drop_indices.clone(),
            ClusterEventFilter::ActiveNode => {
                let Some(target) = active_node else {
                    return self.visible_indices.clone();
                };
                self.visible_indices
                    .iter()
                    .copied()
                    .filter(|&i| event_touches_node(&self.events[i], target))
                    .collect()
            }
        }
    }

    /// Formats one `NetEvent` for display in the Cluster tab.
    pub fn format_event(&self, ev: &NetEvent) -> FormattedNetEvent {
        match ev {
            NetEvent::Tx {
                ns,
                src,
                dst_mac,
                len,
                summary,
                ..
            } => FormattedNetEvent {
                ns: *ns,
                badge: "TX",
                tone: NetEventTone::Packet,
                node_idx: Some(*src),
                summary: format!(
                    "{} -> {} ({len} B)  {summary}",
                    self.node_name(*src),
                    self.mac_label(dst_mac)
                ),
            },
            NetEvent::Deliver {
                ns,
                src,
                dst,
                len,
                latency_ns,
            } => FormattedNetEvent {
                ns: *ns,
                badge: "DELIVER",
                tone: NetEventTone::Packet,
                node_idx: Some(*dst),
                summary: format!(
                    "{} -> {} ({len} B, {:.2}ms)",
                    self.node_name(*src),
                    self.node_name(*dst),
                    *latency_ns as f64 / 1e6
                ),
            },
            NetEvent::Drop {
                ns,
                src,
                dst,
                len,
                reason,
            } => {
                let why = match reason {
                    DropReason::Partitioned => "partitioned",
                    DropReason::RandomDrop => "random",
                    DropReason::NodeCrashed => "crashed",
                    DropReason::RxOverflow => "rx-overflow",
                };
                FormattedNetEvent {
                    ns: *ns,
                    badge: "DROP",
                    tone: NetEventTone::Drop,
                    node_idx: Some(*src),
                    summary: format!(
                        "{} -> {} ({len} B, {why})",
                        self.node_name(*src),
                        self.node_name(*dst)
                    ),
                }
            }
            NetEvent::Duplicate {
                ns,
                src,
                dst,
                len,
                extra_delay_ns,
            } => FormattedNetEvent {
                ns: *ns,
                badge: "DUP",
                tone: NetEventTone::FaultAmber,
                node_idx: Some(*src),
                summary: format!(
                    "{} -> {} ({len} B, +{:.2}ms)",
                    self.node_name(*src),
                    self.node_name(*dst),
                    *extra_delay_ns as f64 / 1e6
                ),
            },
            NetEvent::Partition {
                ns,
                blocked_pairs,
                asymmetric,
            } => FormattedNetEvent {
                ns: *ns,
                badge: "PARTITION",
                tone: NetEventTone::FaultBlue,
                node_idx: None,
                summary: format!(
                    "{} partition ({blocked_pairs} directed pair{})",
                    if *asymmetric { "asymmetric" } else { "symmetric" },
                    if *blocked_pairs == 1 { "" } else { "s" }
                ),
            },
            NetEvent::Heal { ns } => FormattedNetEvent {
                ns: *ns,
                badge: "HEAL",
                tone: NetEventTone::FaultGreen,
                node_idx: None,
                summary: "healed network partitions".into(),
            },
            NetEvent::NodeCrash { ns, node } => FormattedNetEvent {
                ns: *ns,
                badge: "CRASH",
                tone: NetEventTone::FaultRed,
                node_idx: Some(*node),
                summary: format!("hard crashed {}", self.node_name(*node)),
            },
            NetEvent::NodeRestart {
                ns,
                node,
                boot_count,
            } => FormattedNetEvent {
                ns: *ns,
                badge: "RESTART",
                tone: NetEventTone::FaultGreen,
                node_idx: Some(*node),
                summary: format!("restarted {} (boot #{boot_count})", self.node_name(*node)),
            },
            NetEvent::NodePause { ns, node } => FormattedNetEvent {
                ns: *ns,
                badge: "PAUSE",
                tone: NetEventTone::FaultAmber,
                node_idx: Some(*node),
                summary: format!("paused vCPU on {}", self.node_name(*node)),
            },
            NetEvent::NodeResume { ns, node } => FormattedNetEvent {
                ns: *ns,
                badge: "RESUME",
                tone: NetEventTone::FaultGreen,
                node_idx: Some(*node),
                summary: format!("resumed vCPU on {}", self.node_name(*node)),
            },
            NetEvent::Clog {
                ns,
                from,
                to,
                until_ns,
                mode,
            } => {
                let dir = match (from, to) {
                    (Some(a), Some(b)) => format!("{} -> {}", self.node_name(*a), self.node_name(*b)),
                    (Some(a), None) => format!("{} ({mode:?})", self.node_name(*a)),
                    (None, Some(b)) => format!("-> {}", self.node_name(*b)),
                    (None, None) => "all".into(),
                };
                let dur_ms = until_ns.saturating_sub(*ns) as f64 / 1e6;
                FormattedNetEvent {
                    ns: *ns,
                    badge: "CLOG",
                    tone: NetEventTone::FaultAmber,
                    node_idx: (*from).or(*to),
                    summary: format!("clogged {dir} for {dur_ms:.1}ms"),
                }
            }
            NetEvent::Unclog { ns, from, to } => {
                let dir = match (from, to) {
                    (Some(a), Some(b)) => format!("{} -> {}", self.node_name(*a), self.node_name(*b)),
                    (Some(a), None) => self.node_name(*a).to_string(),
                    (None, Some(b)) => self.node_name(*b).to_string(),
                    (None, None) => "all".into(),
                };
                FormattedNetEvent {
                    ns: *ns,
                    badge: "UNCLOG",
                    tone: NetEventTone::FaultGreen,
                    node_idx: (*from).or(*to),
                    summary: format!("unclogged {dir}"),
                }
            }
            NetEvent::ClockStep { ns, node, delta_ns } => FormattedNetEvent {
                ns: *ns,
                badge: "CLOCK",
                tone: NetEventTone::FaultBlue,
                node_idx: Some(*node),
                summary: format!(
                    "stepped {} clock {:+.1}ms",
                    self.node_name(*node),
                    *delta_ns as f64 / 1e6
                ),
            },
            NetEvent::ClockDrift {
                ns,
                node,
                drift_ppm,
            } => FormattedNetEvent {
                ns: *ns,
                badge: "DRIFT",
                tone: NetEventTone::FaultBlue,
                node_idx: Some(*node),
                summary: format!(
                    "set {} clock drift {:+} ppm",
                    self.node_name(*node),
                    drift_ppm
                ),
            },
            NetEvent::LinkUpdated {
                ns,
                from,
                to,
                latency_ns,
                jitter_ns,
                drop_ppm,
                ..
            } => FormattedNetEvent {
                ns: *ns,
                badge: "LINK",
                tone: NetEventTone::FaultBlue,
                node_idx: Some(*from),
                summary: format!(
                    "{} -> {} lat={:.1}ms jit={:.1}ms drop={:.1}%",
                    self.node_name(*from),
                    self.node_name(*to),
                    *latency_ns as f64 / 1e6,
                    *jitter_ns as f64 / 1e6,
                    *drop_ppm as f64 / 10_000.0
                ),
            },
            NetEvent::NemesisToggled { ns, enabled } => FormattedNetEvent {
                ns: *ns,
                badge: "NEMESIS",
                tone: if *enabled {
                    NetEventTone::FaultAmber
                } else {
                    NetEventTone::FaultGreen
                },
                node_idx: None,
                summary: if *enabled {
                    "nemesis enabled".into()
                } else {
                    "nemesis paused (settle phase)".into()
                },
            },
            NetEvent::BuggifyToggled { ns, enabled } => FormattedNetEvent {
                ns: *ns,
                badge: "BUGGIFY",
                tone: if *enabled {
                    NetEventTone::FaultAmber
                } else {
                    NetEventTone::FaultGreen
                },
                node_idx: None,
                summary: if *enabled {
                    "buggify enabled".into()
                } else {
                    "buggify disabled".into()
                },
            },
        }
    }

    fn mac_label(&self, mac: &str) -> String {
        if mac == "ff:ff:ff:ff:ff:ff" {
            return "broadcast".into();
        }
        if mac == "52:54:00:fe:fe:fe" {
            return "http://rewind".into();
        }
        for nm in &self.manifest.nodes {
            if rewind_trace::net_log::format_mac(nm.mac) == mac {
                return nm.name.clone();
            }
        }
        mac.to_string()
    }
}

fn event_touches_node(ev: &NetEvent, target: usize) -> bool {
    match *ev {
        NetEvent::Tx { src, .. } => src == target,
        NetEvent::Deliver { src, dst, .. }
        | NetEvent::Drop { src, dst, .. }
        | NetEvent::Duplicate { src, dst, .. } => src == target || dst == target,
        NetEvent::NodeCrash { node, .. }
        | NetEvent::NodeRestart { node, .. }
        | NetEvent::NodePause { node, .. }
        | NetEvent::NodeResume { node, .. }
        | NetEvent::ClockStep { node, .. }
        | NetEvent::ClockDrift { node, .. } => node == target,
        NetEvent::LinkUpdated { from, to, .. } => from == target || to == target,
        NetEvent::Clog { from, to, .. } | NetEvent::Unclog { from, to, .. } => {
            from == Some(target) || to == Some(target) || (from.is_none() && to.is_none())
        }
        NetEvent::Partition { .. }
        | NetEvent::Heal { .. }
        | NetEvent::NemesisToggled { .. }
        | NetEvent::BuggifyToggled { .. } => true,
    }
}

fn build_spans(
    node_count: usize,
    total_ns: u64,
    events: &[NetEvent],
    fault_indices: &[usize],
) -> (Vec<Vec<NodeSpan>>, Vec<(u64, u64)>) {
    let mut states = vec![NodeSpanKind::Running; node_count];
    let mut state_start = vec![0u64; node_count];
    let mut spans: Vec<Vec<NodeSpan>> = vec![Vec::new(); node_count];
    let mut partition_spans = Vec::new();
    let mut partition_start: Option<u64> = None;

    let mut transition =
        |idx: usize, at_ns: u64, only_if: Option<NodeSpanKind>, next: NodeSpanKind| {
            if idx >= node_count {
                return;
            }
            if let Some(expected) = only_if
                && states[idx] != expected
            {
                return;
            }
            if states[idx] != next {
                if at_ns > state_start[idx] {
                    spans[idx].push(NodeSpan {
                        start_ns: state_start[idx],
                        end_ns: at_ns,
                        kind: states[idx],
                    });
                }
                states[idx] = next;
                state_start[idx] = at_ns;
            }
        };

    for &fi in fault_indices {
        match events[fi] {
            NetEvent::NodeCrash { ns, node } => transition(node, ns, None, NodeSpanKind::Crashed),
            NetEvent::NodeRestart { ns, node, .. } => {
                transition(node, ns, None, NodeSpanKind::Running)
            }
            NetEvent::NodePause { ns, node } => transition(node, ns, None, NodeSpanKind::Paused),
            NetEvent::NodeResume { ns, node } => transition(node, ns, None, NodeSpanKind::Running),
            NetEvent::Clog { ns, from, to, .. } => {
                if let Some(a) = from {
                    transition(a, ns, Some(NodeSpanKind::Running), NodeSpanKind::Clogged);
                }
                if let Some(b) = to {
                    transition(b, ns, Some(NodeSpanKind::Running), NodeSpanKind::Clogged);
                }
            }
            NetEvent::Unclog { ns, from, to } => {
                if from.is_none() && to.is_none() {
                    for idx in 0..node_count {
                        transition(idx, ns, Some(NodeSpanKind::Clogged), NodeSpanKind::Running);
                    }
                } else {
                    if let Some(a) = from {
                        transition(a, ns, Some(NodeSpanKind::Clogged), NodeSpanKind::Running);
                    }
                    if let Some(b) = to {
                        transition(b, ns, Some(NodeSpanKind::Clogged), NodeSpanKind::Running);
                    }
                }
            }
            NetEvent::Partition { ns, .. } => {
                if partition_start.is_none() {
                    partition_start = Some(ns);
                }
            }
            NetEvent::Heal { ns } => {
                if let Some(start) = partition_start.take() {
                    partition_spans.push((start, ns));
                }
            }
            _ => {}
        }
    }

    for idx in 0..node_count {
        spans[idx].push(NodeSpan {
            start_ns: state_start[idx],
            end_ns: total_ns.max(state_start[idx]),
            kind: states[idx],
        });
    }
    if let Some(start) = partition_start {
        partition_spans.push((start, total_ns.max(start)));
    }

    (spans, partition_spans)
}

/// Formats virtual nanoseconds compactly (`12.3ms` or `48.5s`).
pub fn format_virtual_ns(ns: u64) -> String {
    let ms = ns as f64 / 1_000_000.0;
    if ms >= 10_000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else if ms >= 100.0 {
        format!("{ms:.1}ms")
    } else {
        format!("{ms:.2}ms")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rewind_trace::cluster::CLUSTER_MANIFEST_VERSION;
    use rewind_trace::net_log::{ClogMode, NetLogWriter};

    fn sample_manifest_json(id: &str, name: &str) -> serde_json::Value {
        let mk_node = |n: &str, ip: &str, role: &str, mac_last: u8| {
            serde_json::json!({
                "name": n,
                "ip": ip,
                "mac": [0x52, 0x54, 0x00, 0x00, 0x00, mac_last],
                "role": role,
                "zone": "az1",
                "clock_skew_ns": 0,
                "clock_drift_ppm": 0,
                "spec": {
                    "kernel": "/kernel",
                    "initrd": "/initrd",
                    "kernel_debug": null,
                    "image": null,
                    "image_hash": null,
                    "mem_mib": 256,
                    "cores": 1,
                    "seed": 42,
                    "epoch": 0,
                    "quantum": 1000,
                    "schedule": 7,
                    "schedule_from": 0,
                    "schedule_until": 0,
                    "inherited_schedules": [],
                    "reseeds": [],
                    "cpu": "host",
                    "clock": "exits",
                    "preemption": "at_exits",
                    "extras": "reserved",
                    "cmdline": "",
                    "job": {
                        "program": null,
                        "argv": ["/bin/true"],
                        "env": [],
                        "cwd": "/",
                        "uid": 0,
                        "gid": 0,
                        "hostname": n,
                        "root": "image",
                        "files": [],
                        "outputs": [],
                        "output": "plain"
                    }
                }
            })
        };
        serde_json::json!({
            "version": CLUSTER_MANIFEST_VERSION,
            "id": id,
            "name": name,
            "created": 1_700_000_000u64,
            "spec_source": "test",
            "seed": 42,
            "schedule": 7,
            "fault_window_from_ns": 0,
            "fault_window_until_ns": 0,
            "epoch": 0,
            "cpu": "host",
            "clock": "exits",
            "stop_when": "driver",
            "max_virtual_ns": 10_000_000_000u64,
            "slice_ns": 50_000,
            "network_json": {},
            "nodes": [
                mk_node("n1", "10.0.0.1", "service", 1),
                mk_node("control", "10.0.0.2", "driver", 2),
            ],
            "parent": null,
            "outcome": {
                "stop": "driver exited",
                "cluster_ns": 5_000_000_000u64,
                "cluster_step": 1000,
                "wall_ms": 250,
                "nodes": [
                    {
                        "name": "n1",
                        "role": "service",
                        "steps": 500,
                        "local_ns": 5_000_000_000u64,
                        "boot_count": 2,
                        "status": 0,
                        "verdict": { "state": "passed" }
                    },
                    {
                        "name": "control",
                        "role": "driver",
                        "steps": 500,
                        "local_ns": 5_000_000_000u64,
                        "boot_count": 1,
                        "status": 0,
                        "verdict": { "state": "passed" }
                    }
                ],
                "tx_frames": 2,
                "delivered_frames": 1,
                "dropped_frames": 1,
                "duplicated_frames": 0,
                "partitions": 1,
                "crashes": 1,
                "restarts": 1
            },
            "verdict": { "state": "passed" },
            "trace_hash": null,
            "recorded_by": "test"
        })
    }

    #[test]
    fn detect_open_and_spans_round_trip() {
        let base = std::env::temp_dir().join(format!(
            "rewind-app-cluster-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&base);
        let cdir = base.join("abcd1234abcd1234");
        fs::create_dir_all(cdir.join(CLUSTER_NODES_DIR).join("n1")).unwrap();
        fs::create_dir_all(cdir.join(CLUSTER_NODES_DIR).join("control")).unwrap();

        let manifest_json = sample_manifest_json("abcd1234abcd1234", "jepsen-test");
        fs::write(
            cdir.join(CLUSTER_MANIFEST),
            serde_json::to_vec_pretty(&manifest_json).unwrap(),
        )
        .unwrap();

        let events = vec![
            NetEvent::Partition {
                ns: 1_000_000_000,
                blocked_pairs: 2,
                asymmetric: false,
            },
            NetEvent::NodeCrash {
                ns: 1_500_000_000,
                node: 0,
            },
            NetEvent::Drop {
                ns: 1_600_000_000,
                src: 1,
                dst: 0,
                len: 64,
                reason: DropReason::NodeCrashed,
            },
            NetEvent::NodeRestart {
                ns: 2_500_000_000,
                node: 0,
                boot_count: 2,
            },
            NetEvent::Clog {
                ns: 3_000_000_000,
                from: Some(0),
                to: Some(1),
                until_ns: 3_500_000_000,
                mode: ClogMode::All,
            },
            NetEvent::Unclog {
                ns: 3_500_000_000,
                from: Some(0),
                to: Some(1),
            },
            NetEvent::Heal { ns: 4_000_000_000 },
            NetEvent::ClockStep {
                ns: 4_200_000_000,
                node: 0,
                delta_ns: 500_000_000,
            },
        ];
        {
            let f = File::create(cdir.join(CLUSTER_NETWORK_LOG)).unwrap();
            let mut w = NetLogWriter::new(f);
            for ev in &events {
                w.record(ev).unwrap();
            }
            w.finish().unwrap();
        }

        assert_eq!(ClusterData::detect(&cdir), Some((cdir.clone(), None)));
        assert_eq!(
            ClusterData::detect(&cdir.join(CLUSTER_MANIFEST)),
            Some((cdir.clone(), None))
        );
        assert_eq!(
            ClusterData::detect(&cdir.join(CLUSTER_NODES_DIR).join("n1")),
            Some((cdir.clone(), Some("n1".into())))
        );

        let entries = scan_clusters(&base);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "jepsen-test");
        assert!(entries[0].matches("jepsen"));
        assert!(!entries[0].matches("other"));

        let data = ClusterData::open(&cdir).unwrap();
        assert_eq!(data.default_node_idx(), 1); // driver node ("control")
        assert_eq!(data.events.len(), events.len());
        assert_eq!(data.fault_indices.len(), 7);
        assert_eq!(data.drop_indices.len(), 8);

        assert!(!data.partitioned_at(500_000_000));
        assert!(data.partitioned_at(2_000_000_000));
        assert!(!data.partitioned_at(4_500_000_000));

        assert_eq!(data.node_state_at(0, 500_000_000), NodeSpanKind::Running);
        assert_eq!(data.node_state_at(0, 2_000_000_000), NodeSpanKind::Crashed);
        assert_eq!(data.node_state_at(0, 3_200_000_000), NodeSpanKind::Clogged);
        assert_eq!(data.node_state_at(1, 3_200_000_000), NodeSpanKind::Clogged);
        assert_eq!(data.node_state_at(0, 3_800_000_000), NodeSpanKind::Running);
        assert_eq!(data.node_state_at(1, 3_800_000_000), NodeSpanKind::Running);

        let fmt_clock = data.format_event(events.last().unwrap());
        assert_eq!(fmt_clock.summary, "stepped n1 clock +500.0ms");

        let _ = fs::remove_dir_all(&base);
    }
}
