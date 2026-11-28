//! Language-agnostic NDJSON-RPC 2.0 server (`rewind cluster serve`) driving an
//! interactive [`ClusterSession`].
//!
//! Speaks one JSON object per line over `stdin`/`stdout` (default) or a Unix
//! domain socket (`--socket <PATH>`), allowing any language (Python, TypeScript,
//! Go, Rust, shell) to programmatically build, step, wait on, fault-inject,
//! inspect, snapshot, branch, and finalize deterministic Rewind clusters.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rewind_trace::cluster::{
    ClusterSpecFile, FaultSpecFile, LinkSpecFile, NemesisSpecFile, NetworkSpecFile, NodeFileSpec,
    NodeSpecFile,
};
use rewind_vmm::{ClockSource, CpuModel};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::cluster::{ClusterRun, ClusterRunOptions, parse_spec_file, prepare_cluster};
use crate::cluster_session::{
    ClusterSession, LogStream, cat_result_from_inspection, files_result_from_inspection,
};
use crate::home::{Guest, Home};
use crate::inspect::{Inspection, cat_on_machine, list_on_machine};
use crate::run::{CompareMachine, Keyframes};
use rewind_vmm::net::NetEvent;

/// Server defaults inherited from CLI flags and environment variables (such as
/// `REWIND_SEED`, `REWIND_SCHEDULE`, `REWIND_NEMESIS`, `REWIND_FAULT_FROM_MS`,
/// `REWIND_FAULT_UNTIL_MS`, `REWIND_KEYFRAMES` injected by `rewind cluster check`).
#[derive(Clone, Debug)]
pub struct ServeDefaults {
    pub home: Home,
    pub guest: Guest,
    pub cpu: CpuModel,
    pub clock: ClockSource,
    pub workdir: PathBuf,
}

#[derive(Deserialize)]
struct RpcRequest {
    #[serde(default)]
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Serialize)]
struct RpcError {
    code: i32,
    message: String,
}

/// Serves NDJSON-RPC 2.0 requests from `reader` and writes responses to `writer`.
pub fn serve_stream<R: BufRead, W: Write>(
    reader: R,
    mut writer: W,
    defaults: &ServeDefaults,
) -> Result<()> {
    let mut session: Option<ClusterSession> = None;
    let mut last_finished_dir: Option<PathBuf> = None;

    for line_res in reader.lines() {
        let line = line_res?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: RpcRequest = match serde_json::from_str(trimmed) {
            Ok(r) => r,
            Err(e) => {
                let resp = RpcResponse {
                    jsonrpc: "2.0",
                    id: Value::Null,
                    result: None,
                    error: Some(RpcError {
                        code: -32700,
                        message: format!("parse error: {e}"),
                    }),
                };
                writeln!(writer, "{}", serde_json::to_string(&resp)?)?;
                writer.flush()?;
                continue;
            }
        };

        let is_shutdown = req.method == "shutdown";
        let resp = match dispatch(
            &req.method,
            req.params,
            &mut session,
            &mut last_finished_dir,
            defaults,
        ) {
            Ok(val) => RpcResponse {
                jsonrpc: "2.0",
                id: req.id,
                result: Some(val),
                error: None,
            },
            Err(e) => RpcResponse {
                jsonrpc: "2.0",
                id: req.id,
                result: None,
                error: Some(RpcError {
                    code: -32000,
                    message: format!("{e:#}"),
                }),
            },
        };
        writeln!(writer, "{}", serde_json::to_string(&resp)?)?;
        writer.flush()?;
        if is_shutdown {
            break;
        }
    }
    Ok(())
}

/// Serves NDJSON-RPC 2.0 on a Unix domain socket at `path` (accepting one
/// client connection at a time until `shutdown` or EOF).
pub fn serve_unix_socket(path: &Path, defaults: &ServeDefaults) -> Result<()> {
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("binding cluster RPC socket at {}", path.display()))?;
    eprintln!("rewind cluster RPC listening on {}", path.display());
    for stream in listener.incoming() {
        let stream = stream?;
        let reader = BufReader::new(stream.try_clone()?);
        let res = serve_stream(reader, stream, defaults);
        if let Err(e) = res {
            eprintln!("rewind cluster RPC client error: {e:#}");
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct StartParams {
    #[serde(default = "default_cluster_name")]
    name: String,
    seed: Option<u64>,
    schedule: Option<u64>,
    nemesis_enabled: Option<bool>,
    fault_window_from_ms: Option<u64>,
    fault_window_until_ms: Option<u64>,
    epoch: Option<u64>,
    mem_mib: Option<u64>,
    cores: Option<u32>,
    max_virtual_ms: Option<u64>,
    slice_us: Option<u64>,
    #[serde(default)]
    packages: Vec<String>,
    stop_when: Option<String>,
    keyframes: Option<bool>,
    #[serde(default)]
    echo: bool,
    wall_timeout_ms: Option<u64>,
    #[serde(default)]
    network: NetworkSpecFile,
    #[serde(default)]
    links: Vec<LinkSpecFile>,
    #[serde(default)]
    faults: Vec<FaultSpecFile>,
    nemesis: Option<NemesisSpecFile>,
    nodes: Vec<NodeStartParams>,
}

#[derive(Deserialize)]
struct NodeStartParams {
    name: String,
    ip: Option<String>,
    role: Option<String>,
    root: Option<PathBuf>,
    #[serde(default)]
    packages: Vec<String>,
    command: Vec<String>,
    #[serde(default)]
    env: Vec<String>,
    cwd: Option<String>,
    uid: Option<u32>,
    gid: Option<u32>,
    mem_mib: Option<u64>,
    cores: Option<u32>,
    #[serde(default)]
    clock_skew_ms: i64,
    #[serde(default)]
    clock_drift_ppm: i32,
    #[serde(default)]
    display: bool,
    display_width: Option<u32>,
    display_height: Option<u32>,
    #[serde(default)]
    files: Vec<NodeFileSpec>,
}

fn default_cluster_name() -> String {
    "cluster".into()
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

fn env_bool(key: &str) -> Option<bool> {
    std::env::var(key).ok().and_then(|v| match v.as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    })
}

fn dispatch(
    method: &str,
    params: Value,
    session: &mut Option<ClusterSession>,
    last_finished_dir: &mut Option<PathBuf>,
    defaults: &ServeDefaults,
) -> Result<Value> {
    match method {
        "ping" => Ok(json!({ "ok": true, "version": crate::VERSION })),
        "shutdown" => {
            *session = None;
            *last_finished_dir = None;
            Ok(json!({ "ok": true }))
        }
        "cluster.start" => {
            let p: StartParams = serde_json::from_value(params)
                .context("invalid params for cluster.start")?;
            let mut network = p.network;
            network.links.extend(p.links);
            let spec = ClusterSpecFile {
                name: p.name,
                mem_mib: p.mem_mib,
                cores: p.cores,
                stop_when: p.stop_when,
                max_virtual_ms: p.max_virtual_ms,
                slice_us: p.slice_us,
                packages: p.packages,
                network,
                faults: p.faults,
                nemesis: p.nemesis,
                nodes: p
                    .nodes
                    .into_iter()
                    .map(|n| NodeSpecFile {
                        name: n.name,
                        ip: n.ip,
                        role: n.role,
                        root: n.root,
                        packages: n.packages,
                        command: n.command,
                        env: n.env,
                        cwd: n.cwd,
                        uid: n.uid,
                        gid: n.gid,
                        mem_mib: n.mem_mib,
                        cores: n.cores,
                        clock_skew_ms: n.clock_skew_ms,
                        clock_drift_ppm: n.clock_drift_ppm,
                        display: n.display,
                        display_width: n.display_width,
                        display_height: n.display_height,
                        files: n.files,
                    })
                    .collect(),
            };

            // Environment variables from `rewind cluster check` override defaults
            // so any user script works seamlessly under schedule/nemesis exploration.
            let seed = env_u64("REWIND_SEED").or(p.seed).unwrap_or(0);
            let schedule = env_u64("REWIND_SCHEDULE").or(p.schedule).unwrap_or(0);
            let nemesis_enabled = env_bool("REWIND_NEMESIS")
                .or(p.nemesis_enabled)
                .unwrap_or(false);
            let from_ms = env_u64("REWIND_FAULT_FROM_MS").or(p.fault_window_from_ms);
            let until_ms = env_u64("REWIND_FAULT_UNTIL_MS").or(p.fault_window_until_ms);
            let take_kf = env_bool("REWIND_KEYFRAMES")
                .or(p.keyframes)
                .unwrap_or(true);

            let opts = ClusterRunOptions {
                seed,
                schedule,
                fault_window_from_ns: from_ms.map_or(0, |m| m.saturating_mul(1_000_000)),
                fault_window_until_ns: until_ms.map_or(u64::MAX, |m| m.saturating_mul(1_000_000)),
                epoch: p.epoch,
                cpu: defaults.cpu,
                clock: defaults.clock,
                mem_mib_override: None,
                cores_override: None,
                max_virtual_ms_override: None,
                nemesis_enabled,
                keyframes: if take_kf {
                    Keyframes::Take
                } else {
                    Keyframes::Skip
                },
                echo: p.echo,
                wall_timeout: p.wall_timeout_ms.map(Duration::from_millis),
                parent: None,
            };

            *session = None;
            *last_finished_dir = None;
            let s = ClusterSession::start(
                defaults.home.clone(),
                &defaults.guest,
                &spec,
                &defaults.workdir,
                &opts,
            )?;
            let status = s.status();
            *session = Some(s);
            Ok(serde_json::to_value(status)?)
        }
        "cluster.status" => {
            let s = require_session(session)?;
            Ok(serde_json::to_value(s.status())?)
        }
        "cluster.advance" => {
            #[derive(Deserialize, Default)]
            struct P {
                ms: Option<u64>,
                us: Option<u64>,
                ns: Option<u64>,
                until_ms: Option<u64>,
                wall_timeout_ms: Option<u64>,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let delta_ns = p
                .ns
                .or_else(|| p.us.map(|u| u.saturating_mul(1_000)))
                .or_else(|| p.ms.map(|m| m.saturating_mul(1_000_000)));
            let until_ns = p.until_ms.map(|m| m.saturating_mul(1_000_000));
            let wall = p.wall_timeout_ms.map(Duration::from_millis);
            let s = require_session(session)?;
            Ok(serde_json::to_value(s.advance(delta_ns, until_ns, wall)?)?)
        }
        "cluster.wait_for" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                contains: String,
                stream: Option<String>,
                timeout_ms: Option<u64>,
                #[serde(default)]
                from_start: bool,
                wall_timeout_ms: Option<u64>,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.wait_for")?;
            let stream = match p.stream.as_deref() {
                Some(st) => LogStream::parse(st)?,
                None => LogStream::Output,
            };
            let timeout_ns = p.timeout_ms.map(|m| m.saturating_mul(1_000_000));
            let wall = p.wall_timeout_ms.map(Duration::from_millis);
            let s = require_session(session)?;
            Ok(serde_json::to_value(s.wait_for(
                &p.node,
                &p.contains,
                stream,
                timeout_ns,
                p.from_start,
                wall,
            )?)?)
        }
        "cluster.wait_any" => {
            #[derive(Deserialize)]
            struct P {
                #[serde(default)]
                nodes: Vec<String>,
                contains: String,
                stream: Option<String>,
                timeout_ms: Option<u64>,
                #[serde(default)]
                from_start: bool,
                wall_timeout_ms: Option<u64>,
            }
            let p: P =
                serde_json::from_value(params).context("invalid params for cluster.wait_any")?;
            let stream = match p.stream.as_deref() {
                Some(st) => LogStream::parse(st)?,
                None => LogStream::Output,
            };
            let timeout_ns = p.timeout_ms.map(|m| m.saturating_mul(1_000_000));
            let wall = p.wall_timeout_ms.map(Duration::from_millis);
            let s = require_session(session)?;
            Ok(serde_json::to_value(s.wait_any(
                &p.nodes,
                &p.contains,
                stream,
                timeout_ns,
                p.from_start,
                wall,
            )?)?)
        }
        "cluster.wait_signal" => {
            #[derive(Deserialize, Default)]
            struct P {
                name: Option<String>,
                names: Option<Vec<String>>,
                node: Option<String>,
                nodes: Option<Vec<String>>,
                barrier: Option<bool>,
                timeout_ms: Option<u64>,
                #[serde(default)]
                from_start: bool,
                wall_timeout_ms: Option<u64>,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.wait_signal")?;
            let names: Option<Vec<String>> = match (p.names, p.name) {
                (Some(v), _) => Some(v),
                (None, Some(n)) => Some(vec![n]),
                (None, None) => None,
            };
            let nodes: Option<Vec<String>> = match (p.nodes, p.node) {
                (Some(v), _) => Some(v),
                (None, Some(n)) => Some(vec![n]),
                (None, None) => None,
            };
            let timeout_ns = p.timeout_ms.map(|m| m.saturating_mul(1_000_000));
            let wall = p.wall_timeout_ms.map(Duration::from_millis);
            let s = require_session(session)?;
            Ok(serde_json::to_value(s.wait_signal(
                names.as_deref(),
                nodes.as_deref(),
                p.barrier,
                timeout_ns,
                p.from_start,
                wall,
            )?)?)
        }
        "cluster.release" => {
            use base64::Engine;
            #[derive(Deserialize)]
            struct P {
                name: String,
                node: Option<String>,
                status: Option<u16>,
                content_type: Option<String>,
                body: Option<String>,
                body_base64: Option<String>,
            }
            let p: P =
                serde_json::from_value(params).context("invalid params for cluster.release")?;
            let body = match (p.body, p.body_base64) {
                (Some(s), None) => s.into_bytes(),
                (None, Some(b64)) => base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .context("invalid base64 in cluster.release.body_base64")?,
                (None, None) => Vec::new(),
                (Some(_), Some(_)) => {
                    bail!("cluster.release accepts at most one of `body` or `body_base64`")
                }
            };
            let status = p.status.unwrap_or(200);
            let s = require_session(session)?;
            let released =
                s.release_barrier(&p.name, p.node.as_deref(), status, p.content_type, body)?;
            let st = s.status();
            Ok(json!({
                "cluster_ns": st.cluster_ns,
                "cluster_step": st.cluster_step,
                "released": released,
            }))
        }
        "cluster.signals" => {
            #[derive(Deserialize, Default)]
            struct P {
                #[serde(default)]
                since: u64,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let s = require_session(session)?;
            Ok(json!({
                "signals": s.signals(p.since),
            }))
        }
        "cluster.wait_exit" => {
            #[derive(Deserialize, Default)]
            struct P {
                node: Option<String>,
                timeout_ms: Option<u64>,
                wall_timeout_ms: Option<u64>,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let timeout_ns = p.timeout_ms.map(|m| m.saturating_mul(1_000_000));
            let wall = p.wall_timeout_ms.map(Duration::from_millis);
            let s = require_session(session)?;
            Ok(serde_json::to_value(s.wait_exit(
                p.node.as_deref(),
                timeout_ns,
                wall,
            )?)?)
        }
        "cluster.partition" => {
            #[derive(Deserialize)]
            struct P {
                a: Vec<String>,
                b: Option<Vec<String>>,
                #[serde(default = "default_true")]
                symmetric: bool,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.partition")?;
            let s = require_session(session)?;
            let blocked = s.partition(&p.a, p.b.as_deref(), p.symmetric)?;
            let st = s.status();
            Ok(json!({
                "cluster_ns": st.cluster_ns,
                "cluster_step": st.cluster_step,
                "blocked_links": blocked,
            }))
        }
        "cluster.heal" => {
            #[derive(Deserialize, Default)]
            struct P {
                a: Option<Vec<String>>,
                b: Option<Vec<String>>,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let s = require_session(session)?;
            s.heal(p.a.as_deref(), p.b.as_deref())?;
            let st = s.status();
            Ok(json!({
                "cluster_ns": st.cluster_ns,
                "cluster_step": st.cluster_step,
            }))
        }
        "cluster.set_link" => {
            #[derive(Deserialize)]
            struct P {
                from: String,
                to: String,
                latency_ms: Option<u64>,
                latency_us: Option<u64>,
                jitter_ms: Option<u64>,
                jitter_us: Option<u64>,
                drop_ppm: Option<u32>,
                duplicate_ppm: Option<u32>,
                #[serde(default = "default_true")]
                bidirectional: bool,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.set_link")?;
            let latency_ns = p
                .latency_us
                .map(|u| u.saturating_mul(1_000))
                .or_else(|| p.latency_ms.map(|m| m.saturating_mul(1_000_000)));
            let jitter_ns = p
                .jitter_us
                .map(|u| u.saturating_mul(1_000))
                .or_else(|| p.jitter_ms.map(|m| m.saturating_mul(1_000_000)));
            let s = require_session(session)?;
            s.set_link(
                &p.from,
                &p.to,
                latency_ns,
                jitter_ns,
                p.drop_ppm,
                p.duplicate_ppm,
                p.bidirectional,
            )?;
            let st = s.status();
            Ok(json!({
                "cluster_ns": st.cluster_ns,
                "cluster_step": st.cluster_step,
            }))
        }
        "cluster.crash" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.crash")?;
            let s = require_session(session)?;
            let boot_count = s.crash(&p.node)?;
            let st = s.status();
            Ok(json!({
                "cluster_ns": st.cluster_ns,
                "cluster_step": st.cluster_step,
                "boot_count": boot_count,
            }))
        }
        "cluster.restart" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.restart")?;
            let s = require_session(session)?;
            let boot_count = s.restart(&p.node)?;
            let st = s.status();
            Ok(json!({
                "cluster_ns": st.cluster_ns,
                "cluster_step": st.cluster_step,
                "boot_count": boot_count,
            }))
        }
        "cluster.send_input" => {
            use base64::Engine;
            #[derive(Deserialize)]
            struct P {
                node: String,
                data: Option<String>,
                data_base64: Option<String>,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.send_input")?;
            let bytes = match (p.data, p.data_base64) {
                (Some(s), None) => s.into_bytes(),
                (None, Some(b64)) => base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .context("invalid base64 in cluster.send_input.data_base64")?,
                _ => bail!("cluster.send_input requires exactly one of `data` or `data_base64`"),
            };
            let s = require_session(session)?;
            s.send_input(&p.node, bytes)?;
            let st = s.status();
            Ok(json!({
                "cluster_ns": st.cluster_ns,
                "cluster_step": st.cluster_step,
            }))
        }
        "cluster.send_hid" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                events: Vec<rewind_vmm::net::HidEvent>,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.send_hid")?;
            let s = require_session(session)?;
            s.send_hid(&p.node, p.events)?;
            let st = s.status();
            Ok(json!({
                "cluster_ns": st.cluster_ns,
                "cluster_step": st.cluster_step,
            }))
        }
        "cluster.logs" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                stream: Option<String>,
                #[serde(default)]
                since: usize,
                #[serde(default)]
                cluster: Option<String>,
                step: Option<u64>,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.logs")?;
            if p.cluster.is_some() || session.is_none() {
                let run = resolve_cluster_run(
                    p.cluster.as_deref(),
                    last_finished_dir,
                    defaults,
                    "cluster.logs",
                )?;
                let node_run = run.node_run(&p.node)?;
                let mut all_lines = Vec::new();
                node_run.output_lines(p.step, &mut |line| {
                    all_lines.push(line.text.to_string());
                })?;
                let next_offset = all_lines.len();
                let start = p.since.min(next_offset);
                return Ok(json!({
                    "lines": &all_lines[start..],
                    "next_offset": next_offset,
                }));
            }
            let stream = match p.stream.as_deref() {
                Some(st) => LogStream::parse(st)?,
                None => LogStream::Output,
            };
            let s = require_session(session)?;
            let (lines, next_offset) = s.logs(&p.node, stream, p.since)?;
            Ok(json!({ "lines": lines, "next_offset": next_offset }))
        }
        "cluster.net_events" => {
            #[derive(Deserialize, Default)]
            struct P {
                #[serde(default)]
                since: usize,
                #[serde(default)]
                cluster: Option<String>,
                node: Option<String>,
                #[serde(default)]
                faults: bool,
                #[serde(default)]
                drops: bool,
                from_ms: Option<u64>,
                to_ms: Option<u64>,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let (raw_events, next_offset, node_names): (Vec<NetEvent>, usize, Vec<String>) =
                if p.cluster.is_some() || session.is_none() {
                    let run = resolve_cluster_run(
                        p.cluster.as_deref(),
                        last_finished_dir,
                        defaults,
                        "cluster.net_events",
                    )?;
                    let all = run.net_events()?;
                    let total = all.len();
                    let start = p.since.min(total);
                    let names = run.manifest.nodes.iter().map(|n| n.name.clone()).collect();
                    (all[start..].to_vec(), total, names)
                } else {
                    let s = require_session(session)?;
                    let (events, next) = s.net_events(p.since);
                    (events, next, s.node_names().to_vec())
                };
            let filtered = filter_net_events(
                &raw_events,
                &node_names,
                p.node.as_deref(),
                p.faults,
                p.drops,
                p.from_ms,
                p.to_ms,
            )?;
            Ok(json!({ "events": filtered, "next_offset": next_offset }))
        }
        "cluster.ps" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                #[serde(default)]
                cluster: Option<String>,
                step: Option<u64>,
                #[serde(default)]
                all: bool,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.ps")?;
            if p.cluster.is_some() || session.is_none() {
                let run = resolve_cluster_run(
                    p.cluster.as_deref(),
                    last_finished_dir,
                    defaults,
                    "cluster.ps",
                )?;
                let node_run = run.node_run(&p.node)?;
                let trace = node_run.trace()?;
                Ok(processes_json(trace, p.step, p.all))
            } else {
                let s = require_session(session)?;
                let trace = s.node_trace(&p.node)?;
                Ok(processes_json(&trace, p.step, p.all))
            }
        }
        "cluster.events" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                #[serde(default)]
                cluster: Option<String>,
                from: Option<u64>,
                to: Option<u64>,
            }
            let p: P =
                serde_json::from_value(params).context("invalid params for cluster.events")?;
            let from = p.from.unwrap_or(0);
            let to = p.to.unwrap_or(u64::MAX);
            if p.cluster.is_some() || session.is_none() {
                let run = resolve_cluster_run(
                    p.cluster.as_deref(),
                    last_finished_dir,
                    defaults,
                    "cluster.events",
                )?;
                let node_run = run.node_run(&p.node)?;
                let trace = node_run.trace()?;
                let events: Vec<_> = trace
                    .events
                    .iter()
                    .filter(|e| (from..=to).contains(&e.step))
                    .collect();
                Ok(json!({ "events": events }))
            } else {
                let s = require_session(session)?;
                let trace = s.node_trace(&p.node)?;
                let events: Vec<_> = trace
                    .events
                    .iter()
                    .filter(|e| (from..=to).contains(&e.step))
                    .collect();
                Ok(json!({ "events": events }))
            }
        }
        "cluster.cat" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                path: String,
                pid: Option<u32>,
                #[serde(default)]
                cluster: Option<String>,
                step: Option<u64>,
                out_path: Option<PathBuf>,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.cat")?;
            let insp = if p.cluster.is_some() || p.step.is_some() || session.is_none() {
                let run = resolve_cluster_run(
                    p.cluster.as_deref(),
                    last_finished_dir,
                    defaults,
                    "cluster.cat",
                )?;
                let target_step = match p.step {
                    Some(st) => st,
                    None => run.node_run(&p.node)?.trace()?.last_step(),
                };
                let (mut machine, actual_step) =
                    run.seek_node(&defaults.home, &p.node, target_step)?;
                cat_on_machine(&mut machine, actual_step, p.pid, &p.path)?
            } else {
                let s = require_session(session)?;
                s.cat_inspection(&p.node, &p.path, p.pid)?
            };
            if let Some(out) = p.out_path {
                let resolved = resolve_path(out, defaults);
                match insp {
                    Inspection::Contents(bytes) => {
                        if let Some(parent) = resolved.parent()
                            && !parent.as_os_str().is_empty()
                        {
                            std::fs::create_dir_all(parent)?;
                        }
                        std::fs::write(&resolved, &bytes)
                            .with_context(|| format!("writing {}", resolved.display()))?;
                        Ok(json!({
                            "found": true,
                            "path": resolved.display().to_string(),
                            "bytes": bytes.len(),
                        }))
                    }
                    Inspection::NotFound(msg) | Inspection::Failed(msg) => Ok(json!({
                        "found": false,
                        "error": msg,
                    })),
                }
            } else {
                Ok(serde_json::to_value(cat_result_from_inspection(insp)?)?)
            }
        }
        "cluster.files" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                path: String,
                pid: Option<u32>,
                #[serde(default)]
                cluster: Option<String>,
                step: Option<u64>,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.files")?;
            if p.cluster.is_some() || p.step.is_some() || session.is_none() {
                let run = resolve_cluster_run(
                    p.cluster.as_deref(),
                    last_finished_dir,
                    defaults,
                    "cluster.files",
                )?;
                let target_step = match p.step {
                    Some(st) => st,
                    None => run.node_run(&p.node)?.trace()?.last_step(),
                };
                let (mut machine, actual_step) =
                    run.seek_node(&defaults.home, &p.node, target_step)?;
                let insp = list_on_machine(&mut machine, actual_step, p.pid, &p.path)?;
                Ok(serde_json::to_value(files_result_from_inspection(
                    &p.path, insp,
                )?)?)
            } else {
                let s = require_session(session)?;
                Ok(serde_json::to_value(s.files(&p.node, &p.path, p.pid)?)?)
            }
        }
        "cluster.screenshot" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                path: PathBuf,
                #[serde(default)]
                timestamp: bool,
                #[serde(default)]
                show_input: bool,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.screenshot")?;
            let resolved = if p.path.is_absolute() {
                p.path
            } else {
                defaults.workdir.join(p.path)
            };
            let s = require_session(session)?;
            Ok(serde_json::to_value(s.screenshot(
                &p.node,
                &resolved,
                p.timestamp,
                p.show_input,
            )?)?)
        }
        "cluster.export_video" => {
            #[derive(Deserialize)]
            struct P {
                node: String,
                path: PathBuf,
                #[serde(default)]
                cluster: Option<String>,
                fps: Option<u32>,
                from_ms: Option<u64>,
                to_ms: Option<u64>,
                #[serde(default)]
                timestamp: bool,
                #[serde(default)]
                show_input: bool,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.export_video")?;
            let resolved = if p.path.is_absolute() {
                p.path
            } else {
                defaults.workdir.join(p.path)
            };
            let opts = crate::cluster::VideoExportOptions {
                fps: p.fps.unwrap_or(30),
                from_ms: p.from_ms,
                to_ms: p.to_ms,
                timestamp_overlay: p.timestamp,
                input_overlay: p.show_input,
            };
            let run = resolve_cluster_run(
                p.cluster.as_deref(),
                last_finished_dir,
                defaults,
                "cluster.export_video",
            )?;
            let stats = run.export_video(&defaults.home, &p.node, &resolved, &opts)?;
            Ok(json!({
                "path": resolved.display().to_string(),
                "frames": stats.frames,
                "width": stats.width,
                "height": stats.height,
                "fps": stats.fps,
                "from_ns": stats.from_ns,
                "to_ns": stats.to_ns,
                "non_blank_frames": stats.non_blank_frames,
            }))
        }
        "cluster.export_pcap" => {
            #[derive(Deserialize)]
            struct P {
                path: PathBuf,
                #[serde(default)]
                cluster: Option<String>,
                node: Option<String>,
                from_ms: Option<u64>,
                to_ms: Option<u64>,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.export_pcap")?;
            let resolved = resolve_path(p.path, defaults);
            if let Some(parent) = resolved.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)?;
            }
            let run = resolve_cluster_run(
                p.cluster.as_deref(),
                last_finished_dir,
                defaults,
                "cluster.export_pcap",
            )?;
            let file = std::fs::File::create(&resolved)
                .with_context(|| format!("creating {}", resolved.display()))?;
            let mut out = std::io::BufWriter::new(file);
            let stats = run.export_pcap(
                &defaults.home,
                &mut out,
                p.from_ms,
                p.to_ms,
                p.node.as_deref(),
            )?;
            Ok(json!({
                "path": resolved.display().to_string(),
                "packets": stats.packets,
                "bytes": stats.bytes,
                "from_ns": stats.from_ns,
                "to_ns": stats.to_ns,
            }))
        }
        "cluster.run_spec" => {
            #[derive(Deserialize)]
            struct P {
                path: PathBuf,
                seed: Option<u64>,
                schedule: Option<u64>,
                epoch: Option<u64>,
                mem_mib: Option<u64>,
                cores: Option<u32>,
                max_virtual_ms: Option<u64>,
                nemesis_enabled: Option<bool>,
                keyframes: Option<bool>,
                #[serde(default)]
                echo: bool,
                wall_timeout_ms: Option<u64>,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.run_spec")?;
            let resolved = resolve_path(p.path, defaults);
            let spec_file = parse_spec_file(&resolved)?;
            let spec_dir = resolved
                .parent()
                .filter(|d| !d.as_os_str().is_empty())
                .map(Path::to_path_buf)
                .unwrap_or_else(|| defaults.workdir.clone());
            let spec_source = std::fs::read_to_string(&resolved)
                .with_context(|| format!("reading {}", resolved.display()))?;
            let take_kf = env_bool("REWIND_KEYFRAMES")
                .or(p.keyframes)
                .unwrap_or(true);
            let kf_mode = if take_kf {
                Keyframes::Take
            } else {
                Keyframes::Skip
            };
            let opts = ClusterRunOptions {
                seed: env_u64("REWIND_SEED").or(p.seed).unwrap_or(0),
                schedule: env_u64("REWIND_SCHEDULE").or(p.schedule).unwrap_or(0),
                fault_window_from_ns: env_u64("REWIND_FAULT_FROM_MS")
                    .map_or(0, |m| m.saturating_mul(1_000_000)),
                fault_window_until_ns: env_u64("REWIND_FAULT_UNTIL_MS")
                    .map_or(u64::MAX, |m| m.saturating_mul(1_000_000)),
                epoch: p.epoch,
                cpu: defaults.cpu,
                clock: defaults.clock,
                mem_mib_override: p.mem_mib,
                cores_override: p.cores,
                max_virtual_ms_override: p.max_virtual_ms,
                nemesis_enabled: env_bool("REWIND_NEMESIS")
                    .or(p.nemesis_enabled)
                    .unwrap_or(false),
                keyframes: kf_mode,
                echo: p.echo,
                wall_timeout: p.wall_timeout_ms.map(Duration::from_millis),
                parent: None,
            };
            *session = None;
            *last_finished_dir = None;
            let prepared = prepare_cluster(
                &defaults.home,
                &defaults.guest,
                &spec_file,
                &spec_dir,
                &spec_source,
                &opts,
            )?;
            let run = ClusterRun::record(&defaults.home, &prepared, kf_mode, p.echo, opts.wall_timeout)?;
            let outcome = run.manifest.outcome.clone();
            *last_finished_dir = Some(run.dir);
            Ok(json!({
                "id": run.manifest.id.as_str(),
                "name": run.manifest.name,
                "verdict": if run.manifest.verdict.passed() { "passed" } else { "failed" },
                "trace_hash": run.manifest.trace_hash,
                "outcome": outcome,
            }))
        }
        "cluster.ls" => {
            let runs = ClusterRun::list(&defaults.home)?;
            let clusters: Vec<_> = runs.into_iter().map(|r| r.manifest).collect();
            Ok(json!({ "clusters": clusters }))
        }
        "cluster.show" => {
            #[derive(Deserialize, Default)]
            struct P {
                #[serde(default)]
                cluster: Option<String>,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let run = resolve_cluster_run(
                p.cluster.as_deref(),
                last_finished_dir,
                defaults,
                "cluster.show",
            )?;
            Ok(serde_json::to_value(&run.manifest)?)
        }
        "cluster.remove" => {
            #[derive(Deserialize, Default)]
            struct P {
                #[serde(default)]
                clusters: Vec<String>,
                cluster: Option<String>,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.remove")?;
            let mut queries = p.clusters;
            if let Some(one) = p.cluster {
                queries.push(one);
            }
            if queries.is_empty() {
                bail!("cluster.remove requires at least one cluster id");
            }
            let runs: Vec<ClusterRun> = queries
                .iter()
                .map(|q| ClusterRun::find(&defaults.home, q))
                .collect::<Result<_>>()?;
            let mut removed = Vec::with_capacity(runs.len());
            for run in runs {
                let id = run.manifest.id.clone();
                if last_finished_dir.as_deref() == Some(run.dir.as_path()) {
                    *last_finished_dir = None;
                }
                ClusterRun::remove(&defaults.home, &id)?;
                removed.push(id.as_str().to_string());
            }
            Ok(json!({ "removed": removed }))
        }
        "cluster.replay" => {
            #[derive(Deserialize, Default)]
            struct P {
                #[serde(default)]
                cluster: Option<String>,
                machine: Option<String>,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let compare = match p.machine.as_deref().unwrap_or("every") {
                "every" => CompareMachine::Every,
                "last" => CompareMachine::Last,
                "none" | "never" => CompareMachine::Never,
                other => bail!("unknown machine check mode {other:?} (expected every, last, or none)"),
            };
            let run = resolve_cluster_run(
                p.cluster.as_deref(),
                last_finished_dir,
                defaults,
                "cluster.replay",
            )?;
            let replayed = run.replay(&defaults.home, compare)?;
            Ok(json!({
                "cluster": run.manifest.id.as_str(),
                "identical": replayed.identical,
                "total_events": replayed.total_events,
                "net_events": replayed.net_events,
                "keyframes_compared": replayed.keyframes_compared,
                "node_diffs": replayed.node_diffs,
                "net_diff": replayed.net_diff,
                "machine_diff": replayed.machine_diff,
            }))
        }
        "cluster.fork" => {
            #[derive(Deserialize)]
            struct P {
                #[serde(default)]
                cluster: Option<String>,
                at_ms: u64,
                #[serde(default = "default_fork_schedule")]
                schedule: u64,
                seed: Option<u64>,
                keyframes: Option<bool>,
                #[serde(default)]
                echo: bool,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.fork")?;
            let kf_mode = if p.keyframes.unwrap_or(true) {
                Keyframes::Take
            } else {
                Keyframes::Skip
            };
            let parent = resolve_cluster_run(
                p.cluster.as_deref(),
                last_finished_dir,
                defaults,
                "cluster.fork",
            )?;
            let forked = parent.fork(
                &defaults.home,
                p.at_ms,
                p.schedule,
                p.seed,
                kf_mode,
                p.echo,
            )?;
            let outcome = forked.manifest.outcome.clone();
            Ok(json!({
                "id": forked.manifest.id.as_str(),
                "name": forked.manifest.name,
                "verdict": if forked.manifest.verdict.passed() { "passed" } else { "failed" },
                "trace_hash": forked.manifest.trace_hash,
                "outcome": outcome,
                "parent": forked.manifest.parent,
            }))
        }
        "cluster.snapshot" => {
            #[derive(Deserialize, Default)]
            struct P {
                name: Option<String>,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let s = require_session(session)?;
            let (snapshot, cluster_ns, cluster_step) = s.snapshot(p.name)?;
            Ok(json!({
                "snapshot": snapshot,
                "cluster_ns": cluster_ns,
                "cluster_step": cluster_step,
            }))
        }
        "cluster.save_snapshot" => {
            #[derive(Deserialize)]
            struct P {
                path: PathBuf,
                name: Option<String>,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.save_snapshot")?;
            let resolved = if p.path.is_absolute() {
                p.path
            } else {
                defaults.workdir.join(p.path)
            };
            let s = require_session(session)?;
            let (snapshot, cluster_ns, cluster_step) = s.save_snapshot(p.name, &resolved)?;
            Ok(json!({
                "snapshot": snapshot,
                "path": resolved.display().to_string(),
                "cluster_ns": cluster_ns,
                "cluster_step": cluster_step,
            }))
        }
        "cluster.start_from_snapshot" => {
            #[derive(Deserialize)]
            struct P {
                path: PathBuf,
                name: Option<String>,
                keyframes: Option<bool>,
                #[serde(default)]
                echo: bool,
            }
            let p: P = serde_json::from_value(params)
                .context("invalid params for cluster.start_from_snapshot")?;
            let resolved = if p.path.is_absolute() {
                p.path
            } else {
                defaults.workdir.join(p.path)
            };
            let take_kf = env_bool("REWIND_KEYFRAMES")
                .or(p.keyframes)
                .unwrap_or(true);
            let kf_mode = if take_kf {
                Keyframes::Take
            } else {
                Keyframes::Skip
            };
            *session = None;
            *last_finished_dir = None;
            let s = ClusterSession::start_from_snapshot(
                defaults.home.clone(),
                &resolved,
                p.name,
                kf_mode,
                p.echo,
            )?;
            let status = s.status();
            *session = Some(s);
            Ok(serde_json::to_value(status)?)
        }
        "cluster.restore" => {
            #[derive(Deserialize)]
            struct P {
                snapshot: String,
            }
            let p: P = serde_json::from_value(params).context("invalid params for cluster.restore")?;
            let s = require_session(session)?;
            let (cluster_ns, cluster_step) = s.restore(&p.snapshot)?;
            Ok(json!({
                "snapshot": p.snapshot,
                "cluster_ns": cluster_ns,
                "cluster_step": cluster_step,
            }))
        }
        "cluster.finish" => {
            #[derive(Deserialize, Default)]
            struct P {
                verdict: Option<String>,
                reason: Option<String>,
            }
            let p: P = serde_json::from_value(params).unwrap_or_default();
            let verdict_override = match p.verdict.as_deref() {
                Some("passed" | "pass") => Some(true),
                Some("failed" | "fail") => Some(false),
                Some(other) => bail!("unknown verdict {other:?} (expected \"passed\" or \"failed\")"),
                None => None,
            };
            let s = require_session(session)?;
            let run = s.finish(verdict_override, p.reason)?;
            let outcome = run.manifest.outcome.clone();
            // Drop the live ClusterSession (and its HVF/KVM VMs) now that the
            // run is persisted on disk so subsequent `cluster.export_video` or
            // `cluster.start` calls can boot a fresh VM cleanly (macOS HVF
            // permits only one VM per process).
            *last_finished_dir = Some(run.dir);
            *session = None;
            Ok(json!({
                "id": run.manifest.id.as_str(),
                "name": run.manifest.name,
                "verdict": if run.manifest.verdict.passed() { "passed" } else { "failed" },
                "trace_hash": run.manifest.trace_hash,
                "outcome": outcome,
            }))
        }
        other => bail!("unknown RPC method {other:?}"),
    }
}

fn require_session(session: &mut Option<ClusterSession>) -> Result<&mut ClusterSession> {
    session
        .as_mut()
        .context("no active cluster session; call cluster.start first")
}

fn resolve_path(path: PathBuf, defaults: &ServeDefaults) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        defaults.workdir.join(path)
    }
}

fn resolve_cluster_run(
    cluster: Option<&str>,
    last_finished_dir: &Option<PathBuf>,
    defaults: &ServeDefaults,
    method: &str,
) -> Result<ClusterRun> {
    if let Some(q) = cluster {
        ClusterRun::find(&defaults.home, q)
    } else {
        let dir = last_finished_dir
            .as_deref()
            .with_context(|| format!("call cluster.finish() or pass `cluster` before {method}()"))?;
        ClusterRun::open(dir)
    }
}

fn net_event_ns(ev: &NetEvent) -> u64 {
    match ev {
        NetEvent::Tx { ns, .. }
        | NetEvent::Deliver { ns, .. }
        | NetEvent::Drop { ns, .. }
        | NetEvent::Duplicate { ns, .. }
        | NetEvent::Partition { ns, .. }
        | NetEvent::Heal { ns }
        | NetEvent::NodeCrash { ns, .. }
        | NetEvent::NodeRestart { ns, .. }
        | NetEvent::LinkUpdated { ns, .. } => *ns,
    }
}

fn net_event_involves(ev: &NetEvent, node: usize) -> bool {
    match ev {
        NetEvent::Tx { src, .. } => *src == node,
        NetEvent::Deliver { src, dst, .. }
        | NetEvent::Drop { src, dst, .. }
        | NetEvent::Duplicate { src, dst, .. } => *src == node || *dst == node,
        NetEvent::LinkUpdated { from, to, .. } => *from == node || *to == node,
        NetEvent::NodeCrash { node: n, .. } | NetEvent::NodeRestart { node: n, .. } => *n == node,
        NetEvent::Partition { .. } | NetEvent::Heal { .. } => true,
    }
}

fn filter_net_events(
    events: &[NetEvent],
    node_names: &[String],
    node: Option<&str>,
    faults: bool,
    drops: bool,
    from_ms: Option<u64>,
    to_ms: Option<u64>,
) -> Result<Vec<NetEvent>> {
    let filter_node = match node {
        Some(name) => {
            let idx = node_names.iter().position(|n| n == name).with_context(|| {
                format!("unknown node {name:?} (nodes: {})", node_names.join(", "))
            })?;
            Some(idx)
        }
        None => None,
    };
    let from_ns = from_ms.map_or(0, |ms| ms.saturating_mul(1_000_000));
    let to_ns = to_ms.map_or(u64::MAX, |ms| ms.saturating_mul(1_000_000));
    Ok(events
        .iter()
        .filter(|ev| {
            let ns = net_event_ns(ev);
            if ns < from_ns || ns > to_ns {
                return false;
            }
            if drops && !matches!(ev, NetEvent::Drop { .. }) {
                return false;
            }
            if faults && matches!(ev, NetEvent::Tx { .. } | NetEvent::Deliver { .. }) {
                return false;
            }
            if let Some(target) = filter_node
                && !net_event_involves(ev, target)
            {
                return false;
            }
            true
        })
        .cloned()
        .collect())
}

fn processes_json(trace: &rewind_trace::Trace, step: Option<u64>, all: bool) -> Value {
    let at = step.unwrap_or_else(|| trace.last_step());
    let processes: Vec<Value> = trace
        .processes()
        .into_iter()
        .filter(|p| p.alive_at(at))
        .filter(|p| {
            all || (p.pid != rewind_trace::KTHREADD_PID && p.parent != rewind_trace::KTHREADD_PID)
        })
        .map(|p| p.as_of(at))
        .map(|p| {
            let threads: Vec<u32> = p
                .threads
                .iter()
                .filter(|(_, start, end)| *start <= at && end.is_none_or(|e| at < e))
                .map(|(tid, _, _)| *tid)
                .collect();
            json!({
                "pid": p.pid,
                "parent": p.parent,
                "argv": p.argv,
                "execd": p.execd,
                "threads": threads,
            })
        })
        .collect();
    json!({ "step": at, "processes": processes })
}

fn default_fork_schedule() -> u64 {
    1
}

fn default_true() -> bool {
    true
}
