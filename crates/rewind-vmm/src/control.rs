//! Deterministic virtual coordinator endpoint (`http://rewind`, e.g. `10.0.0.254`)
//! built directly into the cluster's L2 virtual Ethernet switch.
//!
//! Exposes a zero-dependency, single-request HTTP/1.1 control plane over TCP
//! port 80 (plus ARP and ICMP Echo) so guest workloads in any language —
//! including non-root processes, `curl`, Python, Go, Java, Node, and browser
//! JS — can coordinate with the host test script without touching character
//! devices:
//!
//! - `GET  /meta`            → JSON metadata for the calling node & cluster peers
//! - `POST /signal/<name>`   → emits a deterministic [`ControlSignal`] and replies `200 OK`
//!   (`GET /signal/<name>` also accepted)
//! - `GET  /barrier/<name>`  → emits a barrier [`ControlSignal`] and holds the HTTP
//!   (`POST /barrier/<name>`   response open until the host invokes
//!   also accepted)            [`FaultKind::ControlRelease`] with an optional payload
//!
//! Because all replies and TCP state live deterministically inside
//! [`NetSwitchState`], cluster keyframes, snapshot restores, `rewind cluster
//! replay`, `video`, and `pcap` reproduce every exchange bit-for-bit, and
//! partitions between peer nodes never block a node from reaching the
//! coordinator.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Virtual MAC address of the switch-resident coordinator endpoint.
pub const CONTROL_MAC: [u8; 6] = [0x52, 0x54, 0x00, 0xfe, 0xfe, 0xfe];

/// Virtual TCP port served by the coordinator (`http://rewind`).
pub const CONTROL_PORT: u16 = 80;

/// Hostname injected into every guest's `/etc/hosts` pointing at `control_ip`.
pub const CONTROL_HOST: &str = "rewind";

/// Sentinel `InFlightFrame::src` value for frames originating from the switch's
/// virtual coordinator (never matches any node index in `blocked`).
pub const CONTROL_SRC_NODE: usize = usize::MAX;

const MAX_REQ_BYTES: usize = 64 * 1024;
const TCP_MSS: usize = 1460;

const ETH_TYPE_IPV4: u16 = 0x0800;
const ETH_TYPE_ARP: u16 = 0x0806;

const IP_PROTO_ICMP: u8 = 1;
const IP_PROTO_TCP: u8 = 6;

const TCP_FIN: u8 = 0x01;
const TCP_SYN: u8 = 0x02;
const TCP_RST: u8 = 0x04;
const TCP_PSH: u8 = 0x08;
const TCP_ACK: u8 = 0x10;

/// Default two-stage FDB-style BUGGIFY probabilities (25% activate per site,
/// 25% fire per call when activated).
pub const DEFAULT_BUGGIFY_ACTIVATE_PPM: u32 = 250_000;
pub const DEFAULT_BUGGIFY_FIRE_PPM: u32 = 250_000;
const BUGGIFY_PPM_SCALE: u64 = 1_000_000;

/// Static per-node metadata returned by `GET http://rewind/meta`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlNodeMeta {
    pub name: String,
    pub ip: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rack: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dc: Option<String>,
}

/// One deterministic signal or barrier arrival sent by a guest to `http://rewind`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlSignal {
    pub seq: u64,
    pub ns: u64,
    pub node: usize,
    pub name: String,
    pub payload: String,
    pub barrier: bool,
    pub conn_id: Option<u64>,
}

/// Connection 3-tuple identifying one guest TCP flow to `control_ip:80`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ControlConnKey {
    pub node: usize,
    pub src_ip: [u8; 4],
    pub src_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum ControlConnState {
    Established,
    WaitingBarrier { name: String },
    Sending { fin_sent: bool },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ControlConn {
    conn_id: u64,
    src_mac: [u8; 6],
    guest_seq: u32,
    ctrl_seq: u32,
    guest_wnd: u32,
    last_rx_ns: u64,
    continue_sent: bool,
    req_buf: Vec<u8>,
    tx_buf: Vec<u8>,
    tx_offset: usize,
    state: ControlConnState,
}

/// Serializable state of the virtual coordinator, captured inside
/// [`NetSwitchState`] so snapshots preserve in-flight barriers, BUGGIFY
/// activations, and CODE_PROBE counts cleanly.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneState {
    next_conn_id: u64,
    next_signal_seq: u64,
    next_ip_id: u16,
    conns: BTreeMap<ControlConnKey, ControlConn>,
    #[serde(default)]
    buggify_enabled: bool,
    #[serde(default)]
    buggify_sites: BTreeMap<String, bool>,
    #[serde(default)]
    buggify_calls: BTreeMap<String, u64>,
    #[serde(default)]
    probes: BTreeMap<String, u64>,
}

/// Output produced by handling one guest frame or one host barrier release.
#[derive(Clone, Debug, Default)]
pub(crate) struct ControlOutput {
    /// Outbound frames `(deliver_base_ns, dst_node, raw_ethernet_frame)` to
    /// enqueue onto `NetSwitch::in_flight`.
    pub frames: Vec<(u64, usize, Vec<u8>)>,
    /// Newly emitted guest signals / barrier arrivals.
    pub signals: Vec<ControlSignal>,
    /// Number of waiting barrier connections released (for `release_barrier`).
    pub released: usize,
}

/// Switch-resident deterministic virtual coordinator (`http://rewind`).
#[derive(Clone, Debug)]
pub(crate) struct ControlPlane {
    ip: Option<[u8; 4]>,
    nodes: Vec<ControlNodeMeta>,
    buggify_seed: u64,
    buggify_activate_ppm: u32,
    buggify_fire_ppm: u32,
    state: ControlPlaneState,
}

impl ControlPlane {
    pub(crate) fn new(
        ip: Option<[u8; 4]>,
        nodes: Vec<ControlNodeMeta>,
        buggify_enabled: bool,
        buggify_seed: u64,
    ) -> ControlPlane {
        ControlPlane {
            ip,
            nodes,
            buggify_seed,
            buggify_activate_ppm: DEFAULT_BUGGIFY_ACTIVATE_PPM,
            buggify_fire_ppm: DEFAULT_BUGGIFY_FIRE_PPM,
            state: ControlPlaneState {
                buggify_enabled,
                ..ControlPlaneState::default()
            },
        }
    }

    pub(crate) fn restore(
        ip: Option<[u8; 4]>,
        nodes: Vec<ControlNodeMeta>,
        buggify_seed: u64,
        state: ControlPlaneState,
    ) -> ControlPlane {
        ControlPlane {
            ip,
            nodes,
            buggify_seed,
            buggify_activate_ppm: DEFAULT_BUGGIFY_ACTIVATE_PPM,
            buggify_fire_ppm: DEFAULT_BUGGIFY_FIRE_PPM,
            state,
        }
    }

    pub(crate) fn set_buggify(&mut self, enabled: bool) {
        self.state.buggify_enabled = enabled;
    }

    pub(crate) fn buggify_enabled(&self) -> bool {
        self.state.buggify_enabled
    }

    pub(crate) fn buggify_sites(&self) -> &BTreeMap<String, bool> {
        &self.state.buggify_sites
    }

    pub(crate) fn probes(&self) -> &BTreeMap<String, u64> {
        &self.state.probes
    }

    pub(crate) fn snapshot(&self) -> ControlPlaneState {
        self.state.clone()
    }

    pub(crate) fn enabled(&self) -> bool {
        self.ip.is_some()
    }

    /// Drops any open connections owned by a crashed node so a rebooted guest
    /// starts with a clean slate.
    pub(crate) fn drop_node(&mut self, node: usize) {
        self.state.conns.retain(|k, _| k.node != node);
    }

    /// Handles an inbound Ethernet frame from `src_node` at `now_ns`.
    /// Returns `ControlOutput` containing any reply frames and emitted signals.
    pub(crate) fn on_frame(&mut self, now_ns: u64, src_node: usize, frame: &[u8]) -> ControlOutput {
        let mut out = ControlOutput::default();
        let Some(ctrl_ip) = self.ip else {
            return out;
        };
        if frame.len() < 14 {
            return out;
        }
        let dst_mac: [u8; 6] = [frame[0], frame[1], frame[2], frame[3], frame[4], frame[5]];
        let src_mac: [u8; 6] = [frame[6], frame[7], frame[8], frame[9], frame[10], frame[11]];
        let ethertype = u16::from_be_bytes([frame[12], frame[13]]);

        if ethertype == ETH_TYPE_ARP {
            if let Some(reply) = handle_arp(ctrl_ip, src_mac, &frame[14..]) {
                out.frames.push((now_ns, src_node, reply));
            }
            return out;
        }

        if ethertype != ETH_TYPE_IPV4 || dst_mac != CONTROL_MAC {
            return out;
        }

        let ip = &frame[14..];
        if ip.len() < 20 || (ip[0] >> 4) != 4 {
            return out;
        }
        let ihl = ((ip[0] & 0x0f) as usize) * 4;
        let total_len = u16::from_be_bytes([ip[2], ip[3]]) as usize;
        if ihl < 20 || total_len < ihl || total_len > ip.len() {
            return out;
        }
        let dst_ip = [ip[16], ip[17], ip[18], ip[19]];
        if dst_ip != ctrl_ip {
            return out;
        }
        let src_ip = [ip[12], ip[13], ip[14], ip[15]];
        let proto = ip[9];
        let payload = &ip[ihl..total_len];

        match proto {
            IP_PROTO_ICMP => {
                if let Some(reply) = self.handle_icmp(ctrl_ip, src_mac, src_ip, payload) {
                    out.frames.push((now_ns, src_node, reply));
                }
            }
            IP_PROTO_TCP => {
                self.handle_tcp(now_ns, src_node, ctrl_ip, src_mac, src_ip, payload, &mut out);
            }
            _ => {}
        }
        out
    }

    /// Returns deterministically sorted `(node_idx, barrier_name)` pairs for
    /// every guest connection currently parked on `http://rewind/barrier/<name>`.
    pub(crate) fn waiting_barriers(&self) -> Vec<(usize, String)> {
        let mut out: Vec<(usize, String)> = self
            .state
            .conns
            .iter()
            .filter_map(|(k, c)| match &c.state {
                ControlConnState::WaitingBarrier { name } => Some((k.node, name.clone())),
                _ => None,
            })
            .collect();
        out.sort();
        out
    }

    /// Releases all connections currently blocked on `/barrier/<name>` (optionally
    /// restricted to `target_node`), sending an HTTP/1.1 response with `status`
    /// and `body`.
    pub(crate) fn release_barrier(
        &mut self,
        now_ns: u64,
        name: &str,
        target_node: Option<usize>,
        status: u16,
        content_type: &str,
        body: &[u8],
    ) -> ControlOutput {
        let mut out = ControlOutput::default();
        let Some(ctrl_ip) = self.ip else {
            return out;
        };

        let matching_keys: Vec<ControlConnKey> = self
            .state
            .conns
            .iter()
            .filter_map(|(&k, c)| match &c.state {
                ControlConnState::WaitingBarrier { name: n }
                    if n == name && target_node.is_none_or(|t| t == k.node) =>
                {
                    Some(k)
                }
                _ => None,
            })
            .collect();

        for key in matching_keys {
            let resp = format_http_response(status, content_type, body);
            let mut frames_to_push = Vec::new();
            let base_ns;
            if let Some(conn) = self.state.conns.get_mut(&key) {
                base_ns = now_ns.max(conn.last_rx_ns);
                conn.tx_buf = resp;
                conn.tx_offset = 0;
                conn.state = ControlConnState::Sending { fin_sent: false };
                self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
                flush_tx_segments(
                    ctrl_ip,
                    key,
                    conn,
                    &mut self.state.next_ip_id,
                    &mut frames_to_push,
                );
                out.released += 1;
                for frame in frames_to_push {
                    out.frames.push((base_ns, key.node, frame));
                }
            }
        }
        out
    }

    fn handle_icmp(
        &mut self,
        ctrl_ip: [u8; 4],
        src_mac: [u8; 6],
        src_ip: [u8; 4],
        icmp: &[u8],
    ) -> Option<Vec<u8>> {
        if icmp.len() < 8 || icmp[0] != 8 || icmp[1] != 0 {
            return None;
        }
        let mut reply_icmp = icmp.to_vec();
        reply_icmp[0] = 0; // Echo Reply
        reply_icmp[2] = 0;
        reply_icmp[3] = 0;
        let csum = ones_complement_checksum(&reply_icmp);
        reply_icmp[2..4].copy_from_slice(&csum.to_be_bytes());
        self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
        Some(build_ipv4_frame(
            src_mac,
            CONTROL_MAC,
            ctrl_ip,
            src_ip,
            IP_PROTO_ICMP,
            self.state.next_ip_id,
            &reply_icmp,
        ))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "internal TCP segment handler taking parsed Ethernet/IPv4 fields and output collector"
    )]
    fn handle_tcp(
        &mut self,
        now_ns: u64,
        src_node: usize,
        ctrl_ip: [u8; 4],
        src_mac: [u8; 6],
        src_ip: [u8; 4],
        tcp: &[u8],
        out: &mut ControlOutput,
    ) {
        if tcp.len() < 20 {
            return;
        }
        let src_port = u16::from_be_bytes([tcp[0], tcp[1]]);
        let dst_port = u16::from_be_bytes([tcp[2], tcp[3]]);
        let seq = u32::from_be_bytes([tcp[4], tcp[5], tcp[6], tcp[7]]);
        let _ack = u32::from_be_bytes([tcp[8], tcp[9], tcp[10], tcp[11]]);
        let data_off = ((tcp[12] >> 4) as usize) * 4;
        let flags = tcp[13];
        let window = u32::from(u16::from_be_bytes([tcp[14], tcp[15]]));
        if data_off < 20 || data_off > tcp.len() {
            return;
        }
        let data = &tcp[data_off..];
        let key = ControlConnKey {
            node: src_node,
            src_ip,
            src_port,
        };

        if dst_port != CONTROL_PORT {
            // Connection refused on any port other than 80.
            if (flags & TCP_RST) == 0 {
                self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
                let rst = build_tcp_frame(
                    src_mac,
                    ctrl_ip,
                    src_ip,
                    dst_port,
                    src_port,
                    0,
                    seq.wrapping_add(data.len() as u32 + u32::from((flags & TCP_SYN) != 0)),
                    TCP_RST | TCP_ACK,
                    0,
                    &[],
                    &[],
                    self.state.next_ip_id,
                );
                out.frames.push((now_ns, src_node, rst));
            }
            return;
        }

        if (flags & TCP_RST) != 0 {
            self.state.conns.remove(&key);
            return;
        }

        // New SYN handshake.
        if (flags & TCP_SYN) != 0 && (flags & TCP_ACK) == 0 {
            self.state.next_conn_id += 1;
            let conn_id = self.state.next_conn_id;
            let isn = 0x5257_0000u32.wrapping_add((conn_id as u32).wrapping_mul(0x0101_0101));
            let guest_seq = seq.wrapping_add(1);
            let conn = ControlConn {
                conn_id,
                src_mac,
                guest_seq,
                ctrl_seq: isn.wrapping_add(1),
                guest_wnd: window.max(TCP_MSS as u32),
                last_rx_ns: now_ns,
                continue_sent: false,
                req_buf: Vec::new(),
                tx_buf: Vec::new(),
                tx_offset: 0,
                state: ControlConnState::Established,
            };
            self.state.conns.insert(key, conn);
            self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
            // Advertise MSS = 1460 (no WSCALE so Linux uses clean unscaled 16-bit windows).
            let mss_opt = [2u8, 4, (TCP_MSS >> 8) as u8, (TCP_MSS & 0xff) as u8];
            let syn_ack = build_tcp_frame(
                src_mac,
                ctrl_ip,
                src_ip,
                CONTROL_PORT,
                src_port,
                isn,
                guest_seq,
                TCP_SYN | TCP_ACK,
                65_535,
                &mss_opt,
                &[],
                self.state.next_ip_id,
            );
            out.frames.push((now_ns, src_node, syn_ack));
            return;
        }

        let Some(mut conn) = self.state.conns.remove(&key) else {
            return;
        };
        conn.src_mac = src_mac;
        conn.last_rx_ns = now_ns;
        if window > 0 {
            conn.guest_wnd = window;
        }

        let mut remove_conn = false;

        match conn.state.clone() {
            ControlConnState::Established => {
                if !data.is_empty() {
                    if seq == conn.guest_seq {
                        let room = MAX_REQ_BYTES.saturating_sub(conn.req_buf.len());
                        let take = data.len().min(room);
                        conn.req_buf.extend_from_slice(&data[..take]);
                        conn.guest_seq = conn.guest_seq.wrapping_add(data.len() as u32);
                    }
                    match try_parse_http_request(&conn.req_buf) {
                        ParseHttp::Incomplete { expect_continue } => {
                            if expect_continue && !conn.continue_sent {
                                conn.continue_sent = true;
                                let cont = b"HTTP/1.1 100 Continue\r\n\r\n";
                                self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
                                let f = build_tcp_frame(
                                    conn.src_mac,
                                    ctrl_ip,
                                    src_ip,
                                    CONTROL_PORT,
                                    src_port,
                                    conn.ctrl_seq,
                                    conn.guest_seq,
                                    TCP_PSH | TCP_ACK,
                                    65_535,
                                    &[],
                                    cont,
                                    self.state.next_ip_id,
                                );
                                conn.ctrl_seq = conn.ctrl_seq.wrapping_add(cont.len() as u32);
                                out.frames.push((now_ns, src_node, f));
                            } else {
                                self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
                                let ack_f = build_tcp_frame(
                                    conn.src_mac,
                                    ctrl_ip,
                                    src_ip,
                                    CONTROL_PORT,
                                    src_port,
                                    conn.ctrl_seq,
                                    conn.guest_seq,
                                    TCP_ACK,
                                    65_535,
                                    &[],
                                    &[],
                                    self.state.next_ip_id,
                                );
                                out.frames.push((now_ns, src_node, ack_f));
                            }
                        }
                        ParseHttp::Complete(req) => {
                            self.dispatch_http(now_ns, src_node, ctrl_ip, key, &mut conn, req, out);
                        }
                        ParseHttp::Invalid => {
                            conn.tx_buf = format_http_response(
                                400,
                                "application/json",
                                br#"{"error":"bad request"}"#,
                            );
                            conn.tx_offset = 0;
                            conn.state = ControlConnState::Sending { fin_sent: false };
                            let mut frames = Vec::new();
                            flush_tx_segments(
                                ctrl_ip,
                                key,
                                &mut conn,
                                &mut self.state.next_ip_id,
                                &mut frames,
                            );
                            for f in frames {
                                out.frames.push((now_ns, src_node, f));
                            }
                        }
                    }
                }
                if (flags & TCP_FIN) != 0 {
                    conn.guest_seq = conn.guest_seq.wrapping_add(1);
                    self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
                    let ack_f = build_tcp_frame(
                        conn.src_mac,
                        ctrl_ip,
                        src_ip,
                        CONTROL_PORT,
                        src_port,
                        conn.ctrl_seq,
                        conn.guest_seq,
                        TCP_ACK,
                        65_535,
                        &[],
                        &[],
                        self.state.next_ip_id,
                    );
                    out.frames.push((now_ns, src_node, ack_f));
                    if matches!(conn.state, ControlConnState::Established) {
                        remove_conn = true;
                    }
                }
            }
            ControlConnState::WaitingBarrier { .. } => {
                if (flags & TCP_FIN) != 0 {
                    // Half-close from guest (e.g. `printf ... | nc rewind 80`
                    // closing stdin via shutdown(SHUT_WR) while still reading
                    // the HTTP response): ACK the FIN and keep the barrier
                    // open until `release_barrier` sends the HTTP response.
                    if seq == conn.guest_seq {
                        conn.guest_seq = conn.guest_seq.wrapping_add(1);
                    }
                    self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
                    let ack = build_tcp_frame(
                        conn.src_mac,
                        ctrl_ip,
                        src_ip,
                        CONTROL_PORT,
                        src_port,
                        conn.ctrl_seq,
                        conn.guest_seq,
                        TCP_ACK,
                        65_535,
                        &[],
                        &[],
                        self.state.next_ip_id,
                    );
                    out.frames.push((now_ns, src_node, ack));
                }
            }
            ControlConnState::Sending { fin_sent } => {
                if !fin_sent && conn.tx_offset < conn.tx_buf.len() {
                    let mut frames = Vec::new();
                    flush_tx_segments(
                        ctrl_ip,
                        key,
                        &mut conn,
                        &mut self.state.next_ip_id,
                        &mut frames,
                    );
                    for f in frames {
                        out.frames.push((now_ns, src_node, f));
                    }
                }
                if (flags & TCP_FIN) != 0 {
                    conn.guest_seq = conn.guest_seq.wrapping_add(1);
                    self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
                    let ack_f = build_tcp_frame(
                        conn.src_mac,
                        ctrl_ip,
                        src_ip,
                        CONTROL_PORT,
                        src_port,
                        conn.ctrl_seq,
                        conn.guest_seq,
                        TCP_ACK,
                        65_535,
                        &[],
                        &[],
                        self.state.next_ip_id,
                    );
                    out.frames.push((now_ns, src_node, ack_f));
                    if matches!(conn.state, ControlConnState::Sending { fin_sent: true }) {
                        remove_conn = true;
                    }
                }
            }
        }

        if !remove_conn {
            self.state.conns.insert(key, conn);
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "internal HTTP route dispatcher on virtual control plane"
    )]
    fn dispatch_http(
        &mut self,
        now_ns: u64,
        src_node: usize,
        ctrl_ip: [u8; 4],
        key: ControlConnKey,
        conn: &mut ControlConn,
        req: HttpRequest,
        out: &mut ControlOutput,
    ) {
        let path_only = req.path.split('?').next().unwrap_or(&req.path);
        let payload_str = String::from_utf8_lossy(&req.body).into_owned();

        if path_only == "/meta" || path_only == "/meta/" {
            let body = self.format_meta_json(src_node, ctrl_ip);
            conn.tx_buf = format_http_response(200, "application/json", body.as_bytes());
            conn.tx_offset = 0;
            conn.state = ControlConnState::Sending { fin_sent: false };
            let mut frames = Vec::new();
            flush_tx_segments(ctrl_ip, key, conn, &mut self.state.next_ip_id, &mut frames);
            for f in frames {
                out.frames.push((now_ns, src_node, f));
            }
            return;
        }

        if let Some(raw_name) = path_only.strip_prefix("/signal/") {
            let name = percent_decode(raw_name.trim_end_matches('/'));
            if !name.is_empty() {
                self.state.next_signal_seq += 1;
                let seq = self.state.next_signal_seq;
                out.signals.push(ControlSignal {
                    seq,
                    ns: now_ns,
                    node: src_node,
                    name: name.clone(),
                    payload: payload_str,
                    barrier: false,
                    conn_id: None,
                });
                let body = format!(
                    r#"{{"ok":true,"seq":{seq},"name":"{}","ns":{now_ns}}}"#,
                    json_escape(&name)
                );
                conn.tx_buf = format_http_response(200, "application/json", body.as_bytes());
                conn.tx_offset = 0;
                conn.state = ControlConnState::Sending { fin_sent: false };
                let mut frames = Vec::new();
                flush_tx_segments(ctrl_ip, key, conn, &mut self.state.next_ip_id, &mut frames);
                for f in frames {
                    out.frames.push((now_ns, src_node, f));
                }
                return;
            }
        }

        if let Some(raw_name) = path_only.strip_prefix("/barrier/") {
            let name = percent_decode(raw_name.trim_end_matches('/'));
            if !name.is_empty() {
                self.state.next_signal_seq += 1;
                let seq = self.state.next_signal_seq;
                conn.state = ControlConnState::WaitingBarrier { name: name.clone() };
                out.signals.push(ControlSignal {
                    seq,
                    ns: now_ns,
                    node: src_node,
                    name,
                    payload: payload_str,
                    barrier: true,
                    conn_id: Some(conn.conn_id),
                });
                // Acknowledge the HTTP request bytes without sending a response yet,
                // so the guest blocks cleanly until `FaultKind::ControlRelease` arrives.
                self.state.next_ip_id = self.state.next_ip_id.wrapping_add(1);
                let ack_f = build_tcp_frame(
                    conn.src_mac,
                    ctrl_ip,
                    key.src_ip,
                    CONTROL_PORT,
                    key.src_port,
                    conn.ctrl_seq,
                    conn.guest_seq,
                    TCP_ACK,
                    65_535,
                    &[],
                    &[],
                    self.state.next_ip_id,
                );
                out.frames.push((now_ns, src_node, ack_f));
                return;
            }
        }

        if let Some(raw_name) = path_only.strip_prefix("/buggify/") {
            let name = percent_decode(raw_name.trim_end_matches('/'));
            if !name.is_empty() {
                let fire_ppm = parse_query_probability_ppm(&req.path)
                    .unwrap_or(self.buggify_fire_ppm);
                let (activated, fire) = self.eval_buggify(&name, fire_ppm);
                let body = format!(
                    r#"{{"fire":{fire},"activated":{activated},"enabled":{},"name":"{}"}}"#,
                    self.state.buggify_enabled,
                    json_escape(&name),
                );
                conn.tx_buf = format_http_response(200, "application/json", body.as_bytes());
                conn.tx_offset = 0;
                conn.state = ControlConnState::Sending { fin_sent: false };
                let mut frames = Vec::new();
                flush_tx_segments(ctrl_ip, key, conn, &mut self.state.next_ip_id, &mut frames);
                for f in frames {
                    out.frames.push((now_ns, src_node, f));
                }
                return;
            }
        }

        if let Some(raw_name) = path_only.strip_prefix("/probe/") {
            let name = percent_decode(raw_name.trim_end_matches('/'));
            if !name.is_empty() {
                let entry = self.state.probes.entry(name.clone()).or_insert(0);
                *entry = entry.saturating_add(1);
                let count = *entry;
                let body = format!(
                    r#"{{"ok":true,"name":"{}","count":{count}}}"#,
                    json_escape(&name)
                );
                conn.tx_buf = format_http_response(200, "application/json", body.as_bytes());
                conn.tx_offset = 0;
                conn.state = ControlConnState::Sending { fin_sent: false };
                let mut frames = Vec::new();
                flush_tx_segments(ctrl_ip, key, conn, &mut self.state.next_ip_id, &mut frames);
                for f in frames {
                    out.frames.push((now_ns, src_node, f));
                }
                return;
            }
        }

        conn.tx_buf = format_http_response(404, "application/json", br#"{"error":"not found"}"#);
        conn.tx_offset = 0;
        conn.state = ControlConnState::Sending { fin_sent: false };
        let mut frames = Vec::new();
        flush_tx_segments(ctrl_ip, key, conn, &mut self.state.next_ip_id, &mut frames);
        for f in frames {
            out.frames.push((now_ns, src_node, f));
        }
    }

    fn eval_buggify(&mut self, name: &str, fire_ppm: u32) -> (bool, bool) {
        if !self.state.buggify_enabled {
            return (false, false);
        }
        let seed = self.buggify_seed;
        let activate_ppm = self.buggify_activate_ppm;
        let activated = *self
            .state
            .buggify_sites
            .entry(name.to_string())
            .or_insert_with(|| {
                let draw = (buggify_hash(seed, name, 0) % BUGGIFY_PPM_SCALE) as u32;
                draw < activate_ppm
            });
        if !activated {
            return (false, false);
        }
        let calls = self.state.buggify_calls.entry(name.to_string()).or_insert(0);
        *calls = calls.wrapping_add(1);
        let draw = (buggify_hash(seed, name, *calls) % BUGGIFY_PPM_SCALE) as u32;
        (true, draw < fire_ppm)
    }

    fn format_meta_json(&self, src_node: usize, ctrl_ip: [u8; 4]) -> String {
        let ctrl_ip_str = format!("{}.{}.{}.{}", ctrl_ip[0], ctrl_ip[1], ctrl_ip[2], ctrl_ip[3]);
        let node = self.nodes.get(src_node);
        let node_name = node.map_or("", |m| m.name.as_str());
        let node_ip = node.map_or("", |m| m.ip.as_str());
        let mut peers = String::from("[");
        for (i, n) in self.nodes.iter().enumerate() {
            if i > 0 {
                peers.push(',');
            }
            peers.push_str(&format_node_meta_obj(n));
        }
        peers.push(']');
        let mut out = format!(
            r#"{{"node":"{}","ip":"{}","control_ip":"{}","buggify":{}"#,
            json_escape(node_name),
            json_escape(node_ip),
            ctrl_ip_str,
            self.state.buggify_enabled,
        );
        if let Some(m) = node {
            if let Some(z) = &m.zone {
                out.push_str(&format!(r#","zone":"{}""#, json_escape(z)));
            }
            if let Some(r) = &m.rack {
                out.push_str(&format!(r#","rack":"{}""#, json_escape(r)));
            }
            if let Some(d) = &m.dc {
                out.push_str(&format!(r#","dc":"{}""#, json_escape(d)));
            }
        }
        out.push_str(&format!(r#","nodes":{peers}}}"#));
        out
    }
}

fn format_node_meta_obj(n: &ControlNodeMeta) -> String {
    let mut s = format!(
        r#"{{"name":"{}","ip":"{}""#,
        json_escape(&n.name),
        json_escape(&n.ip)
    );
    if let Some(z) = &n.zone {
        s.push_str(&format!(r#","zone":"{}""#, json_escape(z)));
    }
    if let Some(r) = &n.rack {
        s.push_str(&format!(r#","rack":"{}""#, json_escape(r)));
    }
    if let Some(d) = &n.dc {
        s.push_str(&format!(r#","dc":"{}""#, json_escape(d)));
    }
    s.push('}');
    s
}

fn parse_query_probability_ppm(path: &str) -> Option<u32> {
    let query = path.split_once('?')?.1;
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=')
            && k == "p"
            && let Ok(p) = v.parse::<f64>()
            && p.is_finite()
        {
            let clamped = p.clamp(0.0, 1.0);
            return Some((clamped * BUGGIFY_PPM_SCALE as f64).round() as u32);
        }
    }
    None
}

fn buggify_hash(seed: u64, name: &str, call: u64) -> u64 {
    // Deterministic FNV-1a + SplitMix64 mixing so each (seed, site, call) has
    // independent, well-distributed bits.
    let mut h = seed ^ 0xcbf2_9ce4_8422_2325u64;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= call.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

struct HttpRequest {
    path: String,
    body: Vec<u8>,
}

enum ParseHttp {
    Incomplete { expect_continue: bool },
    Complete(HttpRequest),
    Invalid,
}

fn try_parse_http_request(buf: &[u8]) -> ParseHttp {
    let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        return ParseHttp::Incomplete {
            expect_continue: false,
        };
    };
    let head = match std::str::from_utf8(&buf[..hdr_end]) {
        Ok(s) => s,
        Err(_) => return ParseHttp::Invalid,
    };
    let mut lines = head.split("\r\n");
    let Some(req_line) = lines.next() else {
        return ParseHttp::Invalid;
    };
    let mut parts = req_line.split_whitespace();
    let Some(_method) = parts.next() else {
        return ParseHttp::Invalid;
    };
    let Some(target) = parts.next() else {
        return ParseHttp::Invalid;
    };

    let mut content_len: usize = 0;
    let mut expect_continue = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim();
            let val = v.trim();
            if key.eq_ignore_ascii_case("content-length") {
                content_len = match val.parse::<usize>() {
                    Ok(n) if n <= MAX_REQ_BYTES => n,
                    _ => return ParseHttp::Invalid,
                };
            } else if key.eq_ignore_ascii_case("expect")
                && val.to_ascii_lowercase().contains("100-continue")
            {
                expect_continue = true;
            }
        }
    }

    let body_start = hdr_end + 4;
    if buf.len() < body_start + content_len {
        return ParseHttp::Incomplete { expect_continue };
    }
    ParseHttp::Complete(HttpRequest {
        path: target.to_string(),
        body: buf[body_start..body_start + content_len].to_vec(),
    })
}

fn format_http_response(status: u16, content_type: &str, body: &[u8]) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut resp = Vec::with_capacity(header.len() + body.len());
    resp.extend_from_slice(header.as_bytes());
    resp.extend_from_slice(body);
    resp
}

fn flush_tx_segments(
    ctrl_ip: [u8; 4],
    key: ControlConnKey,
    conn: &mut ControlConn,
    ip_id: &mut u16,
    out: &mut Vec<Vec<u8>>,
) {
    // Cap burst to guest window (and at most 16 MSS segments per ACK so we never
    // overflow the guest's 64-slot paravirtual RX ring).
    let max_burst = (conn.guest_wnd as usize).clamp(TCP_MSS, 16 * TCP_MSS);
    let mut sent = 0usize;
    while conn.tx_offset < conn.tx_buf.len() && sent < max_burst {
        let rem = conn.tx_buf.len() - conn.tx_offset;
        let chunk_len = rem.min(TCP_MSS).min(max_burst - sent);
        let chunk = &conn.tx_buf[conn.tx_offset..conn.tx_offset + chunk_len];
        let is_last = conn.tx_offset + chunk_len == conn.tx_buf.len();
        let mut flags = TCP_PSH | TCP_ACK;
        if is_last {
            flags |= TCP_FIN;
        }
        *ip_id = ip_id.wrapping_add(1);
        let frame = build_tcp_frame(
            conn.src_mac,
            ctrl_ip,
            key.src_ip,
            CONTROL_PORT,
            key.src_port,
            conn.ctrl_seq,
            conn.guest_seq,
            flags,
            65_535,
            &[],
            chunk,
            *ip_id,
        );
        conn.ctrl_seq = conn
            .ctrl_seq
            .wrapping_add(chunk_len as u32 + u32::from(is_last));
        conn.tx_offset += chunk_len;
        sent += chunk_len;
        if is_last {
            conn.state = ControlConnState::Sending { fin_sent: true };
        }
        out.push(frame);
    }
    if conn.tx_buf.is_empty() && matches!(conn.state, ControlConnState::Sending { fin_sent: false }) {
        *ip_id = ip_id.wrapping_add(1);
        let frame = build_tcp_frame(
            conn.src_mac,
            ctrl_ip,
            key.src_ip,
            CONTROL_PORT,
            key.src_port,
            conn.ctrl_seq,
            conn.guest_seq,
            TCP_FIN | TCP_ACK,
            65_535,
            &[],
            &[],
            *ip_id,
        );
        conn.ctrl_seq = conn.ctrl_seq.wrapping_add(1);
        conn.state = ControlConnState::Sending { fin_sent: true };
        out.push(frame);
    }
}

fn handle_arp(ctrl_ip: [u8; 4], src_mac: [u8; 6], arp: &[u8]) -> Option<Vec<u8>> {
    if arp.len() < 28 {
        return None;
    }
    let htype = u16::from_be_bytes([arp[0], arp[1]]);
    let ptype = u16::from_be_bytes([arp[2], arp[3]]);
    let hlen = arp[4];
    let plen = arp[5];
    let oper = u16::from_be_bytes([arp[6], arp[7]]);
    if htype != 1 || ptype != ETH_TYPE_IPV4 || hlen != 6 || plen != 4 || oper != 1 {
        return None;
    }
    let tpa = [arp[24], arp[25], arp[26], arp[27]];
    if tpa != ctrl_ip {
        return None;
    }
    let spa = [arp[14], arp[15], arp[16], arp[17]];
    let mut frame = Vec::with_capacity(42);
    frame.extend_from_slice(&src_mac);
    frame.extend_from_slice(&CONTROL_MAC);
    frame.extend_from_slice(&ETH_TYPE_ARP.to_be_bytes());
    frame.extend_from_slice(&1u16.to_be_bytes()); // Ethernet
    frame.extend_from_slice(&ETH_TYPE_IPV4.to_be_bytes()); // IPv4
    frame.push(6);
    frame.push(4);
    frame.extend_from_slice(&2u16.to_be_bytes()); // ARP Reply
    frame.extend_from_slice(&CONTROL_MAC);
    frame.extend_from_slice(&ctrl_ip);
    frame.extend_from_slice(&src_mac);
    frame.extend_from_slice(&spa);
    Some(frame)
}

fn build_ipv4_frame(
    dst_mac: [u8; 6],
    src_mac: [u8; 6],
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    proto: u8,
    ip_id: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = (20 + payload.len()) as u16;
    let mut hdr = [0u8; 20];
    hdr[0] = 0x45; // IPv4, IHL=5
    hdr[2..4].copy_from_slice(&total_len.to_be_bytes());
    hdr[4..6].copy_from_slice(&ip_id.to_be_bytes());
    hdr[6..8].copy_from_slice(&0x4000u16.to_be_bytes()); // Don't Fragment
    hdr[8] = 64; // TTL
    hdr[9] = proto;
    hdr[12..16].copy_from_slice(&src_ip);
    hdr[16..20].copy_from_slice(&dst_ip);
    let csum = ones_complement_checksum(&hdr);
    hdr[10..12].copy_from_slice(&csum.to_be_bytes());

    let mut frame = Vec::with_capacity(14 + 20 + payload.len());
    frame.extend_from_slice(&dst_mac);
    frame.extend_from_slice(&src_mac);
    frame.extend_from_slice(&ETH_TYPE_IPV4.to_be_bytes());
    frame.extend_from_slice(&hdr);
    frame.extend_from_slice(payload);
    frame
}

#[allow(
    clippy::too_many_arguments,
    reason = "packet builder taking standard Ethernet/IPv4/TCP header fields"
)]
fn build_tcp_frame(
    dst_mac: [u8; 6],
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    src_port: u16,
    dst_port: u16,
    seq: u32,
    ack: u32,
    flags: u8,
    window: u16,
    options: &[u8],
    payload: &[u8],
    ip_id: u16,
) -> Vec<u8> {
    let opt_padded = (options.len() + 3) & !3;
    let hdr_len = 20 + opt_padded;
    let mut tcp = vec![0u8; hdr_len + payload.len()];
    tcp[0..2].copy_from_slice(&src_port.to_be_bytes());
    tcp[2..4].copy_from_slice(&dst_port.to_be_bytes());
    tcp[4..8].copy_from_slice(&seq.to_be_bytes());
    tcp[8..12].copy_from_slice(&ack.to_be_bytes());
    tcp[12] = ((hdr_len / 4) as u8) << 4;
    tcp[13] = flags;
    tcp[14..16].copy_from_slice(&window.to_be_bytes());
    tcp[20..20 + options.len()].copy_from_slice(options);
    tcp[hdr_len..].copy_from_slice(payload);

    let csum = tcp_checksum(src_ip, dst_ip, &tcp);
    tcp[16..18].copy_from_slice(&csum.to_be_bytes());
    build_ipv4_frame(dst_mac, CONTROL_MAC, src_ip, dst_ip, IP_PROTO_TCP, ip_id, &tcp)
}

fn ones_complement_checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let mut i = 0;
    while i + 1 < data.len() {
        sum += u32::from(u16::from_be_bytes([data[i], data[i + 1]]));
        i += 2;
    }
    if i < data.len() {
        sum += u32::from(data[i]) << 8;
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn tcp_checksum(src_ip: [u8; 4], dst_ip: [u8; 4], tcp: &[u8]) -> u16 {
    let mut pseudo = [0u8; 12];
    pseudo[0..4].copy_from_slice(&src_ip);
    pseudo[4..8].copy_from_slice(&dst_ip);
    pseudo[9] = IP_PROTO_TCP;
    pseudo[10..12].copy_from_slice(&(tcp.len() as u16).to_be_bytes());

    let mut sum: u32 = 0;
    for chunk in pseudo.as_chunks::<2>().0 {
        sum += u32::from(u16::from_be_bytes(*chunk));
    }
    let mut i = 0;
    while i + 1 < tcp.len() {
        sum += u32::from(u16::from_be_bytes([tcp[i], tcp[i + 1]]));
        i += 2;
    }
    if i < tcp.len() {
        sum += u32::from(tcp[i]) << 8;
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(
        clippy::too_many_arguments,
        reason = "test helper building a raw guest TCP segment"
    )]
    fn guest_tcp(
        src_mac: [u8; 6],
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        seq: u32,
        ack: u32,
        flags: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut tcp = vec![0u8; 20 + payload.len()];
        tcp[0..2].copy_from_slice(&src_port.to_be_bytes());
        tcp[2..4].copy_from_slice(&CONTROL_PORT.to_be_bytes());
        tcp[4..8].copy_from_slice(&seq.to_be_bytes());
        tcp[8..12].copy_from_slice(&ack.to_be_bytes());
        tcp[12] = 5 << 4;
        tcp[13] = flags;
        tcp[14..16].copy_from_slice(&65_535u16.to_be_bytes());
        tcp[20..].copy_from_slice(payload);
        let csum = tcp_checksum(src_ip, dst_ip, &tcp);
        tcp[16..18].copy_from_slice(&csum.to_be_bytes());
        build_ipv4_frame(CONTROL_MAC, src_mac, src_ip, dst_ip, IP_PROTO_TCP, 1, &tcp)
    }

    fn tcp_payload_from_frame(frame: &[u8]) -> (&[u8], u8, u32) {
        let ip = &frame[14..];
        let ihl = ((ip[0] & 0x0f) as usize) * 4;
        let tcp = &ip[ihl..];
        let off = ((tcp[12] >> 4) as usize) * 4;
        let flags = tcp[13];
        let seq = u32::from_be_bytes([tcp[4], tcp[5], tcp[6], tcp[7]]);
        (&tcp[off..], flags, seq)
    }

    #[test]
    fn arp_signal_barrier_release_and_snapshot_roundtrip() {
        let ctrl_ip = [10, 0, 0, 254];
        let guest_ip = [10, 0, 0, 1];
        let guest_mac = [0x52, 0x54, 0x00, 0x00, 0x00, 0x01];
        let nodes = vec![ControlNodeMeta {
            name: "n1".into(),
            ip: "10.0.0.1".into(),
            zone: Some("az-1".into()),
            rack: Some("r1".into()),
            dc: Some("us-east".into()),
        }];
        let mut cp = ControlPlane::new(Some(ctrl_ip), nodes.clone(), true, 42);

        // 1. ARP resolution for 10.0.0.254
        let mut arp_req = vec![0u8; 42];
        arp_req[0..6].copy_from_slice(&[0xff; 6]);
        arp_req[6..12].copy_from_slice(&guest_mac);
        arp_req[12..14].copy_from_slice(&ETH_TYPE_ARP.to_be_bytes());
        arp_req[14..16].copy_from_slice(&1u16.to_be_bytes());
        arp_req[16..18].copy_from_slice(&ETH_TYPE_IPV4.to_be_bytes());
        arp_req[18] = 6;
        arp_req[19] = 4;
        arp_req[20..22].copy_from_slice(&1u16.to_be_bytes());
        arp_req[22..28].copy_from_slice(&guest_mac);
        arp_req[28..32].copy_from_slice(&guest_ip);
        arp_req[38..42].copy_from_slice(&ctrl_ip);

        let arp_out = cp.on_frame(1_000, 0, &arp_req);
        assert_eq!(arp_out.frames.len(), 1);
        assert_eq!(&arp_out.frames[0].2[6..12], &CONTROL_MAC);

        // 2. POST /signal/leader-elected
        let syn = guest_tcp(guest_mac, guest_ip, ctrl_ip, 40000, 100, 0, TCP_SYN, &[]);
        let syn_out = cp.on_frame(2_000, 0, &syn);
        assert_eq!(syn_out.frames.len(), 1);
        let (_, syn_flags, ctrl_isn) = tcp_payload_from_frame(&syn_out.frames[0].2);
        assert_eq!(syn_flags, TCP_SYN | TCP_ACK);

        let req = b"POST /signal/leader-elected HTTP/1.1\r\nHost: rewind\r\nContent-Length: 10\r\n\r\n{\"term\":4}";
        let psh = guest_tcp(
            guest_mac,
            guest_ip,
            ctrl_ip,
            40000,
            101,
            ctrl_isn.wrapping_add(1),
            TCP_PSH | TCP_ACK,
            req,
        );
        let sig_out = cp.on_frame(3_000, 0, &psh);
        assert_eq!(sig_out.signals.len(), 1);
        assert_eq!(sig_out.signals[0].name, "leader-elected");
        assert_eq!(sig_out.signals[0].payload, "{\"term\":4}");
        assert!(!sig_out.signals[0].barrier);
        let (http_resp, resp_flags, _) = tcp_payload_from_frame(&sig_out.frames[0].2);
        let resp_text = String::from_utf8_lossy(http_resp);
        assert!(resp_text.starts_with("HTTP/1.1 200 OK"), "{resp_text}");
        assert_ne!(resp_flags & TCP_FIN, 0);

        // 3. GET /barrier/ready -> snapshot -> release two different payloads!
        let syn2 = guest_tcp(guest_mac, guest_ip, ctrl_ip, 40001, 500, 0, TCP_SYN, &[]);
        let syn2_out = cp.on_frame(4_000, 0, &syn2);
        let (_, _, ctrl_isn2) = tcp_payload_from_frame(&syn2_out.frames[0].2);
        let req2 = b"GET /barrier/ready HTTP/1.1\r\nHost: rewind\r\n\r\n";
        let psh2 = guest_tcp(
            guest_mac,
            guest_ip,
            ctrl_ip,
            40001,
            501,
            ctrl_isn2.wrapping_add(1),
            TCP_PSH | TCP_ACK,
            req2,
        );
        let bar_out = cp.on_frame(5_000, 0, &psh2);
        assert_eq!(bar_out.signals.len(), 1);
        assert!(bar_out.signals[0].barrier);
        assert_eq!(bar_out.signals[0].name, "ready");
        // Only TCP ACK so far — HTTP response is held open!
        let (ack_payload, ack_flags, _) = tcp_payload_from_frame(&bar_out.frames[0].2);
        assert!(ack_payload.is_empty());
        assert_eq!(ack_flags, TCP_ACK);

        // Client half-closes write side (`shutdown(SHUT_WR)` like `printf | nc`):
        // control plane ACKs the FIN without closing the waiting barrier!
        let fin2 = guest_tcp(
            guest_mac,
            guest_ip,
            ctrl_ip,
            40001,
            501 + req2.len() as u32,
            ctrl_isn2.wrapping_add(1),
            TCP_FIN | TCP_ACK,
            &[],
        );
        let fin_out = cp.on_frame(5_500, 0, &fin2);
        let (_, fin_ack_flags, _) = tcp_payload_from_frame(&fin_out.frames[0].2);
        assert_eq!(fin_ack_flags, TCP_ACK);

        // Snapshot while waiting at barrier, then restore & release two branches!
        let snap = cp.snapshot();
        let rel_a = cp.release_barrier(
            6_000,
            "ready",
            None,
            200,
            "text/plain; charset=utf-8",
            b"branch-A",
        );
        assert_eq!(rel_a.released, 1);
        let (body_a, _, _) = tcp_payload_from_frame(&rel_a.frames[0].2);
        assert!(String::from_utf8_lossy(body_a).ends_with("\r\n\r\nbranch-A"));

        let mut cp_b = ControlPlane::restore(Some(ctrl_ip), nodes, 42, snap);
        let rel_b = cp_b.release_barrier(
            6_000,
            "ready",
            Some(0),
            200,
            "text/plain; charset=utf-8",
            b"branch-B",
        );
        assert_eq!(rel_b.released, 1);
        let (body_b, _, _) = tcp_payload_from_frame(&rel_b.frames[0].2);
        assert!(String::from_utf8_lossy(body_b).ends_with("\r\n\r\nbranch-B"));
    }

    fn http_roundtrip(
        cp: &mut ControlPlane,
        ctrl_ip: [u8; 4],
        guest_mac: [u8; 6],
        guest_ip: [u8; 4],
        port: u16,
        now_ns: u64,
        req: &[u8],
    ) -> String {
        let syn = guest_tcp(guest_mac, guest_ip, ctrl_ip, port, 100, 0, TCP_SYN, &[]);
        let syn_out = cp.on_frame(now_ns, 0, &syn);
        let (_, _, isn) = tcp_payload_from_frame(&syn_out.frames[0].2);
        let psh = guest_tcp(
            guest_mac,
            guest_ip,
            ctrl_ip,
            port,
            101,
            isn.wrapping_add(1),
            TCP_PSH | TCP_ACK,
            req,
        );
        let out = cp.on_frame(now_ns + 1_000, 0, &psh);
        let (payload, _, _) = tcp_payload_from_frame(&out.frames[0].2);
        let text = String::from_utf8_lossy(payload);
        match text.split_once("\r\n\r\n") {
            Some((_, b)) => b.to_string(),
            None => text.into_owned(),
        }
    }

    #[test]
    fn meta_locality_buggify_two_stage_and_probes_roundtrip() {
        let ctrl_ip = [10, 0, 0, 254];
        let guest_ip = [10, 0, 0, 1];
        let guest_mac = [0x52, 0x54, 0x00, 0x00, 0x00, 0x01];
        let nodes = vec![ControlNodeMeta {
            name: "n1".into(),
            ip: "10.0.0.1".into(),
            zone: Some("az-a".into()),
            rack: Some("rack-1".into()),
            dc: Some("dc-1".into()),
        }];
        let mut cp = ControlPlane::new(Some(ctrl_ip), nodes.clone(), true, 12345);

        let meta = http_roundtrip(
            &mut cp,
            ctrl_ip,
            guest_mac,
            guest_ip,
            41000,
            1_000,
            b"GET /meta HTTP/1.1\r\nHost: rewind\r\n\r\n",
        );
        assert!(meta.contains(r#""zone":"az-a""#), "{meta}");
        assert!(meta.contains(r#""rack":"rack-1""#), "{meta}");
        assert!(meta.contains(r#""dc":"dc-1""#), "{meta}");
        assert!(meta.contains(r#""buggify":true"#), "{meta}");

        // Hit 20 distinct buggify sites with p=1.0: some activate (~25%), some do not,
        // and re-querying any activated site always stays activated.
        let mut port = 41001;
        let mut activated_site = None;
        let mut inactive_site = None;
        for i in 0..32u32 {
            let req = format!("GET /buggify/site-{i}?p=1.0 HTTP/1.1\r\nHost: rewind\r\n\r\n");
            let body = http_roundtrip(
                &mut cp,
                ctrl_ip,
                guest_mac,
                guest_ip,
                port,
                10_000 + u64::from(i) * 5_000,
                req.as_bytes(),
            );
            port += 1;
            if body.contains(r#""activated":true"#) {
                assert!(body.contains(r#""fire":true"#), "{body}");
                activated_site.get_or_insert(format!("site-{i}"));
            } else {
                assert!(body.contains(r#""fire":false"#), "{body}");
                inactive_site.get_or_insert(format!("site-{i}"));
            }
        }
        let act = activated_site.expect("at least one site activates");
        let inact = inactive_site.expect("at least one site stays inactive");
        assert_eq!(cp.buggify_sites().get(&act), Some(&true));
        assert_eq!(cp.buggify_sites().get(&inact), Some(&false));

        // Disabling buggify prevents even activated sites from firing.
        cp.set_buggify(false);
        let req = format!("GET /buggify/{act}?p=1.0 HTTP/1.1\r\nHost: rewind\r\n\r\n");
        let body = http_roundtrip(
            &mut cp,
            ctrl_ip,
            guest_mac,
            guest_ip,
            port,
            500_000,
            req.as_bytes(),
        );
        port += 1;
        assert!(body.contains(r#""fire":false"#), "{body}");
        assert!(body.contains(r#""enabled":false"#), "{body}");

        // Probes increment deterministically and survive snapshot/restore.
        let p1 = http_roundtrip(
            &mut cp,
            ctrl_ip,
            guest_mac,
            guest_ip,
            port,
            600_000,
            b"POST /probe/recovery-complete HTTP/1.1\r\nHost: rewind\r\nContent-Length: 0\r\n\r\n",
        );
        port += 1;
        assert!(p1.contains(r#""count":1"#), "{p1}");
        let snap = cp.snapshot();
        let mut cp2 = ControlPlane::restore(Some(ctrl_ip), nodes, 12345, snap);
        let p2 = http_roundtrip(
            &mut cp2,
            ctrl_ip,
            guest_mac,
            guest_ip,
            port,
            700_000,
            b"GET /probe/recovery-complete HTTP/1.1\r\nHost: rewind\r\n\r\n",
        );
        assert!(p2.contains(r#""count":2"#), "{p2}");
        assert_eq!(cp2.probes().get("recovery-complete"), Some(&2));
    }
}
