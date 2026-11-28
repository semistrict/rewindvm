//! Binary `network.bin` log types and streaming reader/writer for recorded
//! multi-node clusters.
//!
//! Kept in `rewind-trace` (with `#![forbid(unsafe_code)]`) so both the VMM
//! switch (`rewind-vmm`) and the desktop/web viewer (`rewind-app`) share one
//! exact wire format.

use std::io::{self, BufWriter, Read, Write};

use serde::{Deserialize, Serialize};

/// Magic header for binary `network.bin` files, matching `rwtrace\0` / `rwckf01\0`.
pub const NET_LOG_MAGIC: &[u8; 8] = b"rwnet01\0";
/// Binary format version of `network.bin`.
pub const NET_LOG_VERSION: u32 = 1;
const NET_LOG_HEADER_LEN: usize = NET_LOG_MAGIC.len() + 4;
const NET_RECORD_MAX: usize = 64 * 1024;

const TAG_TX: u8 = 1;
const TAG_DELIVER: u8 = 2;
const TAG_DROP: u8 = 3;
const TAG_DUPLICATE: u8 = 4;
const TAG_PARTITION: u8 = 5;
const TAG_HEAL: u8 = 6;
const TAG_NODE_CRASH: u8 = 7;
const TAG_NODE_RESTART: u8 = 8;
const TAG_LINK_UPDATED: u8 = 9;
const TAG_CLOG: u8 = 10;
const TAG_UNCLOG: u8 = 11;
const TAG_NODE_PAUSE: u8 = 12;
const TAG_NODE_RESUME: u8 = 13;
const TAG_CLOCK_STEP: u8 = 14;
const TAG_CLOCK_DRIFT: u8 = 15;
const TAG_NEMESIS_TOGGLED: u8 = 16;
const TAG_BUGGIFY_TOGGLED: u8 = 17;

/// Direction for node-level network clogging (`FaultKind::ClogNode`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClogMode {
    /// Clog both outbound (`send`) and inbound (`recv`) packets for the node.
    #[default]
    All,
    /// Clog only outbound packets transmitted by the node.
    Send,
    /// Clog only inbound packets addressed to the node.
    Recv,
}

/// Why a frame was dropped by the virtual switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropReason {
    Partitioned,
    RandomDrop,
    NodeCrashed,
    RxOverflow,
}

/// A deterministic cluster network or fault event, recorded to `network.bin`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NetEvent {
    Tx {
        ns: u64,
        src: usize,
        dst_mac: String,
        len: usize,
        frame_hash: u64,
        summary: String,
    },
    Deliver {
        ns: u64,
        src: usize,
        dst: usize,
        len: usize,
        latency_ns: u64,
    },
    Drop {
        ns: u64,
        src: usize,
        dst: usize,
        len: usize,
        reason: DropReason,
    },
    Duplicate {
        ns: u64,
        src: usize,
        dst: usize,
        len: usize,
        extra_delay_ns: u64,
    },
    Partition {
        ns: u64,
        blocked_pairs: usize,
        asymmetric: bool,
    },
    Heal {
        ns: u64,
    },
    NodeCrash {
        ns: u64,
        node: usize,
    },
    NodeRestart {
        ns: u64,
        node: usize,
        boot_count: u32,
    },
    LinkUpdated {
        ns: u64,
        from: usize,
        to: usize,
        latency_ns: u64,
        jitter_ns: u64,
        drop_ppm: u32,
        duplicate_ppm: u32,
    },
    Clog {
        ns: u64,
        from: Option<usize>,
        to: Option<usize>,
        until_ns: u64,
        mode: ClogMode,
    },
    Unclog {
        ns: u64,
        from: Option<usize>,
        to: Option<usize>,
    },
    NodePause {
        ns: u64,
        node: usize,
    },
    NodeResume {
        ns: u64,
        node: usize,
    },
    ClockStep {
        ns: u64,
        node: usize,
        delta_ns: u64,
    },
    ClockDrift {
        ns: u64,
        node: usize,
        drift_ppm: i32,
    },
    NemesisToggled {
        ns: u64,
        enabled: bool,
    },
    BuggifyToggled {
        ns: u64,
        enabled: bool,
    },
}

impl NetEvent {
    /// Cluster virtual timestamp (in nanoseconds) of this event.
    pub fn ns(&self) -> u64 {
        match *self {
            NetEvent::Tx { ns, .. }
            | NetEvent::Deliver { ns, .. }
            | NetEvent::Drop { ns, .. }
            | NetEvent::Duplicate { ns, .. }
            | NetEvent::Partition { ns, .. }
            | NetEvent::Heal { ns }
            | NetEvent::NodeCrash { ns, .. }
            | NetEvent::NodeRestart { ns, .. }
            | NetEvent::LinkUpdated { ns, .. }
            | NetEvent::Clog { ns, .. }
            | NetEvent::Unclog { ns, .. }
            | NetEvent::NodePause { ns, .. }
            | NetEvent::NodeResume { ns, .. }
            | NetEvent::ClockStep { ns, .. }
            | NetEvent::ClockDrift { ns, .. }
            | NetEvent::NemesisToggled { ns, .. }
            | NetEvent::BuggifyToggled { ns, .. } => ns,
        }
    }

    /// Whether this event is a fault/control transition (as opposed to a normal
    /// Ethernet frame TX/Deliver/Drop/Duplicate).
    pub fn is_fault(&self) -> bool {
        !matches!(
            self,
            NetEvent::Tx { .. }
                | NetEvent::Deliver { .. }
                | NetEvent::Drop { .. }
                | NetEvent::Duplicate { .. }
        )
    }
}

/// Formats a 6-byte MAC address as lowercase colon-separated hex.
pub fn format_mac(mac: [u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
    )
}

/// Parses a colon-separated hex MAC address into 6 bytes.
pub fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let mut out = [0u8; 6];
    let mut parts = s.split(':');
    for slot in &mut out {
        let hex = parts.next()?;
        *slot = u8::from_str_radix(hex, 16).ok()?;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(out)
}

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if v == 0 {
            break;
        }
    }
}

fn get_varint(slice: &mut &[u8]) -> io::Result<u64> {
    let mut out = 0u64;
    let mut shift = 0u32;
    loop {
        let Some((&b, rest)) = slice.split_first() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated varint in network.bin",
            ));
        };
        *slice = rest;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow in network.bin",
            ));
        }
        out |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(out);
        }
        shift += 7;
    }
}

fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn get_bytes<'a>(slice: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if slice.len() < n {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated field in network.bin",
        ));
    }
    let (head, tail) = slice.split_at(n);
    *slice = tail;
    Ok(head)
}

fn get_u8(slice: &mut &[u8]) -> io::Result<u8> {
    Ok(get_bytes(slice, 1)?[0])
}

fn get_string(slice: &mut &[u8]) -> io::Result<String> {
    let len = get_varint(slice)? as usize;
    let bytes = get_bytes(slice, len)?;
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn encode_event_body(ev: &NetEvent, prev_ns: &mut u64, out: &mut Vec<u8>) {
    out.clear();
    let ns = ev.ns();
    let delta = (ns as i64).wrapping_sub(*prev_ns as i64);
    *prev_ns = ns;

    match ev {
        NetEvent::Tx {
            src,
            dst_mac,
            len,
            frame_hash,
            summary,
            ..
        } => {
            out.push(TAG_TX);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *src as u64);
            let mac = parse_mac(dst_mac).unwrap_or([0; 6]);
            out.extend_from_slice(&mac);
            put_varint(out, *len as u64);
            out.extend_from_slice(&frame_hash.to_le_bytes());
            put_varint(out, summary.len() as u64);
            out.extend_from_slice(summary.as_bytes());
        }
        NetEvent::Deliver {
            src,
            dst,
            len,
            latency_ns,
            ..
        } => {
            out.push(TAG_DELIVER);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *src as u64);
            put_varint(out, *dst as u64);
            put_varint(out, *len as u64);
            put_varint(out, *latency_ns);
        }
        NetEvent::Drop {
            src,
            dst,
            len,
            reason,
            ..
        } => {
            out.push(TAG_DROP);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *src as u64);
            put_varint(out, *dst as u64);
            put_varint(out, *len as u64);
            out.push(match reason {
                DropReason::Partitioned => 0,
                DropReason::RandomDrop => 1,
                DropReason::NodeCrashed => 2,
                DropReason::RxOverflow => 3,
            });
        }
        NetEvent::Duplicate {
            src,
            dst,
            len,
            extra_delay_ns,
            ..
        } => {
            out.push(TAG_DUPLICATE);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *src as u64);
            put_varint(out, *dst as u64);
            put_varint(out, *len as u64);
            put_varint(out, *extra_delay_ns);
        }
        NetEvent::Partition {
            blocked_pairs,
            asymmetric,
            ..
        } => {
            out.push(TAG_PARTITION);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *blocked_pairs as u64);
            out.push(u8::from(*asymmetric));
        }
        NetEvent::Heal { .. } => {
            out.push(TAG_HEAL);
            put_varint(out, zigzag_encode(delta));
        }
        NetEvent::NodeCrash { node, .. } => {
            out.push(TAG_NODE_CRASH);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *node as u64);
        }
        NetEvent::NodeRestart {
            node, boot_count, ..
        } => {
            out.push(TAG_NODE_RESTART);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *node as u64);
            put_varint(out, u64::from(*boot_count));
        }
        NetEvent::LinkUpdated {
            from,
            to,
            latency_ns,
            jitter_ns,
            drop_ppm,
            duplicate_ppm,
            ..
        } => {
            out.push(TAG_LINK_UPDATED);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *from as u64);
            put_varint(out, *to as u64);
            put_varint(out, *latency_ns);
            put_varint(out, *jitter_ns);
            put_varint(out, u64::from(*drop_ppm));
            put_varint(out, u64::from(*duplicate_ppm));
        }
        NetEvent::Clog {
            from,
            to,
            until_ns,
            mode,
            ..
        } => {
            out.push(TAG_CLOG);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, from.map_or(0, |v| (v as u64) + 1));
            put_varint(out, to.map_or(0, |v| (v as u64) + 1));
            put_varint(out, *until_ns);
            out.push(match mode {
                ClogMode::All => 0,
                ClogMode::Send => 1,
                ClogMode::Recv => 2,
            });
        }
        NetEvent::Unclog { from, to, .. } => {
            out.push(TAG_UNCLOG);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, from.map_or(0, |v| (v as u64) + 1));
            put_varint(out, to.map_or(0, |v| (v as u64) + 1));
        }
        NetEvent::NodePause { node, .. } => {
            out.push(TAG_NODE_PAUSE);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *node as u64);
        }
        NetEvent::NodeResume { node, .. } => {
            out.push(TAG_NODE_RESUME);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *node as u64);
        }
        NetEvent::ClockStep { node, delta_ns, .. } => {
            out.push(TAG_CLOCK_STEP);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *node as u64);
            put_varint(out, *delta_ns);
        }
        NetEvent::ClockDrift {
            node, drift_ppm, ..
        } => {
            out.push(TAG_CLOCK_DRIFT);
            put_varint(out, zigzag_encode(delta));
            put_varint(out, *node as u64);
            put_varint(out, zigzag_encode(i64::from(*drift_ppm)));
        }
        NetEvent::NemesisToggled { enabled, .. } => {
            out.push(TAG_NEMESIS_TOGGLED);
            put_varint(out, zigzag_encode(delta));
            out.push(u8::from(*enabled));
        }
        NetEvent::BuggifyToggled { enabled, .. } => {
            out.push(TAG_BUGGIFY_TOGGLED);
            put_varint(out, zigzag_encode(delta));
            out.push(u8::from(*enabled));
        }
    }
}

fn decode_event_body(mut s: &[u8], prev_ns: &mut u64) -> io::Result<NetEvent> {
    let tag = get_u8(&mut s)?;
    let delta = zigzag_decode(get_varint(&mut s)?);
    let ns = (*prev_ns as i64).wrapping_add(delta) as u64;
    *prev_ns = ns;

    let ev = match tag {
        TAG_TX => {
            let src = get_varint(&mut s)? as usize;
            let mac_b = get_bytes(&mut s, 6)?;
            let mac = [mac_b[0], mac_b[1], mac_b[2], mac_b[3], mac_b[4], mac_b[5]];
            let len = get_varint(&mut s)? as usize;
            let hash_b = get_bytes(&mut s, 8)?;
            let frame_hash = u64::from_le_bytes(hash_b.try_into().expect("8 bytes"));
            let summary = get_string(&mut s)?;
            NetEvent::Tx {
                ns,
                src,
                dst_mac: format_mac(mac),
                len,
                frame_hash,
                summary,
            }
        }
        TAG_DELIVER => NetEvent::Deliver {
            ns,
            src: get_varint(&mut s)? as usize,
            dst: get_varint(&mut s)? as usize,
            len: get_varint(&mut s)? as usize,
            latency_ns: get_varint(&mut s)?,
        },
        TAG_DROP => {
            let src = get_varint(&mut s)? as usize;
            let dst = get_varint(&mut s)? as usize;
            let len = get_varint(&mut s)? as usize;
            let reason = match get_u8(&mut s)? {
                0 => DropReason::Partitioned,
                1 => DropReason::RandomDrop,
                2 => DropReason::NodeCrashed,
                3 => DropReason::RxOverflow,
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown drop reason {other} in network.bin"),
                    ));
                }
            };
            NetEvent::Drop {
                ns,
                src,
                dst,
                len,
                reason,
            }
        }
        TAG_DUPLICATE => NetEvent::Duplicate {
            ns,
            src: get_varint(&mut s)? as usize,
            dst: get_varint(&mut s)? as usize,
            len: get_varint(&mut s)? as usize,
            extra_delay_ns: get_varint(&mut s)?,
        },
        TAG_PARTITION => NetEvent::Partition {
            ns,
            blocked_pairs: get_varint(&mut s)? as usize,
            asymmetric: get_u8(&mut s)? != 0,
        },
        TAG_HEAL => NetEvent::Heal { ns },
        TAG_NODE_CRASH => NetEvent::NodeCrash {
            ns,
            node: get_varint(&mut s)? as usize,
        },
        TAG_NODE_RESTART => NetEvent::NodeRestart {
            ns,
            node: get_varint(&mut s)? as usize,
            boot_count: get_varint(&mut s)? as u32,
        },
        TAG_LINK_UPDATED => NetEvent::LinkUpdated {
            ns,
            from: get_varint(&mut s)? as usize,
            to: get_varint(&mut s)? as usize,
            latency_ns: get_varint(&mut s)?,
            jitter_ns: get_varint(&mut s)?,
            drop_ppm: get_varint(&mut s)? as u32,
            duplicate_ppm: get_varint(&mut s)? as u32,
        },
        TAG_CLOG => {
            let from = match get_varint(&mut s)? {
                0 => None,
                v => Some((v - 1) as usize),
            };
            let to = match get_varint(&mut s)? {
                0 => None,
                v => Some((v - 1) as usize),
            };
            let until_ns = get_varint(&mut s)?;
            let mode = match get_u8(&mut s)? {
                0 => ClogMode::All,
                1 => ClogMode::Send,
                2 => ClogMode::Recv,
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown clog mode {other} in network.bin"),
                    ));
                }
            };
            NetEvent::Clog {
                ns,
                from,
                to,
                until_ns,
                mode,
            }
        }
        TAG_UNCLOG => {
            let from = match get_varint(&mut s)? {
                0 => None,
                v => Some((v - 1) as usize),
            };
            let to = match get_varint(&mut s)? {
                0 => None,
                v => Some((v - 1) as usize),
            };
            NetEvent::Unclog { ns, from, to }
        }
        TAG_NODE_PAUSE => NetEvent::NodePause {
            ns,
            node: get_varint(&mut s)? as usize,
        },
        TAG_NODE_RESUME => NetEvent::NodeResume {
            ns,
            node: get_varint(&mut s)? as usize,
        },
        TAG_CLOCK_STEP => NetEvent::ClockStep {
            ns,
            node: get_varint(&mut s)? as usize,
            delta_ns: get_varint(&mut s)?,
        },
        TAG_CLOCK_DRIFT => NetEvent::ClockDrift {
            ns,
            node: get_varint(&mut s)? as usize,
            drift_ppm: zigzag_decode(get_varint(&mut s)?) as i32,
        },
        TAG_NEMESIS_TOGGLED => NetEvent::NemesisToggled {
            ns,
            enabled: get_u8(&mut s)? != 0,
        },
        TAG_BUGGIFY_TOGGLED => NetEvent::BuggifyToggled {
            ns,
            enabled: get_u8(&mut s)? != 0,
        },
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown event tag {other} in network.bin"),
            ));
        }
    };
    if !s.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} trailing bytes in network.bin record", s.len()),
        ));
    }
    Ok(ev)
}

/// Streaming binary writer for `clusters/<id>/network.bin`, matching
/// `rewind_trace::TraceWriter` (versioned magic header + length-prefixed
/// binary records, cut-off-safe if killed mid-run).
#[derive(Debug)]
pub struct NetLogWriter<W: Write> {
    out: BufWriter<W>,
    headed: bool,
    prev_ns: u64,
    scratch: Vec<u8>,
}

impl<W: Write> NetLogWriter<W> {
    pub fn new(out: W) -> Self {
        NetLogWriter {
            out: BufWriter::new(out),
            headed: false,
            prev_ns: 0,
            scratch: Vec::with_capacity(128),
        }
    }

    fn head(&mut self) -> io::Result<()> {
        if self.headed {
            return Ok(());
        }
        self.out.write_all(NET_LOG_MAGIC)?;
        self.out.write_all(&NET_LOG_VERSION.to_le_bytes())?;
        self.headed = true;
        Ok(())
    }

    pub fn record(&mut self, event: &NetEvent) -> io::Result<()> {
        self.head()?;
        encode_event_body(event, &mut self.prev_ns, &mut self.scratch);
        let len = self.scratch.len() as u32;
        self.out.write_all(&len.to_le_bytes())?;
        self.out.write_all(&self.scratch)
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.head()?;
        self.out.flush()?;
        self.out.into_inner().map_err(|e| e.into_error())
    }
}

/// Streaming binary reader for `clusters/<id>/network.bin`, cut-off-safe on
/// `UnexpectedEof` just like `rewind_trace::Records`.
#[derive(Debug)]
pub struct NetLogReader<R: Read> {
    reader: R,
    prev_ns: u64,
    ended: bool,
}

impl<R: Read> NetLogReader<R> {
    pub fn new(mut reader: R) -> io::Result<Self> {
        let mut header = [0u8; NET_LOG_HEADER_LEN];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(NetLogReader {
                    reader,
                    prev_ns: 0,
                    ended: true,
                });
            }
            Err(e) => return Err(e),
        }
        let (magic, ver_b) = header.split_at(NET_LOG_MAGIC.len());
        if magic != NET_LOG_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a valid Rewind network.bin log",
            ));
        }
        let version = u32::from_le_bytes(ver_b.try_into().expect("four bytes"));
        if version != NET_LOG_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "network.bin version {version} is not supported (expected {NET_LOG_VERSION})"
                ),
            ));
        }
        Ok(NetLogReader {
            reader,
            prev_ns: 0,
            ended: false,
        })
    }

    fn read_one(&mut self) -> io::Result<Option<NetEvent>> {
        let mut len_b = [0u8; 4];
        match self.reader.read_exact(&mut len_b) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_b) as usize;
        if len == 0 || len > NET_RECORD_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid network.bin record length {len}"),
            ));
        }
        let mut buf = vec![0u8; len];
        match self.reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        Ok(Some(decode_event_body(&buf, &mut self.prev_ns)?))
    }
}

impl<R: Read> Iterator for NetLogReader<R> {
    type Item = io::Result<NetEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        let res = self.read_one();
        self.ended = !matches!(res, Ok(Some(_)));
        res.transpose()
    }
}
