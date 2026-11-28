//! Deterministic, crash-consistent paravirtual block device (`rewind-blk` / `/dev/rwd0`).
//!
//! Models a real disk controller with a volatile write cache:
//! - Normal writes (`BLK_OP_WRITE`) land in `dirty` (512-byte sectors) and are
//!   immediately visible to subsequent reads on the same boot.
//! - Flush (`BLK_OP_FLUSH`, triggered by Linux `REQ_OP_FLUSH` / `fsync`) commits
//!   all `dirty` sectors into `committed` (sparse 4 KiB blocks) and clears `dirty`.
//! - FUA writes (`BLK_OP_WRITE_FUA`, triggered by `REQ_FUA`) write directly to
//!   `committed` and remove any stale copy in `dirty`.
//! - Power loss (`VirtualDisk::on_power_loss`, called on `Cluster::crash`) drops
//!   every uncommitted sector in `dirty` while preserving `committed` intact across
//!   the node's reboot.
//!
//! Because `committed` and `dirty` are deterministic `BTreeMap`s stored entirely
//! in memory, disk state snapshots, restores, and cluster forks are bit-for-bit
//! deterministic with zero host filesystem side effects.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const SECTOR_SIZE: usize = 512;
pub const BLOCK_SIZE: usize = 4096;
const SECTORS_PER_BLOCK: u64 = (BLOCK_SIZE / SECTOR_SIZE) as u64;

/// Runtime counters for a node's paravirtual disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskStats {
    pub capacity_bytes: u64,
    pub reads: u64,
    pub writes: u64,
    pub fua_writes: u64,
    pub flushes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub committed_blocks: u64,
    pub dirty_sectors: u64,
    pub dirty_dropped_on_crash: u64,
    pub power_losses: u64,
}

/// Serializable point-in-time snapshot of a [`VirtualDisk`], stored inside
/// [`crate::cluster::ClusterNodeKeyframe`] so snapshot/restore/fork preserve
/// exact committed and volatile write-cache state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskSnapshot {
    pub capacity_bytes: u64,
    /// `(block_idx, 4 KiB block bytes)` for all non-zero committed blocks.
    pub committed: Vec<(u64, Vec<u8>)>,
    /// `(sector_idx, 512 B sector bytes)` for all uncommitted volatile sectors.
    pub dirty: Vec<(u64, Vec<u8>)>,
    pub stats: DiskStats,
}

/// Sparse, deterministic virtual block device with volatile write-cache
/// power-loss semantics.
#[derive(Clone, Debug)]
pub struct VirtualDisk {
    capacity_bytes: u64,
    /// Durable storage (4 KiB sparse blocks), survives `on_power_loss()`.
    committed: BTreeMap<u64, Box<[u8; BLOCK_SIZE]>>,
    /// Volatile write cache (512-byte sectors), dropped on `on_power_loss()`.
    dirty: BTreeMap<u64, [u8; SECTOR_SIZE]>,
    stats: DiskStats,
}

impl VirtualDisk {
    /// Creates an empty (all-zero) virtual disk of `size_mib` MiB.
    pub fn new(size_mib: u64) -> VirtualDisk {
        let capacity_bytes = size_mib.max(1).saturating_mul(1024 * 1024);
        VirtualDisk {
            capacity_bytes,
            committed: BTreeMap::new(),
            dirty: BTreeMap::new(),
            stats: DiskStats {
                capacity_bytes,
                ..DiskStats::default()
            },
        }
    }

    /// Total capacity in bytes.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Total capacity in 512-byte sectors.
    pub fn sectors(&self) -> u64 {
        self.capacity_bytes / SECTOR_SIZE as u64
    }

    /// Current I/O and durability statistics.
    pub fn stats(&self) -> DiskStats {
        let mut s = self.stats;
        s.committed_blocks = self.committed.len() as u64;
        s.dirty_sectors = self.dirty.len() as u64;
        s
    }

    /// Reads `out.len()` bytes starting at 512-byte `sector`. `out.len()` must
    /// be a non-zero multiple of 512 and stay within disk bounds.
    pub fn read_sectors(&mut self, sector: u64, out: &mut [u8]) -> bool {
        if out.is_empty() || !out.len().is_multiple_of(SECTOR_SIZE) {
            return false;
        }
        let count = (out.len() / SECTOR_SIZE) as u64;
        let Some(end_sector) = sector.checked_add(count) else {
            return false;
        };
        if end_sector > self.sectors() {
            return false;
        }
        for i in 0..count {
            let s = sector + i;
            let dst = &mut out[(i as usize) * SECTOR_SIZE..((i as usize) + 1) * SECTOR_SIZE];
            if let Some(cached) = self.dirty.get(&s) {
                dst.copy_from_slice(cached);
            } else {
                let blk_idx = s / SECTORS_PER_BLOCK;
                let off = ((s % SECTORS_PER_BLOCK) as usize) * SECTOR_SIZE;
                if let Some(blk) = self.committed.get(&blk_idx) {
                    dst.copy_from_slice(&blk[off..off + SECTOR_SIZE]);
                } else {
                    dst.fill(0);
                }
            }
        }
        self.stats.reads += 1;
        self.stats.bytes_read += out.len() as u64;
        true
    }

    /// Writes `data` starting at 512-byte `sector`.
    /// - When `fua` is `false`, writes go only into the volatile `dirty` cache.
    /// - When `fua` is `true` (`REQ_FUA`), writes commit directly to `committed`
    ///   (and clear any superseded volatile copy of those sectors).
    pub fn write_sectors(&mut self, sector: u64, data: &[u8], fua: bool) -> bool {
        if data.is_empty() || !data.len().is_multiple_of(SECTOR_SIZE) {
            return false;
        }
        let count = (data.len() / SECTOR_SIZE) as u64;
        let Some(end_sector) = sector.checked_add(count) else {
            return false;
        };
        if end_sector > self.sectors() {
            return false;
        }
        for i in 0..count {
            let s = sector + i;
            let src = &data[(i as usize) * SECTOR_SIZE..((i as usize) + 1) * SECTOR_SIZE];
            if fua {
                self.dirty.remove(&s);
                self.commit_sector(s, src);
            } else {
                let mut buf = [0u8; SECTOR_SIZE];
                buf.copy_from_slice(src);
                self.dirty.insert(s, buf);
            }
        }
        self.stats.writes += 1;
        if fua {
            self.stats.fua_writes += 1;
        }
        self.stats.bytes_written += data.len() as u64;
        true
    }

    /// Commits every sector in the volatile write cache (`dirty`) into durable
    /// storage (`committed`), modeling `REQ_OP_FLUSH` / `SYNCHRONIZE CACHE`.
    pub fn flush(&mut self) {
        let dirty = std::mem::take(&mut self.dirty);
        for (sector, bytes) in dirty {
            self.commit_sector(sector, &bytes);
        }
        self.stats.flushes += 1;
    }

    /// Simulates sudden power loss: discards every uncommitted sector in the
    /// volatile write cache (`dirty`) while keeping `committed` durable blocks
    /// intact for the node's next boot.
    pub fn on_power_loss(&mut self) -> u64 {
        let dropped = self.dirty.len() as u64;
        self.dirty.clear();
        self.stats.dirty_dropped_on_crash += dropped;
        self.stats.power_losses += 1;
        dropped
    }

    /// Overwrites arbitrary bytes directly in committed storage (e.g. for
    /// deterministic bitflip / torn-sector / corruption injection tests).
    pub fn corrupt_bytes(&mut self, offset: u64, bytes: &[u8]) -> bool {
        let Some(end) = offset.checked_add(bytes.len() as u64) else {
            return false;
        };
        if end > self.capacity_bytes {
            return false;
        }
        for (i, &b) in bytes.iter().enumerate() {
            let pos = offset + i as u64;
            let sector = pos / SECTOR_SIZE as u64;
            let s_off = (pos % SECTOR_SIZE as u64) as usize;
            if let Some(cached) = self.dirty.get_mut(&sector) {
                cached[s_off] = b;
            }
            let blk_idx = pos / BLOCK_SIZE as u64;
            let b_off = (pos % BLOCK_SIZE as u64) as usize;
            let blk = self
                .committed
                .entry(blk_idx)
                .or_insert_with(|| Box::new([0u8; BLOCK_SIZE]));
            blk[b_off] = b;
        }
        true
    }

    /// Captures a serializable deterministic snapshot of both committed and
    /// volatile disk state.
    pub fn snapshot(&self) -> DiskSnapshot {
        DiskSnapshot {
            capacity_bytes: self.capacity_bytes,
            committed: self
                .committed
                .iter()
                .map(|(&k, v)| (k, v.to_vec()))
                .collect(),
            dirty: self.dirty.iter().map(|(&k, v)| (k, v.to_vec())).collect(),
            stats: self.stats(),
        }
    }

    /// Restores committed and volatile disk state from a [`DiskSnapshot`].
    pub fn restore(&mut self, snap: &DiskSnapshot) {
        self.capacity_bytes = snap.capacity_bytes;
        self.committed.clear();
        for (k, v) in &snap.committed {
            if v.len() == BLOCK_SIZE {
                let mut arr = Box::new([0u8; BLOCK_SIZE]);
                arr.copy_from_slice(v);
                self.committed.insert(*k, arr);
            }
        }
        self.dirty.clear();
        for (k, v) in &snap.dirty {
            if v.len() == SECTOR_SIZE {
                let mut arr = [0u8; SECTOR_SIZE];
                arr.copy_from_slice(v);
                self.dirty.insert(*k, arr);
            }
        }
        self.stats = snap.stats;
    }

    fn commit_sector(&mut self, sector: u64, data: &[u8]) {
        let blk_idx = sector / SECTORS_PER_BLOCK;
        let off = ((sector % SECTORS_PER_BLOCK) as usize) * SECTOR_SIZE;
        let blk = self
            .committed
            .entry(blk_idx)
            .or_insert_with(|| Box::new([0u8; BLOCK_SIZE]));
        blk[off..off + SECTOR_SIZE].copy_from_slice(data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn power_loss_drops_unflushed_writes_and_preserves_flushed_writes() {
        let mut disk = VirtualDisk::new(4);
        let a = [0xAAu8; SECTOR_SIZE];
        let b = [0xBBu8; SECTOR_SIZE];
        let c = [0xCCu8; SECTOR_SIZE];

        // Sector 0: written + flushed (durable).
        assert!(disk.write_sectors(0, &a, false));
        disk.flush();

        // Sector 1: written WITHOUT flush (volatile).
        assert!(disk.write_sectors(1, &b, false));

        // Sector 2: written with FUA (immediately durable).
        assert!(disk.write_sectors(2, &c, true));

        // Before power loss, all three sectors read back as written.
        let mut buf = [0u8; SECTOR_SIZE * 3];
        assert!(disk.read_sectors(0, &mut buf));
        assert_eq!(&buf[0..512], &a);
        assert_eq!(&buf[512..1024], &b);
        assert_eq!(&buf[1024..1536], &c);

        // Simulate power loss: only volatile sector 1 is lost.
        assert_eq!(disk.on_power_loss(), 1);

        let mut after = [0u8; SECTOR_SIZE * 3];
        assert!(disk.read_sectors(0, &mut after));
        assert_eq!(&after[0..512], &a);
        assert_eq!(&after[512..1024], &[0u8; SECTOR_SIZE]);
        assert_eq!(&after[1024..1536], &c);

        let stats = disk.stats();
        assert_eq!(stats.flushes, 1);
        assert_eq!(stats.fua_writes, 1);
        assert_eq!(stats.dirty_dropped_on_crash, 1);
        assert_eq!(stats.power_losses, 1);
    }

    #[test]
    fn snapshot_restore_round_trips_committed_and_dirty() {
        let mut disk = VirtualDisk::new(2);
        let a = [0x11u8; SECTOR_SIZE];
        let b = [0x22u8; SECTOR_SIZE];
        assert!(disk.write_sectors(4, &a, true));
        assert!(disk.write_sectors(5, &b, false));
        let snap = disk.snapshot();

        disk.on_power_loss();
        let mut out = [0u8; SECTOR_SIZE];
        assert!(disk.read_sectors(5, &mut out));
        assert_eq!(out, [0u8; SECTOR_SIZE]);

        disk.restore(&snap);
        assert!(disk.read_sectors(4, &mut out));
        assert_eq!(out, a);
        assert!(disk.read_sectors(5, &mut out));
        assert_eq!(out, b);
    }
}
