//! v0.7 WS-plane PSYNC snapshot: export / install the process-global
//! `WorkspaceRegistry` as a moon-private RDB aux blob (Wave B ws-plane).
//!
//! Unlike vector/text/graph state, the workspace registry is NOT sharded — it
//! is one `Mutex<Option<Box<WorkspaceRegistry>>>` on `ShardDatabases`, shared
//! by every shard's thread (`shard/shared_databases.rs`). Only shard 0
//! captures it (shard-0-authoritative, mirroring the WS.CREATE/DROP
//! write-path pin — the registry's replication-offset accounting lives
//! entirely on shard 0's thread, so the snapshot leg captures it from the
//! same place): the single-shard master path
//! (`master::handle_psync_inline_single_shard`) captures it directly; the
//! multi-shard merge path (`master::handle_psync_inline_multi_shard`)
//! captures it ONLY in shard 0's `PrepareReplicaSync` leg
//! (`PreparedShardSync::ws_registry_blob`), leaving it `None` on every other
//! shard so the merge loop's "keep the first `Some`" convention (same as
//! `vector_defs`/`text_defs`) picks shard 0's copy.
//!
//! Blob format (version 1, little-endian):
//! ```text
//! [u8  version = 1]
//! [u32 entry_count]
//! per entry:
//!   [16 bytes ws_id]
//!   [u32 name_len][name bytes]
//!   [i64 created_at_ms]
//! ```
//!
//! An empty registry (or no registry ever initialized) still encodes as
//! `entry_count = 0` — this lets the replica distinguish "master
//! authoritatively has zero workspaces" (install an empty registry,
//! replacing whatever local entries exist) from "pre-WS-sync master, aux
//! absent entirely" (warn and keep local state), the same convention
//! `export_graph_store` uses for graphs.

use bytes::Bytes;

use crate::workspace::WorkspaceId;
use crate::workspace::registry::{WorkspaceMetadata, WorkspaceRegistry};

const FORMAT_VERSION: u8 = 1;

/// Export the registry as a snapshot blob. `registry: None` (never
/// initialized) encodes identically to `Some(empty)`.
pub fn export_workspace_registry(registry: Option<&WorkspaceRegistry>) -> Vec<u8> {
    let count = registry.map_or(0, |r| r.len());
    let mut buf = Vec::with_capacity(5 + count * 32);
    buf.push(FORMAT_VERSION);
    buf.extend_from_slice(&(count as u32).to_le_bytes());
    if let Some(reg) = registry {
        for (id, meta) in reg.iter() {
            buf.extend_from_slice(id.as_bytes());
            buf.extend_from_slice(&(meta.name.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta.name);
            buf.extend_from_slice(&meta.created_at.to_le_bytes());
        }
    }
    buf
}

/// Decode a snapshot blob into a fresh `WorkspaceRegistry` (authoritative
/// replace — the caller installs it in place of whatever the replica already
/// had). Returns `None` on a malformed blob (truncated / unknown version) —
/// the caller aborts the sync and the replica retries with a fresh full
/// resync, matching `graph_sync::install_graph_store`'s contract.
pub fn install_workspace_registry(blob: &[u8]) -> Option<WorkspaceRegistry> {
    let mut cur = Cursor { data: blob, pos: 0 };
    if cur.u8()? != FORMAT_VERSION {
        return None;
    }
    let count = cur.u32()? as usize;
    let mut reg = WorkspaceRegistry::new();
    for _ in 0..count {
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(cur.take(16)?);
        let name_len = cur.u32()? as usize;
        let name = Bytes::copy_from_slice(cur.take(name_len)?);
        let created_at = cur.i64()?;
        let id = WorkspaceId::from_bytes(id_bytes);
        reg.insert(
            id,
            WorkspaceMetadata {
                id,
                name,
                created_at,
            },
        );
    }
    Some(reg)
}

/// Minimal bounds-checked reader over the blob (mirrors `graph_sync::Cursor`).
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        if end > self.data.len() {
            return None;
        }
        let s = &self.data[self.pos..end];
        self.pos = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        #[allow(clippy::unwrap_used)] // take(4) guarantees the length
        Some(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Option<i64> {
        #[allow(clippy::unwrap_used)] // take(8) guarantees the length
        Some(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(id: WorkspaceId, name: &str, created_at: i64) -> WorkspaceMetadata {
        WorkspaceMetadata {
            id,
            name: Bytes::from(name.to_owned()),
            created_at,
        }
    }

    #[test]
    fn export_install_round_trip_preserves_entries() {
        let mut reg = WorkspaceRegistry::new();
        let id1 = WorkspaceId::from_bytes([1u8; 16]);
        let id2 = WorkspaceId::from_bytes([2u8; 16]);
        reg.insert(id1, make_meta(id1, "acme", 1_752_000_000_000));
        reg.insert(id2, make_meta(id2, "widgets", -5));

        let blob = export_workspace_registry(Some(&reg));
        let installed = install_workspace_registry(&blob).expect("valid blob");

        assert_eq!(installed.len(), 2);
        let m1 = installed.get(&id1).expect("id1 present");
        assert_eq!(m1.name.as_ref(), b"acme");
        assert_eq!(m1.created_at, 1_752_000_000_000);
        let m2 = installed.get(&id2).expect("id2 present");
        assert_eq!(m2.name.as_ref(), b"widgets");
        assert_eq!(m2.created_at, -5);
    }

    #[test]
    fn none_and_empty_registry_both_encode_as_zero_entries() {
        let empty = WorkspaceRegistry::new();
        assert_eq!(
            export_workspace_registry(None),
            export_workspace_registry(Some(&empty))
        );
        let installed = install_workspace_registry(&export_workspace_registry(None)).unwrap();
        assert!(installed.is_empty());
    }

    #[test]
    fn malformed_blob_rejected() {
        assert!(install_workspace_registry(b"").is_none());
        assert!(install_workspace_registry(&[9u8]).is_none()); // bad version, no count
        assert!(install_workspace_registry(&[FORMAT_VERSION]).is_none()); // truncated count

        // Truncated mid-entry: valid header claiming 1 entry but no data.
        let mut bad = vec![FORMAT_VERSION];
        bad.extend_from_slice(&1u32.to_le_bytes());
        assert!(install_workspace_registry(&bad).is_none());
    }

    #[test]
    fn truncated_full_blob_rejected() {
        let mut reg = WorkspaceRegistry::new();
        let id = WorkspaceId::from_bytes([3u8; 16]);
        reg.insert(id, make_meta(id, "acme", 42));
        let blob = export_workspace_registry(Some(&reg));
        assert!(install_workspace_registry(&blob[..blob.len() - 3]).is_none());
    }

    #[test]
    fn empty_name_and_negative_created_at_survive() {
        let mut reg = WorkspaceRegistry::new();
        let id = WorkspaceId::from_bytes([4u8; 16]);
        reg.insert(id, make_meta(id, "", -1));
        let blob = export_workspace_registry(Some(&reg));
        let installed = install_workspace_registry(&blob).unwrap();
        let m = installed.get(&id).unwrap();
        assert!(m.name.is_empty());
        assert_eq!(m.created_at, -1);
    }
}
