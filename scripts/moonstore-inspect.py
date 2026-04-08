#!/usr/bin/env python3
"""MoonStore V2 file inspector — decode and display all tier data.

Usage:
  python3 scripts/moonstore-inspect.py /tmp/moon-tier-32mb
  python3 scripts/moonstore-inspect.py /tmp/moon-tier-32mb --tier cold
  python3 scripts/moonstore-inspect.py /tmp/moon-tier-32mb --tier warm
  python3 scripts/moonstore-inspect.py /tmp/moon-tier-32mb --tier kv
  python3 scripts/moonstore-inspect.py /tmp/moon-tier-32mb --tier manifest
  python3 scripts/moonstore-inspect.py /tmp/moon-tier-32mb --tier wal
  python3 scripts/moonstore-inspect.py /tmp/moon-tier-32mb --tier all
"""

import argparse
import glob
import os
import struct
import sys

# ── Constants from Rust source ───────────────────────────────────────────

MOONPAGE_MAGIC = 0x4D4E5047  # "GPNM" LE
PAGE_4K = 4096
PAGE_64K = 65536
HEADER_SIZE = 64
KV_PAGE_HEADER_SIZE = 16
KV_DATA_START = HEADER_SIZE + KV_PAGE_HEADER_SIZE  # 80
SLOT_SIZE = 4
NEIGHBOR_SENTINEL = 0xFFFFFFFF

PAGE_TYPES = {
    0x01: "ManifestRoot", 0x02: "ManifestEntry", 0x03: "ControlPage",
    0x04: "ClogPage", 0x10: "KvLeaf", 0x11: "KvOverflow", 0x12: "KvIndex",
    0x18: "HashBucket", 0x19: "ListChunk", 0x1A: "SetBucket",
    0x1B: "ZSetSkip", 0x1C: "StreamEntries", 0x20: "VecCodes",
    0x21: "VecFull", 0x22: "VecGraph", 0x23: "VecMvcc", 0x24: "VecMeta",
    0x25: "VecUndo",
}

VALUE_TYPES = {0: "String", 1: "Hash", 2: "List", 3: "Set", 4: "SortedSet", 5: "Stream"}

MANIFEST_TIERS = {0: "Hot", 1: "Warm", 2: "Cold"}
MANIFEST_STATUS = {0: "Active", 1: "Building", 2: "Compacting", 3: "Tombstone"}


# ── MoonPage Header Parser ──────────────────────────────────────────────

def parse_header(buf):
    """Parse 64-byte MoonPageHeader. Returns dict or None."""
    if len(buf) < HEADER_SIZE:
        return None
    magic = struct.unpack_from("<I", buf, 0)[0]
    if magic != MOONPAGE_MAGIC:
        return None
    return {
        "magic": magic,
        "format_version": buf[4],
        "page_type": buf[5],
        "page_type_name": PAGE_TYPES.get(buf[5], f"Unknown(0x{buf[5]:02X})"),
        "flags": struct.unpack_from("<H", buf, 6)[0],
        "page_lsn": struct.unpack_from("<Q", buf, 8)[0],
        "checksum": struct.unpack_from("<I", buf, 16)[0],
        "payload_bytes": struct.unpack_from("<I", buf, 20)[0],
        "page_id": struct.unpack_from("<Q", buf, 24)[0],
        "file_id": struct.unpack_from("<Q", buf, 32)[0],
        "prev_page": struct.unpack_from("<I", buf, 40)[0],
        "next_page": struct.unpack_from("<I", buf, 44)[0],
        "txn_id": struct.unpack_from("<Q", buf, 48)[0],
        "entry_count": struct.unpack_from("<I", buf, 56)[0],
        "reserved": struct.unpack_from("<I", buf, 60)[0],
    }


def fmt_header(h, indent="  "):
    return (
        f"{indent}page_type: {h['page_type_name']} (0x{h['page_type']:02X})\n"
        f"{indent}page_id:   {h['page_id']}  file_id: {h['file_id']}\n"
        f"{indent}lsn:       {h['page_lsn']}  checksum: 0x{h['checksum']:08X}\n"
        f"{indent}payload:   {h['payload_bytes']} bytes  entries: {h['entry_count']}\n"
        f"{indent}flags:     0x{h['flags']:04X}  txn_id: {h['txn_id']}"
    )


# ── KV Spill Inspector ──────────────────────────────────────────────────

def inspect_kv_spill(shard_dir, max_files=5):
    print("\n" + "=" * 65)
    print(" KV SPILL TIER: heap-*.mpf DataFiles")
    print("=" * 65)

    heap_files = sorted(glob.glob(os.path.join(shard_dir, "data/heap-*.mpf")))
    if not heap_files:
        print("  (no heap files)")
        return

    total = sum(os.path.getsize(f) for f in heap_files)
    print(f"  Files: {len(heap_files)} | Total: {total:,} bytes ({total/1024:.1f} KB)")
    print()

    for hf in heap_files[:max_files]:
        fname = os.path.basename(hf)
        with open(hf, "rb") as f:
            page = f.read(PAGE_4K)

        hdr = parse_header(page)
        if not hdr:
            print(f"  {fname}: INVALID HEADER")
            continue

        print(f"  {fname}:")
        print(fmt_header(hdr, "    "))

        # Parse slot directory + entries
        # Slots start at KV_DATA_START, entries grow after slots
        slot_count = hdr["entry_count"]
        if slot_count == 0:
            # Try reading slot_count from KV page header area
            slot_count = struct.unpack_from("<H", page, HEADER_SIZE)[0]

        if slot_count > 0 and slot_count < 200:
            print(f"    Entries ({slot_count}):")
            for s in range(min(slot_count, 5)):
                slot_pos = KV_DATA_START + s * SLOT_SIZE
                if slot_pos + SLOT_SIZE > len(page):
                    break
                entry_off = struct.unpack_from("<H", page, slot_pos)[0]
                entry_len = struct.unpack_from("<H", page, slot_pos + 2)[0]

                if entry_off == 0 or entry_off + 4 > len(page):
                    continue

                cursor = entry_off
                key_len = struct.unpack_from("<H", page, cursor)[0]
                cursor += 2
                vtype = page[cursor]
                cursor += 1
                flags = page[cursor]
                cursor += 1

                ttl = None
                if flags & 0x01:  # HAS_TTL
                    if cursor + 8 <= len(page):
                        ttl = struct.unpack_from("<Q", page, cursor)[0]
                        cursor += 8

                key = page[cursor:cursor + min(key_len, 60)]
                cursor += key_len

                if cursor + 4 <= len(page):
                    val_len = struct.unpack_from("<I", page, cursor)[0]
                    cursor += 4
                    val_preview = page[cursor:cursor + min(val_len, 40)]
                else:
                    val_len = 0
                    val_preview = b""

                compressed = " [LZ4]" if flags & 0x02 else ""
                ttl_str = f" ttl={ttl}ms" if ttl else ""
                try:
                    key_str = key.decode("utf-8", errors="replace")
                except Exception:
                    key_str = repr(key)
                try:
                    val_str = val_preview.decode("utf-8", errors="replace")
                except Exception:
                    val_str = repr(val_preview)

                print(f"      [{s}] key=\"{key_str}\" ({key_len}B) "
                      f"val=\"{val_str}{'...' if val_len > 40 else ''}\" ({val_len}B) "
                      f"type={VALUE_TYPES.get(vtype, vtype)}{compressed}{ttl_str}")
        print()

    if len(heap_files) > max_files:
        print(f"  ... and {len(heap_files) - max_files} more files")


# ── Vamana (COLD) Inspector ──────────────────────────────────────────────

def inspect_cold(shard_dir, max_nodes=5):
    print("\n" + "=" * 65)
    print(" COLD TIER: DiskANN Files")
    print("=" * 65)

    diskann_dirs = sorted(glob.glob(os.path.join(shard_dir, "vectors/segment-*-diskann")))
    if not diskann_dirs:
        print("  (no DiskANN directories)")
        return

    for ddir in diskann_dirs:
        dname = os.path.basename(ddir)
        vamana_path = os.path.join(ddir, "vamana.mpf")
        pq_path = os.path.join(ddir, "pq_codes.bin")

        print(f"\n  {dname}/")

        # ── vamana.mpf ──
        if os.path.exists(vamana_path):
            vsize = os.path.getsize(vamana_path)
            n_pages = vsize // PAGE_4K
            print(f"    vamana.mpf: {vsize:,} bytes ({n_pages} nodes x 4KB)")

            with open(vamana_path, "rb") as f:
                for node_idx in range(min(max_nodes, n_pages)):
                    page = f.read(PAGE_4K)
                    if len(page) < PAGE_4K:
                        break

                    hdr = parse_header(page)
                    if not hdr:
                        print(f"      node[{node_idx}]: INVALID HEADER")
                        continue

                    # Payload at offset 64:
                    # [node_id: u32] [degree: u16] [reserved: u16] [vector: f32 * dim] [neighbors: u32 * max_degree]
                    off = HEADER_SIZE
                    node_id = struct.unpack_from("<I", page, off)[0]
                    off += 4
                    degree = struct.unpack_from("<H", page, off)[0]
                    off += 2
                    off += 2  # reserved

                    # Infer dim from payload_bytes:
                    # payload = 4 + 2 + 2 + dim*4 + max_degree*4
                    # We know degree, try to figure out dim
                    # Read first few floats as vector
                    vec_preview = []
                    for _ in range(min(6, 128)):
                        if off + 4 > len(page):
                            break
                        v = struct.unpack_from("<f", page, off)[0]
                        vec_preview.append(v)
                        off += 4

                    # Read neighbors after vector (we don't know exact dim, scan for sentinel)
                    # For display, show the first few floats and neighbors from degree
                    neighbors = []
                    # Skip to after the vector: estimate dim from payload
                    # payload_bytes = 8 + dim*4 + max_degree*4
                    # If we know payload_bytes and max_degree... just show what we have
                    vec_str = ", ".join(f"{v:.4f}" for v in vec_preview[:4])

                    print(f"      node[{node_idx}]: id={node_id} degree={degree} "
                          f"vec=[{vec_str}, ...]")

            if n_pages > max_nodes:
                print(f"      ... and {n_pages - max_nodes} more nodes")

        # ── pq_codes.bin ──
        if os.path.exists(pq_path):
            pq_size = os.path.getsize(pq_path)
            with open(pq_path, "rb") as f:
                pq_data = f.read()

            # Try common subspace counts: dim/4, dim/8, dim/16
            # Find m where pq_size % m == 0 and n = pq_size/m is reasonable
            print(f"    pq_codes.bin: {pq_size:,} bytes")
            for m in [4, 8, 16, 32, 48, 64]:
                if pq_size % m == 0:
                    n = pq_size // m
                    if 10 <= n <= 1_000_000:
                        print(f"      m={m} subspaces, {n} vectors, {m} bytes/vec")
                        for i in range(min(5, n)):
                            codes = list(pq_data[i * m:(i + 1) * m])
                            code_str = " ".join(f"{c:3d}" for c in codes)
                            print(f"        vec[{i:4d}]: [{code_str}]")
                        if n > 5:
                            print(f"        ... and {n - 5} more vectors")
                        break


# ── Warm Segment Inspector ───────────────────────────────────────────────

def inspect_warm(shard_dir):
    print("\n" + "=" * 65)
    print(" WARM TIER: Segment .mpf Files")
    print("=" * 65)

    seg_dirs = sorted(glob.glob(os.path.join(shard_dir, "vectors/segment-*")))
    seg_dirs = [d for d in seg_dirs if not d.endswith("-diskann")]  # exclude cold

    if not seg_dirs:
        print("  (no warm segments — may have been consumed by cold transition)")
        return

    for sdir in seg_dirs:
        sname = os.path.basename(sdir)
        print(f"\n  {sname}/")

        for fname in sorted(os.listdir(sdir)):
            fpath = os.path.join(sdir, fname)
            fsize = os.path.getsize(fpath)
            n_pages = fsize // PAGE_4K if fsize >= PAGE_4K else 0

            if fname.endswith(".mpf"):
                with open(fpath, "rb") as f:
                    first_page = f.read(min(PAGE_4K, fsize))

                hdr = parse_header(first_page)
                if hdr:
                    print(f"    {fname}: {fsize:,}B ({n_pages} pages) "
                          f"type={hdr['page_type_name']}")
                    print(f"      payload={hdr['payload_bytes']}B entries={hdr['entry_count']} "
                          f"lsn={hdr['page_lsn']}")

                    # For codes.mpf, show TQ code stats
                    if "codes" in fname and fsize > HEADER_SIZE:
                        # Each page has header (64B) + sub-header (32B) + TQ codes
                        code_bytes = fsize - n_pages * (HEADER_SIZE + 32) if n_pages > 0 else 0
                        print(f"      TQ code bytes: ~{code_bytes:,}B")

                    # For graph.mpf, show graph stats
                    if "graph" in fname and fsize > HEADER_SIZE:
                        print(f"      HNSW graph: ~{n_pages} layer-0 pages")

                    # For mvcc.mpf, show ID mapping stats
                    if "mvcc" in fname and hdr["entry_count"] > 0:
                        print(f"      Global ID mappings: {hdr['entry_count']}")

                else:
                    print(f"    {fname}: {fsize:,}B (no MoonPage header)")

            elif fname == "deletion.bitmap":
                print(f"    {fname}: {fsize:,}B")
            else:
                print(f"    {fname}: {fsize:,}B")


# ── Manifest Inspector ───────────────────────────────────────────────────

def inspect_manifest(shard_dir, max_entries=20):
    print("\n" + "=" * 65)
    print(" MANIFEST: File Registry (dual-root atomic)")
    print("=" * 65)

    manifest_path = os.path.join(shard_dir, os.path.basename(shard_dir) + ".manifest")
    if not os.path.exists(manifest_path):
        # Try shard-0.manifest pattern
        candidates = glob.glob(os.path.join(shard_dir, "*.manifest"))
        if candidates:
            manifest_path = candidates[0]
        else:
            print("  (no manifest file)")
            return

    fsize = os.path.getsize(manifest_path)
    print(f"  File: {os.path.basename(manifest_path)} ({fsize:,} bytes)")

    with open(manifest_path, "rb") as f:
        data = f.read()

    # Parse dual-root header (first 2 x 4KB pages)
    # Root page 0 at offset 0, root page 1 at offset 4096
    for root_idx in range(2):
        root_off = root_idx * PAGE_4K
        if root_off + HEADER_SIZE > len(data):
            break
        hdr = parse_header(data[root_off:root_off + HEADER_SIZE])
        if hdr:
            print(f"  Root[{root_idx}]: type={hdr['page_type_name']} "
                  f"entry_count={hdr['entry_count']} lsn={hdr['page_lsn']}")

    # File entries start after 2 root pages (offset 8192)
    # Each FileEntry is serialized as fixed-size records
    # Scan for recognizable patterns
    entry_start = 2 * PAGE_4K
    if entry_start >= len(data):
        # Try scanning from beginning for file entries
        entry_start = PAGE_4K

    # FileEntry layout (from manifest.rs): 48 bytes each
    # [file_id: u64] [file_type: u8] [status: u8] [tier: u8] [page_size_log2: u8]
    # [page_count: u32] [byte_size: u64] [created_lsn: u64] [min_key_hash: u64] [max_key_hash: u64]
    ENTRY_SIZE = 48
    remaining = data[entry_start:]
    n_possible = len(remaining) // ENTRY_SIZE

    entries = []
    for i in range(n_possible):
        off = i * ENTRY_SIZE
        e = remaining[off:off + ENTRY_SIZE]
        if len(e) < ENTRY_SIZE:
            break
        fid = struct.unpack_from("<Q", e, 0)[0]
        ftype = e[8]
        status = e[9]
        tier = e[10]
        pg_log2 = e[11]
        pg_count = struct.unpack_from("<I", e, 12)[0]
        byte_size = struct.unpack_from("<Q", e, 16)[0]
        created_lsn = struct.unpack_from("<Q", e, 24)[0]

        # Filter out garbage entries
        if fid == 0 and ftype == 0 and byte_size == 0:
            continue
        if fid > 100000:  # sanity check
            continue

        entries.append({
            "file_id": fid, "type": PAGE_TYPES.get(ftype, f"0x{ftype:02X}"),
            "status": MANIFEST_STATUS.get(status, f"0x{status:02X}"),
            "tier": MANIFEST_TIERS.get(tier, f"0x{tier:02X}"),
            "pg_size": 1 << pg_log2 if pg_log2 < 20 else 0,
            "pg_count": pg_count, "byte_size": byte_size,
            "created_lsn": created_lsn,
        })

    print(f"  File entries: {len(entries)}")
    print()

    # Group by tier
    for tier_name in ["Hot", "Warm", "Cold"]:
        tier_entries = [e for e in entries if e["tier"] == tier_name]
        if tier_entries:
            print(f"  [{tier_name}] ({len(tier_entries)} files):")
            for e in tier_entries[:max_entries]:
                print(f"    id={e['file_id']:3d} type={e['type']:14s} "
                      f"status={e['status']:10s} pages={e['pg_count']:4d} "
                      f"size={e['byte_size']:8,}B pg={e['pg_size']}B")
            if len(tier_entries) > max_entries:
                print(f"    ... and {len(tier_entries) - max_entries} more")
            print()


# ── Control File Inspector ───────────────────────────────────────────────

def inspect_control(shard_dir):
    print("\n" + "=" * 65)
    print(" CONTROL FILE: Checkpoint State")
    print("=" * 65)

    ctrl_files = glob.glob(os.path.join(shard_dir, "*.control"))
    if not ctrl_files:
        print("  (no control file)")
        return

    ctrl_path = ctrl_files[0]
    with open(ctrl_path, "rb") as f:
        data = f.read()

    print(f"  File: {os.path.basename(ctrl_path)} ({len(data)} bytes)")
    hdr = parse_header(data)
    if hdr:
        print(fmt_header(hdr, "  "))

    # Control file specific fields are after the header
    if len(data) >= HEADER_SIZE + 32:
        off = HEADER_SIZE
        ckpt_lsn = struct.unpack_from("<Q", data, off)[0]
        off += 8
        ckpt_epoch = struct.unpack_from("<Q", data, off)[0]
        off += 8
        print(f"  last_checkpoint_lsn:   {ckpt_lsn}")
        print(f"  last_checkpoint_epoch: {ckpt_epoch}")


# ── WAL v3 Inspector ─────────────────────────────────────────────────────

def inspect_wal(shard_dir, max_records=10):
    print("\n" + "=" * 65)
    print(" WAL v3: Write-Ahead Log Segments")
    print("=" * 65)

    wal_dir = os.path.join(shard_dir, "wal-v3")
    if not os.path.isdir(wal_dir):
        print("  (no wal-v3 directory)")
        return

    wal_files = sorted(glob.glob(os.path.join(wal_dir, "*.wal")))
    total = sum(os.path.getsize(f) for f in wal_files)
    print(f"  Segments: {len(wal_files)} | Total: {total:,} bytes ({total/1024/1024:.1f} MB)")
    print()

    WAL_RECORD_TYPES = {
        0: "Command", 1: "Checkpoint", 2: "FPI", 3: "Commit",
        4: "Abort", 5: "WarmTransition", 6: "ColdTransition",
    }

    for wf in wal_files[:3]:
        fname = os.path.basename(wf)
        fsize = os.path.getsize(wf)
        print(f"  {fname}: {fsize:,} bytes ({fsize/1024:.0f} KB)")

        with open(wf, "rb") as f:
            # WAL v3 record header: [lsn:u64] [type:u8] [flags:u8] [len:u16] [crc:u32] [payload]
            record_count = 0
            while record_count < max_records:
                rec_hdr = f.read(16)
                if len(rec_hdr) < 16:
                    break

                lsn = struct.unpack_from("<Q", rec_hdr, 0)[0]
                rtype = rec_hdr[8]
                rflags = rec_hdr[9]
                rlen = struct.unpack_from("<H", rec_hdr, 10)[0]
                rcrc = struct.unpack_from("<I", rec_hdr, 12)[0]

                if lsn == 0 and rtype == 0 and rlen == 0:
                    break  # end of records

                type_name = WAL_RECORD_TYPES.get(rtype, f"Unknown({rtype})")

                # Read payload
                payload = f.read(rlen) if rlen > 0 else b""

                # For Command records, try to show the Redis command
                preview = ""
                if rtype == 0 and rlen > 0:  # Command
                    try:
                        text = payload.decode("utf-8", errors="replace")
                        # RESP format: *N\r\n$len\r\narg\r\n...
                        parts = text.split("\r\n")
                        cmd_parts = [p for p in parts if p and not p.startswith("*") and not p.startswith("$")]
                        preview = " ".join(cmd_parts[:4])
                        if len(preview) > 60:
                            preview = preview[:57] + "..."
                    except Exception:
                        preview = repr(payload[:30])

                print(f"    lsn={lsn:8d} type={type_name:15s} len={rlen:5d} "
                      f"crc=0x{rcrc:08X}"
                      + (f" | {preview}" if preview else ""))
                record_count += 1

            if record_count >= max_records:
                print(f"    ... (showing first {max_records} records)")
        print()


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="MoonStore V2 file inspector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("data_dir", help="Moon --dir path (e.g. /tmp/moon-tier-32mb)")
    p.add_argument("--tier", default="all",
                   choices=["all", "cold", "warm", "kv", "manifest", "control", "wal"],
                   help="Which tier to inspect")
    p.add_argument("--max-entries", type=int, default=5,
                   help="Max items to show per section")
    args = p.parse_args()

    # Find shard directory
    shard_dirs = sorted(glob.glob(os.path.join(args.data_dir, "shard-*")))
    shard_dirs = [d for d in shard_dirs if os.path.isdir(d) and not d.endswith(".wal")]
    if not shard_dirs:
        print(f"No shard directories found in {args.data_dir}")
        sys.exit(1)

    for shard_dir in shard_dirs:
        print(f"\n{'#' * 65}")
        print(f" Shard: {os.path.basename(shard_dir)}")
        print(f" Path:  {shard_dir}")
        print('#' * 65)

        if args.tier in ("all", "manifest"):
            inspect_manifest(shard_dir, args.max_entries)

        if args.tier in ("all", "control"):
            inspect_control(shard_dir)

        if args.tier in ("all", "cold"):
            inspect_cold(shard_dir, args.max_entries)

        if args.tier in ("all", "warm"):
            inspect_warm(shard_dir)

        if args.tier in ("all", "kv"):
            inspect_kv_spill(shard_dir, args.max_entries)

        if args.tier in ("all", "wal"):
            inspect_wal(shard_dir, args.max_entries)


if __name__ == "__main__":
    main()
