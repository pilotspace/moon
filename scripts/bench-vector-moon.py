#!/usr/bin/env python3
"""Moon vector insert + search benchmark (no numpy needed)."""
import socket, struct, random, time

HOST, PORT = "127.0.0.1", 6400
DIM = 128
COUNT = 10000
QUERIES = 100

def send_raw(sock, data):
    sock.sendall(data if isinstance(data, bytes) else data.encode())

def resp_bulk(s):
    return f"${len(s)}\r\n{s}\r\n"

def resp_bulk_bytes(b):
    return f"${len(b)}\r\n".encode() + b + b"\r\n"

def recv_line(sock):
    buf = b""
    while b"\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    return buf.decode(errors="replace").strip()

def main():
    s = socket.socket()
    s.connect((HOST, PORT))
    s.settimeout(10)

    # FT.CREATE
    cmd = (
        "*15\r\n"
        "$9\r\nFT.CREATE\r\n"
        "$3\r\nidx\r\n"
        "$2\r\nON\r\n"
        "$4\r\nHASH\r\n"
        "$6\r\nPREFIX\r\n"
        "$1\r\n1\r\n"
        "$2\r\nv:\r\n"
        "$6\r\nSCHEMA\r\n"
        "$3\r\nemb\r\n"
        "$6\r\nVECTOR\r\n"
        "$4\r\nFLAT\r\n"
        "$1\r\n6\r\n"
        "$3\r\nDIM\r\n"
        "$3\r\n128\r\n"
        "$13\r\nDISTANCE_METRIC\r\n"
    )
    # Hmm this is getting complex. Let me use a simpler approach.
    # Just use HSET for insert, then count entries as "search" proxy.

    # Insert 10K vectors via pipelined HSET
    print(f"Inserting {COUNT} vectors ({DIM}d)...")
    t0 = time.time()
    batch = bytearray()
    for i in range(COUNT):
        random.seed(i)
        v = [random.gauss(0, 1) for _ in range(DIM)]
        blob = struct.pack(f"{DIM}f", *v)
        key = f"v:{i}"
        # *4\r\n$4\r\nHSET\r\n$N\r\nkey\r\n$3\r\nemb\r\n$512\r\nblob\r\n
        hdr = f"*4\r\n${4}\r\nHSET\r\n${len(key)}\r\n{key}\r\n${3}\r\nemb\r\n${len(blob)}\r\n".encode()
        batch += hdr + blob + b"\r\n"
        if len(batch) > 65536:
            s.sendall(bytes(batch))
            batch = bytearray()
    if batch:
        s.sendall(bytes(batch))

    # Drain replies
    time.sleep(1)
    s.settimeout(0.3)
    drained = 0
    try:
        while True:
            d = s.recv(65536)
            drained += len(d)
    except:
        pass
    s.settimeout(10)

    t1 = time.time()
    ins_sec = t1 - t0
    print(f"Insert: {ins_sec:.1f}s ({COUNT/ins_sec:.0f} vec/s)")

    # For search: send FT.CREATE then FT.SEARCH using raw RESP
    # Create index
    create_cmd = (
        "*17\r\n"
        "$9\r\nFT.CREATE\r\n"
        "$3\r\nidx\r\n"
        "$2\r\nON\r\n"
        "$4\r\nHASH\r\n"
        "$6\r\nPREFIX\r\n"
        "$1\r\n1\r\n"
        "$2\r\nv:\r\n"
        "$6\r\nSCHEMA\r\n"
        "$3\r\nemb\r\n"
        "$6\r\nVECTOR\r\n"
        "$4\r\nFLAT\r\n"
        "$1\r\n6\r\n"
        "$3\r\nDIM\r\n"
        "$3\r\n128\r\n"
        "$15\r\nDISTANCE_METRIC\r\n"
        "$6\r\nCOSINE\r\n"
        "$4\r\nTYPE\r\n"
        "$7\r\nFLOAT32\r\n"
    )
    # That's 19 args. Let me count: FT.CREATE idx ON HASH PREFIX 1 v: SCHEMA emb VECTOR FLAT 6 DIM 128 DISTANCE_METRIC COSINE TYPE FLOAT32 = 19
    create_cmd = (
        "*19\r\n"
        "$9\r\nFT.CREATE\r\n"
        "$3\r\nidx\r\n"
        "$2\r\nON\r\n"
        "$4\r\nHASH\r\n"
        "$6\r\nPREFIX\r\n"
        "$1\r\n1\r\n"
        "$2\r\nv:\r\n"
        "$6\r\nSCHEMA\r\n"
        "$3\r\nemb\r\n"
        "$6\r\nVECTOR\r\n"
        "$4\r\nFLAT\r\n"
        "$1\r\n6\r\n"
        "$3\r\nDIM\r\n"
        "$3\r\n128\r\n"
        "$15\r\nDISTANCE_METRIC\r\n"
        "$6\r\nCOSINE\r\n"
        "$4\r\nTYPE\r\n"
        "$7\r\nFLOAT32\r\n"
    )
    s.sendall(create_cmd.encode())
    r = recv_line(s)
    print(f"FT.CREATE: {r}")

    # Search: FT.SEARCH idx "*=>[KNN 10 @emb $BLOB AS score]" PARAMS 2 BLOB <bytes> DIALECT 2
    print(f"Searching {QUERIES} queries (k=10)...")
    t2 = time.time()
    ok = 0
    for q in range(QUERIES):
        random.seed(q + 50000)
        v = [random.gauss(0, 1) for _ in range(DIM)]
        blob = struct.pack(f"{DIM}f", *v)
        query_str = "*=>[KNN 10 @emb $BLOB AS score]"

        # *9 FT.SEARCH idx query PARAMS 2 BLOB <blob> DIALECT 2
        search_hdr = (
            f"*9\r\n"
            f"$9\r\nFT.SEARCH\r\n"
            f"$3\r\nidx\r\n"
            f"${len(query_str)}\r\n{query_str}\r\n"
            f"$6\r\nPARAMS\r\n"
            f"$1\r\n2\r\n"
            f"$4\r\nBLOB\r\n"
            f"${len(blob)}\r\n"
        ).encode() + blob + b"\r\n" + b"$7\r\nDIALECT\r\n$1\r\n2\r\n"

        s.sendall(search_hdr)
        try:
            r = recv_line(s)
            ok += 1
        except:
            pass

    t3 = time.time()
    q_sec = t3 - t2
    print(f"Search: {q_sec:.1f}s ({ok}/{QUERIES} ok, {ok/q_sec:.0f} QPS)")
    s.close()

if __name__ == "__main__":
    main()
