#!/bin/bash
exec > /tmp/uring-test-result.txt 2>&1
set -x

echo '=== io_uring syscall test ==='
python3 << 'PYEOF'
import ctypes, os
SYS_io_uring_setup = 425
libc = ctypes.CDLL(None, use_errno=True)

class io_uring_params(ctypes.Structure):
    _fields_ = [
        ("sq_entries", ctypes.c_uint32),
        ("cq_entries", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("sq_thread_cpu", ctypes.c_uint32),
        ("sq_thread_idle", ctypes.c_uint32),
        ("features", ctypes.c_uint32),
        ("wq_fd", ctypes.c_uint32),
        ("resv", ctypes.c_uint32 * 3),
        ("sq_off", ctypes.c_uint8 * 40),
        ("cq_off", ctypes.c_uint8 * 40),
    ]

params = io_uring_params()
fd = libc.syscall(SYS_io_uring_setup, 32, ctypes.byref(params))
if fd >= 0:
    print(f"io_uring_setup OK (fd={fd}, features=0x{params.features:x})")
    os.close(fd)
else:
    errno = ctypes.get_errno()
    print(f"io_uring_setup FAILED (errno={errno})")
PYEOF

echo '=== Moon io_uring startup ==='
pkill -9 -f 'target/release/moon' 2>/dev/null
sleep 1
~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /tmp/moon-uring.log 2>&1 &
MPID=$!
sleep 3
cat /tmp/moon-uring.log
echo "PID=$MPID THREADS=$(ls /proc/$MPID/task/ 2>/dev/null | wc -l)"

echo '=== Single connection test ==='
timeout 3 python3 << 'PYEOF'
import socket
s = socket.socket()
s.settimeout(2)
s.connect(("127.0.0.1", 6399))
s.send(b"*1\r\n$4\r\nPING\r\n")
print("GOT:", repr(s.recv(100)))
s.close()
PYEOF
echo "SINGLE_RC=$?"

echo '=== Multi connection test (3 serial) ==='
timeout 8 python3 << 'PYEOF'
import socket, time

for i in range(3):
    s = socket.socket()
    s.settimeout(2)
    try:
        s.connect(("127.0.0.1", 6399))
        s.send(b"*1\r\n$4\r\nPING\r\n")
        data = s.recv(100)
        print(f"conn {i}: {data!r}")
    except Exception as e:
        print(f"conn {i} ERROR: {e}")
    finally:
        s.close()
PYEOF
echo "SERIAL_RC=$?"

echo '=== Multi connection test (3 concurrent) ==='
timeout 8 python3 << 'PYEOF'
import socket

conns = []
for i in range(3):
    s = socket.socket()
    s.settimeout(2)
    s.connect(("127.0.0.1", 6399))
    conns.append(s)
    print(f"conn {i} connected")

for i, s in enumerate(conns):
    s.send(b"*1\r\n$4\r\nPING\r\n")
    print(f"conn {i} sent PING")

for i, s in enumerate(conns):
    try:
        data = s.recv(100)
        print(f"conn {i} GOT: {data!r}")
    except Exception as e:
        print(f"conn {i} ERROR: {e}")
    s.close()
PYEOF
echo "CONCURRENT_RC=$?"

echo '=== redis-benchmark test (10 clients, 1000 ops) ==='
timeout 10 redis-benchmark -p 6399 -c 10 -n 1000 -P 1 -t ping -q 2>&1
echo "BENCH_RC=$?"

kill -9 $MPID 2>/dev/null
echo DONE
